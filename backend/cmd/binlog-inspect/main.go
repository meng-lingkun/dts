package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"qmigration/backend/internal/cdc/mysqlbinlog"
	"qmigration/backend/internal/connector"
	mysqlconnector "qmigration/backend/internal/connector/mysql"
	"qmigration/backend/internal/domain"
	"strconv"
	"strings"
	"time"
)

type summary struct {
	Timestamp           uint32 `json:"timestamp"`
	Type                byte   `json:"type"`
	TypeName            string `json:"type_name"`
	ServerID            uint32 `json:"server_id"`
	LogPos              uint32 `json:"log_pos"`
	Size                uint32 `json:"size"`
	Schema              string `json:"schema,omitempty"`
	Table               string `json:"table,omitempty"`
	TableID             uint64 `json:"table_id,omitempty"`
	Columns             uint64 `json:"columns,omitempty"`
	RotateFile          string `json:"rotate_file,omitempty"`
	RotatePosition      uint64 `json:"rotate_position,omitempty"`
	SQL                 string `json:"sql,omitempty"`
	TransactionPosition string `json:"transaction_position,omitempty"`
	XID                 uint64 `json:"xid,omitempty"`
	Error               string `json:"decode_error,omitempty"`
}

func eventName(t byte) string {
	switch t {
	case mysqlbinlog.QueryEvent:
		return "QUERY"
	case mysqlbinlog.RotateEvent:
		return "ROTATE"
	case mysqlbinlog.FormatDescriptionEvent:
		return "FORMAT_DESCRIPTION"
	case mysqlbinlog.XIDEvent:
		return "XID"
	case mysqlbinlog.TableMapEvent:
		return "TABLE_MAP"
	case mysqlbinlog.WriteRowsEventV2:
		return "WRITE_ROWS_V2"
	case mysqlbinlog.UpdateRowsEventV2:
		return "UPDATE_ROWS_V2"
	case mysqlbinlog.DeleteRowsEventV2:
		return "DELETE_ROWS_V2"
	case mysqlbinlog.PartialUpdateRowsEvent:
		return "PARTIAL_UPDATE_ROWS"
	case mysqlbinlog.TransactionPayloadEvent:
		return "TRANSACTION_PAYLOAD"
	case mysqlbinlog.GTIDEvent:
		return "GTID"
	case mysqlbinlog.AnonymousGTIDEvent:
		return "ANONYMOUS_GTID"
	default:
		return "EVENT_" + strconv.Itoa(int(t))
	}
}

func summarize(e *mysqlbinlog.Event, a *mysqlbinlog.Assembler) summary {
	out := summary{Timestamp: e.Header.Timestamp, Type: e.Header.Type, TypeName: eventName(e.Header.Type), ServerID: e.Header.ServerID, LogPos: e.Header.LogPos, Size: e.Header.EventSize}
	switch e.Header.Type {
	case mysqlbinlog.RotateEvent:
		if v, err := mysqlbinlog.ParseRotate(e); err == nil {
			out.RotateFile = v.File
			out.RotatePosition = v.Position
		} else {
			out.Error = err.Error()
		}
	case mysqlbinlog.QueryEvent:
		if v, err := mysqlbinlog.ParseQuery(e); err == nil {
			out.Schema = v.Schema
			out.SQL = v.SQL
		} else {
			out.Error = err.Error()
		}
	case mysqlbinlog.TableMapEvent:
		if v, err := mysqlbinlog.ParseTableMap(e); err == nil {
			out.Schema = v.Schema
			out.Table = v.Table
			out.TableID = v.TableID
			out.Columns = uint64(len(v.ColumnTypes))
		} else {
			out.Error = err.Error()
		}
	case mysqlbinlog.WriteRowsEventV2, mysqlbinlog.UpdateRowsEventV2, mysqlbinlog.DeleteRowsEventV2, mysqlbinlog.PartialUpdateRowsEvent:
		if v, err := mysqlbinlog.ParseRowsV2(e); err == nil {
			out.TableID = v.TableID
			out.Columns = v.ColumnCount
		} else {
			out.Error = err.Error()
		}
	}
	if tx, err := a.Push(e); err != nil && out.Error == "" {
		out.Error = err.Error()
	} else if tx != nil {
		out.TransactionPosition = tx.Position()
		out.XID = tx.XID
	}
	return out
}

type eventSource interface {
	Next(context.Context) ([]byte, error)
	Close() error
}

type fileSource struct {
	f *os.File
	r *mysqlbinlog.Reader
}

func (s *fileSource) Next(ctx context.Context) ([]byte, error) {
	_ = ctx
	e, err := s.r.Next()
	if err != nil {
		return nil, err
	}
	raw := make([]byte, e.Header.EventSize)
	// Reader already parsed the frame; reconstructing would lose checksum. File mode
	// uses the specialized loop below, so this method is intentionally unused.
	_ = raw
	return nil, io.EOF
}
func (s *fileSource) Close() error { return s.f.Close() }

func inspectFile(path string, checksum int, limit int, enc *json.Encoder) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	r := mysqlbinlog.NewReader(f, checksum)
	if err := r.ReadMagic(); err != nil {
		return err
	}
	a := &mysqlbinlog.Assembler{}
	for n := 0; limit <= 0 || n < limit; n++ {
		e, err := r.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := enc.Encode(summarize(e, a)); err != nil {
			return err
		}
	}
	return nil
}

func inspectLive(ctx context.Context, ds domain.DataSource, file string, pos uint, serverID uint, checksum int, limit int, enc *json.Encoder) error {
	f := mysqlconnector.NewFactory()
	raw, err := f.New(ds)
	if err != nil {
		return err
	}
	defer raw.Close()
	source, ok := raw.(connector.MySQLBinlogSource)
	if !ok {
		return fmt.Errorf("connector does not support native binlog stream")
	}
	stream, err := source.OpenBinlogStream(ctx, file, uint32(pos), uint32(serverID))
	if err != nil {
		return err
	}
	defer stream.Close()
	parser := mysqlbinlog.Parser{ChecksumBytes: checksum}
	a := &mysqlbinlog.Assembler{}
	for n := 0; limit <= 0 || n < limit; n++ {
		data, err := stream.Next(ctx)
		if err != nil {
			return err
		}
		e, err := parser.Parse(data)
		if err != nil {
			return err
		}
		if err := enc.Encode(summarize(e, a)); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	var path, host, user, database, startFile, passwordEnv string
	var port, checksum, limit int
	var startPos, serverID uint
	var timeout time.Duration
	flag.StringVar(&path, "file", "", "raw mysql-bin.* file to inspect")
	flag.IntVar(&checksum, "checksum-bytes", 0, "trailing checksum bytes per event, usually 0 or 4")
	flag.IntVar(&limit, "limit", 0, "maximum events; 0 means unlimited")
	flag.StringVar(&host, "host", "", "live MySQL host")
	flag.IntVar(&port, "port", 3306, "live MySQL port")
	flag.StringVar(&user, "user", "", "replication user")
	flag.StringVar(&passwordEnv, "password-env", "MYSQL_PWD", "environment variable containing password")
	flag.StringVar(&database, "database", "", "database used for authentication")
	flag.StringVar(&startFile, "start-file", "", "binlog file for live mode")
	flag.UintVar(&startPos, "start-pos", 4, "binlog position for live mode")
	flag.UintVar(&serverID, "server-id", 0, "replication server-id; 0 selects a deterministic local default")
	flag.DurationVar(&timeout, "timeout", 0, "optional live stream timeout, e.g. 30s")
	flag.Parse()
	enc := json.NewEncoder(os.Stdout)
	var err error
	if strings.TrimSpace(path) != "" {
		err = inspectFile(path, checksum, limit, enc)
	} else {
		if host == "" || user == "" || startFile == "" {
			fmt.Fprintln(os.Stderr, "either -file or -host/-user/-start-file is required")
			os.Exit(2)
		}
		ctx := context.Background()
		var cancel context.CancelFunc
		if timeout > 0 {
			ctx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}
		ds := domain.DataSource{Type: domain.DataSourceMySQL, Host: host, Port: port, Username: user, Password: os.Getenv(passwordEnv), Database: database}
		err = inspectLive(ctx, ds, startFile, startPos, serverID, checksum, limit, enc)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "binlog inspect:", err)
		os.Exit(1)
	}
}
