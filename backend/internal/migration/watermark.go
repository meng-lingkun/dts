package migration

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"dts/backend/internal/cdc/mysqlbinlog"
	"dts/backend/internal/cdc/pgoutput"
	"dts/backend/internal/domain"
)

// watermarkCovers uses protocol semantics, never lexical position ordering.
// Unsupported/external positions fail closed rather than accepting a lag value.
func watermarkCovers(applied, barrier domain.CDCPosition) (bool, error) {
	if !strings.EqualFold(applied.PositionType, barrier.PositionType) || barrier.PositionValue == "" || applied.PositionValue == "" {
		return false, errors.New("watermark type mismatch or empty position")
	}
	switch strings.ToUpper(barrier.PositionType) {
	case "LSN", "PG_LSN":
		a, e := pgoutput.ParseLSN(applied.PositionValue)
		if e != nil {
			return false, e
		}
		b, e := pgoutput.ParseLSN(barrier.PositionValue)
		return a >= b, e
	case "GTID":
		a, e := mysqlbinlog.ParseGTIDSet(applied.PositionValue)
		if e != nil {
			return false, e
		}
		b, e := mysqlbinlog.ParseGTIDSet(barrier.PositionValue)
		if e != nil {
			return false, e
		}
		return a.Contains(b), nil
	case "BINLOG":
		af, ao, e := parseWatermarkBinlog(applied.PositionValue)
		if e != nil {
			return false, e
		}
		bf, bo, e := parseWatermarkBinlog(barrier.PositionValue)
		if e != nil {
			return false, e
		}
		if af == bf {
			return ao >= bo, nil
		}
		ai, bi := strings.LastIndexByte(af, '.'), strings.LastIndexByte(bf, '.')
		if ai < 0 || bi < 0 || af[:ai] != bf[:bi] {
			return false, errors.New("binlog file lineage differs")
		}
		an, e := strconv.ParseUint(af[ai+1:], 10, 64)
		if e != nil {
			return false, e
		}
		bn, e := strconv.ParseUint(bf[bi+1:], 10, 64)
		return an > bn, e
	default:
		return false, fmt.Errorf("final watermark proof unsupported for %s", barrier.PositionType)
	}
}
func parseWatermarkBinlog(v string) (string, uint64, error) {
	i := strings.LastIndexByte(v, ':')
	if i <= 0 {
		return "", 0, errors.New("invalid binlog position")
	}
	offset, e := strconv.ParseUint(v[i+1:], 10, 64)
	return v[:i], offset, e
}
