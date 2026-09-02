package oracleconnector

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// ensureNativeSessionLocked upgrades a TNS ACCEPT socket through TTC protocol,
// datatype negotiation and password authentication. Callers must hold c.mu.
func (c *Connector) ensureNativeSessionLocked(ctx context.Context) error {
	if c.accepted != nil && c.accepted.Session != nil && c.authenticated {
		return nil
	}
	accepted, err := c.openAcceptedSession(ctx)
	if err != nil {
		return err
	}
	proto, err := c.negotiateTTCProtocol(ctx, accepted)
	if err != nil {
		_ = accepted.Session.Close()
		return err
	}
	data, err := c.negotiateTTCDataTypes(ctx, accepted, proto)
	if err != nil {
		_ = accepted.Session.Close()
		return err
	}
	auth, err := c.authenticateTTC(ctx, accepted, proto, data)
	if err != nil {
		_ = accepted.Session.Close()
		return err
	}
	c.accepted = accepted
	c.proto = proto
	c.data = data
	c.authenticated = true
	c.sessionProperties = auth.SessionProperties
	c.version = fmt.Sprintf("oracle-ttc-v%d-charset-%d-ttc%d-auth", proto.ServerVersion, proto.ServerCharset, data.TTCVersion)
	if strings.EqualFold(accepted.Protocol, "TCPS") || c.ds.TLSMode == "REQUIRED" {
		c.version += "-tcps"
	}
	return nil
}

func (c *Connector) resetNativeSessionLocked() {
	if c.accepted != nil && c.accepted.Session != nil {
		_ = c.accepted.Session.Close()
	}
	c.accepted = nil
	c.authenticated = false
	c.inTransaction = false
	c.prepared = nil
}

func buildTTCStatementRequest(sql string, ttcVersion byte) ([]byte, error) {
	sql = strings.TrimSpace(sql)
	if sql == "" {
		return nil, errors.New("Oracle TTC statement is empty")
	}
	if len(sql) > 4<<20 {
		return nil, errors.New("Oracle TTC statement exceeds 4 MiB limit")
	}
	upper := strings.ToUpper(strings.TrimSpace(sql))
	plsql := strings.HasPrefix(upper, "BEGIN") || strings.HasPrefix(upper, "DECLARE")
	exeOp := uint64(0x21) // parse + execute
	if plsql {
		exeOp |= 0x40000
	} else {
		exeOp |= 0x8000
	}
	w := &ttcEncoder{}
	w.byte(ttcFunctionCall)
	w.byte(0x5e)
	w.byte(0)
	w.compactUint(exeOp, 4)
	w.compactUint(0, 2)
	w.byte(1)
	w.compactUint(uint64(len([]byte(sql))), 4)
	w.byte(1)
	w.compactUint(13, 2)
	w.byte(0)
	w.byte(0)
	w.compactUint(0, 4)
	w.compactUint(0, 4)
	w.compactUint(0x7fffffff, 4)
	w.byte(0) // no bind variables; QMigration renders guarded literals
	w.byte(0)
	w.byte(0)
	w.byte(0)
	w.byte(0)
	w.byte(0)
	w.byte(0) // no define metadata
	w.byte(0)
	if ttcVersion >= 4 {
		w.Write([]byte{0, 0, 1})
	}
	if ttcVersion >= 5 {
		w.Write([]byte{0, 0, 0, 0, 0})
	}
	w.clr([]byte(sql))
	al8i4 := [13]uint64{}
	al8i4[0] = 1
	al8i4[1] = 1
	for _, v := range al8i4 {
		w.compactUint(v, 4)
	}
	return append([]byte(nil), w.Bytes()...), nil
}

func (c *Connector) executeTTCStatementLocked(ctx context.Context, sql string) (oracleTTCSummary, error) {
	var zero oracleTTCSummary
	if err := c.ensureNativeSessionLocked(ctx); err != nil {
		return zero, err
	}
	req, err := buildTTCStatementRequest(sql, c.data.TTCVersion)
	if err != nil {
		return zero, err
	}
	if err := c.accepted.Session.WriteData(ctx, 0, req); err != nil {
		c.resetNativeSessionLocked()
		return zero, fmt.Errorf("Oracle TTC statement request: %w", err)
	}
	state := oracleTTCQueryState{}
	for packets := 0; packets < 4096; packets++ {
		state.SummarySeen = false
		state.FunctionStat = false
		if err := readAndConsumeTTCQueryData(ctx, c.accepted.Session, c.data, c.proto, &state, 1); err != nil {
			c.resetNativeSessionLocked()
			return zero, fmt.Errorf("Oracle TTC statement response: %w", err)
		}
		if state.SummarySeen {
			return state.LastSummary, nil
		}
		if state.FunctionStat {
			return state.LastSummary, nil
		}
	}
	return zero, errors.New("Oracle TTC statement response packet limit exceeded")
}

func (c *Connector) execSQL(ctx context.Context, sql string) (oracleTTCSummary, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.executeTTCStatementLocked(ctx, sql)
}

func (c *Connector) querySQL(ctx context.Context, sql string, fetchRows, maxRows int) (oracleTTCQueryResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out oracleTTCQueryResult
	if err := c.ensureNativeSessionLocked(ctx); err != nil {
		return out, err
	}
	out, err := c.executeTTCSelectBatched(ctx, c.accepted, c.proto, c.data, sql, fetchRows, maxRows)
	if err != nil {
		return out, err
	}
	// Materialize server-side LOB locators while the authenticated session is
	// still alive. The query cursor is exhausted or closed by this point.
	for ri := range out.Rows {
		for ci := range out.Rows[ri] {
			ref, ok := out.Rows[ri][ci].(oracleTTCLobRef)
			if !ok {
				continue
			}
			if len(ref.Locator) == 0 {
				out.Rows[ri][ci] = append([]byte(nil), ref.Inline...)
				continue
			}
			b, e := c.executeTTCLobRead(ctx, c.accepted, c.proto, c.data, ref, oracleMaxTTCResponseBytes)
			if e != nil {
				return out, fmt.Errorf("materialize Oracle LOB column %d row %d: %w", ci, ri, e)
			}
			if len(ref.Inline) > 0 && len(b) == 0 {
				b = ref.Inline
			}
			if ref.DataType == oracleTypeCLOB {
				out.Rows[ri][ci] = string(b)
			} else {
				out.Rows[ri][ci] = append([]byte(nil), b...)
			}
		}
	}
	return out, nil
}
