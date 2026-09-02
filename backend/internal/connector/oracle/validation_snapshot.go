package oracleconnector

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"qmigration/backend/internal/connector"
	"qmigration/backend/internal/domain"
)

var _ connector.ValidationSnapshotConnector = (*Connector)(nil)

// OpenValidationSnapshot returns an independent Oracle connector whose table
// reads are pinned to one durable LogMiner commit SCN through Oracle Flashback
// Query (SELECT ... AS OF SCN). The historical read itself remains fail-closed:
// if UNDO/Flashback history for the requested SCN is no longer available Oracle
// returns an error and validation is aborted rather than silently falling back
// to current data.
func (c *Connector) OpenValidationSnapshot(_ context.Context, position domain.CDCPosition) (connector.DataConnector, error) {
	if !experimentalOracleLogMinerCDCEnabled() {
		return nil, fmt.Errorf("Oracle exact validation snapshots require QMIGRATION_EXPERIMENTAL_ORACLE_LOGMINER_CDC=1")
	}
	if !strings.EqualFold(strings.TrimSpace(position.PositionType), "ORACLE_SCN") {
		return nil, fmt.Errorf("Oracle exact validation snapshot requires ORACLE_SCN position, got %q", position.PositionType)
	}
	scn, err := strconv.ParseUint(strings.TrimSpace(position.PositionValue), 10, 64)
	if err != nil || scn == 0 {
		if err == nil {
			err = fmt.Errorf("SCN must be greater than zero")
		}
		return nil, fmt.Errorf("invalid Oracle validation SCN %q: %w", position.PositionValue, err)
	}
	return &Connector{ds: c.ds, validationSCN: strconv.FormatUint(scn, 10)}, nil
}

func (c *Connector) rejectValidationSnapshotWrite() error {
	if c.validationSCN != "" {
		return fmt.Errorf("Oracle validation snapshot at SCN %s is read-only", c.validationSCN)
	}
	return nil
}
