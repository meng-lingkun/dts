package gbase8sconnector

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"qmigration/backend/internal/cdc/gbase8scdc"
	"qmigration/backend/internal/connector"
	"qmigration/backend/internal/domain"
)

func experimentalCDCEnabled() bool { return envOn("QMIGRATION_EXPERIMENTAL_GBASE8S_CDC") }
func experimentalSmartLOBCDCEnabled() bool {
	return envOn("QMIGRATION_EXPERIMENTAL_GBASE8S_SMART_LOB_CDC")
}

func (c *Connector) cdcAgent() (*gbase8scdc.Client, error) {
	if !experimentalEnabled() || !experimentalCDCEnabled() {
		return nil, errors.New("GBase 8s source CDC requires QMIGRATION_EXPERIMENTAL_GBASE8S_NATIVE=1 and QMIGRATION_EXPERIMENTAL_GBASE8S_CDC=1")
	}
	raw := strings.TrimSpace(c.ds.CDCURL)
	if raw == "" {
		return nil, errors.New("GBase 8s source CDC requires datasource cdc_url pointing at a local CSDK CDC provider")
	}
	return gbase8scdc.NewClient(raw, os.Getenv("QMIGRATION_GBASE8S_CDC_CA_PEM"), os.Getenv("QMIGRATION_GBASE8S_CDC_SERVER_NAME"), os.Getenv("QMIGRATION_GBASE8S_CDC_TOKEN"))
}

func smartLOBCDCType(col domain.ColumnInfo) bool {
	t := strings.ToLower(strings.TrimSpace(col.DataType + " " + col.ColumnType))
	return strings.Contains(t, "blob") || strings.Contains(t, "clob")
}

func unsupportedCDCType(col domain.ColumnInfo) bool {
	t := strings.ToLower(strings.TrimSpace(col.DataType + " " + col.ColumnType))
	// Smart BLOB/CLOB has its own RC28 event-owned image contract. Simple
	// TEXT/BYTE and complex/opaque/collection families remain fail-closed.
	for _, bad := range []string{"text", "byte", "opaque", "collection", "multiset", " set", "list", "row(", "udt"} {
		if strings.Contains(t, bad) {
			return true
		}
	}
	return false
}

func (c *Connector) cdcSelections(ctx context.Context, mappings []domain.TableMapping) ([]gbase8scdc.TableSelection, error) {
	if len(mappings) == 0 {
		return nil, errors.New("GBase 8s CDC requires selected tables")
	}
	out := make([]gbase8scdc.TableSelection, 0, len(mappings))
	seen := map[string]bool{}
	for i, m := range mappings {
		schema, table := strings.TrimSpace(m.SourceSchema), strings.TrimSpace(m.SourceTable)
		if schema == "" {
			schema = strings.TrimSpace(c.ds.Schema)
		}
		if schema == "" || table == "" {
			return nil, fmt.Errorf("GBase 8s CDC selection %d has empty schema/table", i)
		}
		key := strings.ToLower(schema + "\x00" + table)
		if seen[key] {
			return nil, fmt.Errorf("duplicate GBase 8s CDC selection %s.%s", schema, table)
		}
		seen[key] = true
		md, err := c.GetTableMetadata(ctx, schema, table)
		if err != nil {
			return nil, fmt.Errorf("GBase 8s CDC metadata %s.%s: %w", schema, table, err)
		}
		if len(md.PrimaryKeys) == 0 {
			return nil, fmt.Errorf("GBase 8s CDC requires a primary key on %s.%s", schema, table)
		}
		for _, col := range md.Columns {
			if unsupportedCDCType(col) {
				return nil, fmt.Errorf("GBase 8s CDC does not qualify simple-LOB/complex column %s.%s.%s (%s)", schema, table, col.Name, col.ColumnType)
			}
			if smartLOBCDCType(col) && !experimentalSmartLOBCDCEnabled() {
				return nil, fmt.Errorf("GBase 8s smart BLOB/CLOB CDC for %s.%s.%s requires QMIGRATION_EXPERIMENTAL_GBASE8S_SMART_LOB_CDC=1 and an event-owned provider image contract", schema, table, col.Name)
			}
		}
		sel, err := gbase8scdc.BuildTableSelection(schema, table, md.Columns, md.PrimaryKeys)
		if err != nil {
			return nil, err
		}
		out = append(out, sel)
	}
	return out, nil
}

func (c *Connector) ValidateCDCSelection(ctx context.Context, mappings []domain.TableMapping) error {
	agent, err := c.cdcAgent()
	if err != nil {
		return err
	}
	if err := agent.Health(ctx); err != nil {
		return fmt.Errorf("GBase 8s CDC provider health: %w", err)
	}
	_, err = c.cdcSelections(ctx, mappings)
	return err
}

func (c *Connector) CurrentCDCPositionForSelection(ctx context.Context, mappings []domain.TableMapping) (*domain.CDCPosition, error) {
	agent, err := c.cdcAgent()
	if err != nil {
		return nil, err
	}
	if err := agent.Health(ctx); err != nil {
		return nil, fmt.Errorf("GBase 8s CDC provider health: %w", err)
	}
	selections, err := c.cdcSelections(ctx, mappings)
	if err != nil {
		return nil, err
	}
	cp, err := agent.Checkpoint(ctx, gbase8scdc.CheckpointRequest{Database: c.ds.Database, Tables: selections})
	if err != nil {
		return nil, err
	}
	if cp == nil {
		return nil, errors.New("GBase 8s CDC provider returned nil checkpoint")
	}
	if err := gbase8scdc.ValidateSchemaFences(selections, cp.SchemaFences); err != nil {
		return nil, fmt.Errorf("GBase 8s CDC checkpoint schema fence: %w", err)
	}
	seq := strings.TrimSpace(cp.Sequence)
	if _, err := strconv.ParseUint(seq, 10, 64); err != nil {
		return nil, fmt.Errorf("GBase 8s CDC provider returned invalid restart sequence %q: %w", seq, err)
	}
	positionValue, err := gbase8scdc.InitialPosition(seq, cp.CaptureLineage)
	if err != nil {
		return nil, fmt.Errorf("GBase 8s CDC checkpoint capture lineage: %w", err)
	}
	resource := strings.TrimSpace(cp.Resource)
	if resource == "" {
		resource = strings.TrimSpace(c.ds.CDCURL)
	}
	return &domain.CDCPosition{DatabaseType: string(domain.DataSourceGBase8s), PositionType: "GBASE8S_CDC_SEQ", PositionValue: positionValue, Resource: resource, SourceTimestampMS: cp.SourceTimestampMS}, nil
}

var _ connector.CDCSelectionPositionSource = (*Connector)(nil)
var _ connector.CDCSelectionValidator = (*Connector)(nil)
