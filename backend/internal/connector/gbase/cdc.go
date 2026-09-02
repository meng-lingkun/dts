package gbaseconnector

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"qmigration/backend/internal/cdc/gbase8acdc"
	"qmigration/backend/internal/connector"
	"qmigration/backend/internal/domain"
)

func (c *Connector) cdcAgent() (*gbase8acdc.Client, error) {
	if !sourceCDCEnabled() {
		return nil, errors.New("GBase 8a source CDC requires QMIGRATION_EXPERIMENTAL_GBASE8A_NATIVE=1 and QMIGRATION_EXPERIMENTAL_GBASE8A_SOURCE_CDC=1")
	}
	if strings.TrimSpace(c.ds.CDCURL) == "" {
		return nil, errors.New("GBase 8a source CDC requires datasource cdc_url pointing at a datasource-local proof provider")
	}
	return gbase8acdc.NewClient(c.ds.CDCURL, os.Getenv("QMIGRATION_GBASE8A_CDC_CA_PEM"), os.Getenv("QMIGRATION_GBASE8A_CDC_SERVER_NAME"), os.Getenv("QMIGRATION_GBASE8A_CDC_TOKEN"))
}

func (c *Connector) cdcSelections(ctx context.Context, mappings []domain.TableMapping) ([]gbase8acdc.TableSelection, error) {
	if len(mappings) == 0 {
		return nil, errors.New("GBase 8a CDC requires selected tables")
	}
	out := make([]gbase8acdc.TableSelection, 0, len(mappings))
	seen := map[string]bool{}
	for i, m := range mappings {
		schema, table := strings.TrimSpace(m.SourceSchema), strings.TrimSpace(m.SourceTable)
		if schema == "" {
			schema = strings.TrimSpace(c.ds.Schema)
		}
		if schema == "" || table == "" {
			return nil, fmt.Errorf("GBase 8a CDC selection %d has empty schema/table", i)
		}
		key := strings.ToLower(schema + "\x00" + table)
		if seen[key] {
			return nil, fmt.Errorf("duplicate GBase 8a CDC selection %s.%s", schema, table)
		}
		seen[key] = true
		md, err := c.GetTableMetadata(ctx, schema, table)
		if err != nil {
			return nil, fmt.Errorf("GBase 8a CDC metadata %s.%s: %w", schema, table, err)
		}
		pks := append([]string(nil), md.PrimaryKeys...)
		if len(pks) == 0 && strings.TrimSpace(md.PrimaryKey) != "" {
			pks = []string{md.PrimaryKey}
		}
		if len(pks) == 0 {
			return nil, fmt.Errorf("GBase 8a CDC requires a primary/migration key on %s.%s", schema, table)
		}
		sel, err := gbase8acdc.BuildTableSelection(schema, table, md.Columns, pks)
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
	if err = agent.Health(ctx); err != nil {
		return fmt.Errorf("GBase 8a CDC provider health: %w", err)
	}
	_, err = c.cdcSelections(ctx, mappings)
	return err
}

func (c *Connector) CurrentCDCPositionForSelection(ctx context.Context, mappings []domain.TableMapping) (*domain.CDCPosition, error) {
	agent, err := c.cdcAgent()
	if err != nil {
		return nil, err
	}
	if err = agent.Health(ctx); err != nil {
		return nil, fmt.Errorf("GBase 8a CDC provider health: %w", err)
	}
	selections, err := c.cdcSelections(ctx, mappings)
	if err != nil {
		return nil, err
	}
	cp, err := agent.Checkpoint(ctx, gbase8acdc.CheckpointRequest{Database: c.ds.Database, Tables: selections})
	if err != nil {
		return nil, err
	}
	if cp == nil {
		return nil, errors.New("GBase 8a CDC provider returned nil checkpoint")
	}
	if cp.TransactionAtomicity != "COMMITTED_TXN_V1" {
		return nil, fmt.Errorf("GBase 8a CDC provider does not attest committed-transaction atomicity: %q", cp.TransactionAtomicity)
	}
	if err = gbase8acdc.ValidateSchemaFences(selections, cp.SchemaFences); err != nil {
		return nil, fmt.Errorf("GBase 8a CDC schema fence: %w", err)
	}
	seq, err := gbase8acdc.ParseSequence(cp.Sequence)
	if err != nil {
		return nil, fmt.Errorf("invalid GBase 8a CDC checkpoint sequence %q: %w", cp.Sequence, err)
	}
	lineage, err := gbase8acdc.NormalizeLineage(cp.CaptureLineage)
	if err != nil {
		return nil, err
	}
	resource := strings.TrimSpace(cp.Resource)
	if resource == "" {
		resource = c.ds.CDCURL
	}
	return &domain.CDCPosition{DatabaseType: string(domain.DataSourceGBase), PositionType: "GBASE8A_CDC_SEQ", PositionValue: gbase8acdc.FormatPosition(seq, lineage), Resource: resource, SourceTimestampMS: cp.SourceTimestampMS}, nil
}

var _ connector.CDCSelectionPositionSource = (*Connector)(nil)
var _ connector.CDCSelectionValidator = (*Connector)(nil)
