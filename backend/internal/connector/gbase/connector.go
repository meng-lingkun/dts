package gbaseconnector

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"

	"qmigration/backend/internal/connector"
	mysqlconnector "qmigration/backend/internal/connector/mysql"
	"qmigration/backend/internal/domain"
)

// Factory exposes GBase 8a as a distinct QMigration connector family while
// reusing the already-audited MySQL/GBase packet transport.  GBase 8a is not
// treated as a MySQL-family CDC source: only metadata/full read/full write and
// schema creation are advertised in RC18.
type Factory struct{}

func NewFactory() *Factory { return &Factory{} }

func experimentalEnabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("QMIGRATION_EXPERIMENTAL_GBASE8A_NATIVE"))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func sourceCDCEnabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("QMIGRATION_EXPERIMENTAL_GBASE8A_SOURCE_CDC"))) {
	case "1", "true", "yes", "on":
		return experimentalEnabled()
	default:
		return false
	}
}

func transactionalTargetCDCEnabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("QMIGRATION_EXPERIMENTAL_GBASE8A_TRANSACTIONAL_TARGET_CDC"))) {
	case "1", "true", "yes", "on":
		return experimentalEnabled() && targetCDCEnabled()
	default:
		return false
	}
}

func targetCDCEnabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("QMIGRATION_EXPERIMENTAL_GBASE8A_TARGET_CDC"))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func (*Factory) Capabilities(t domain.DataSourceType) connector.Descriptor {
	caps := []connector.Capability{connector.CapabilityProtocolProbe}
	maturity := connector.MaturityProbeOnly
	note := "GBase 8a protocol probe only; set QMIGRATION_EXPERIMENTAL_GBASE8A_NATIVE=1 after real-instance qualification"
	if experimentalEnabled() {
		caps = append(caps,
			connector.CapabilityMetadata,
			connector.CapabilityFullRead,
			connector.CapabilityFullWrite,
			connector.CapabilityKeysetBoundary,
			connector.CapabilitySchemaCreate,
			connector.CapabilityMigrationPrecheck,
		)
		maturity = connector.MaturityExperimental
		note = "EXPERIMENTAL GBase 8a MPP metadata/full-read/full-write/schema data plane over QMigration native packet transport; target Full Write requires validated HASH distribution compatible with the migration key"
		if targetCDCEnabled() {
			caps = append(caps, connector.CapabilityCDCApply, connector.CapabilityPointLookup)
			note += "; target CDC apply is enabled"
		}
		if transactionalTargetCDCEnabled() {
			caps = append(caps, connector.CapabilityCDCTransactional)
			note += "; transactional target CDC uses a DDL-free row DML path inside one target transaction"
		}
		if sourceCDCEnabled() {
			caps = append(caps, connector.CapabilityCDCPosition, connector.CapabilityCDCRead)
			note += "; source CDC uses the datasource-local GBase 8a proof provider with durable sequence/capture-lineage/schema fences"
		}
	}
	return connector.Descriptor{
		Type: t, Protocol: "gbase8a", Capabilities: caps, Native: true,
		Maturity: maturity, QualificationRequired: true, Note: note,
	}
}

func (*Factory) New(ds domain.DataSource) (connector.Connector, error) {
	if ds.Type != domain.DataSourceGBase {
		return nil, errors.New("GBase factory requires datasource type gbase")
	}
	raw, err := mysqlconnector.NewFactory().New(ds)
	if err != nil {
		return nil, err
	}
	inner, ok := raw.(*mysqlconnector.Connector)
	if !ok {
		_ = raw.Close()
		return nil, errors.New("unexpected GBase packet connector type")
	}
	return &Connector{inner: inner, ds: ds}, nil
}

type Connector struct {
	inner         *mysqlconnector.Connector
	ds            domain.DataSource
	mu            sync.Mutex
	inTransaction bool
}

func (c *Connector) TestConnection(ctx context.Context) error       { return c.inner.TestConnection(ctx) }
func (c *Connector) GetVersion(ctx context.Context) (string, error) { return c.inner.GetVersion(ctx) }
func (c *Connector) ListSchemas(ctx context.Context) ([]domain.SchemaInfo, error) {
	return c.inner.ListSchemas(ctx)
}
func (c *Connector) ListTables(ctx context.Context, schema string) ([]domain.TableInfo, error) {
	return c.inner.ListTables(ctx, schema)
}
func (c *Connector) GetTableMetadata(ctx context.Context, schema, table string) (*domain.TableMetadata, error) {
	return c.inner.GetTableMetadata(ctx, schema, table)
}
func (c *Connector) Close() error { return c.inner.Close() }
func (c *Connector) ReadBatch(ctx context.Context, req connector.ReadBatchRequest) (*connector.RowBatch, error) {
	return c.inner.ReadBatch(ctx, req)
}
func (c *Connector) WriteBatch(ctx context.Context, req connector.WriteBatchRequest) (int64, error) {
	c.mu.Lock()
	inTxn := c.inTransaction
	c.mu.Unlock()
	if inTxn {
		if !transactionalTargetCDCEnabled() {
			return 0, errors.New("GBase 8a transactional CDC write requires QMIGRATION_EXPERIMENTAL_GBASE8A_TRANSACTIONAL_TARGET_CDC=1")
		}
		return c.inner.WriteTransactionalGBaseBatch(ctx, req)
	}
	return c.inner.WriteBatch(ctx, req)
}
func (c *Connector) PlanKeysetBoundaries(ctx context.Context, req connector.KeysetBoundaryRequest) ([][]connector.Value, error) {
	return c.inner.PlanKeysetBoundaries(ctx, req)
}
func (c *Connector) EnsureSchema(ctx context.Context, schema string) error {
	return c.inner.EnsureSchema(ctx, schema)
}
func (c *Connector) CreateTable(ctx context.Context, schema, table string, columns []domain.ColumnInfo, primaryKey string) error {
	return c.inner.CreateTable(ctx, schema, table, columns, primaryKey)
}
func (c *Connector) CreateTableWithPrimaryKeys(ctx context.Context, schema, table string, columns []domain.ColumnInfo, primaryKeys []string) error {
	return c.inner.CreateTableWithPrimaryKeys(ctx, schema, table, columns, primaryKeys)
}
func (c *Connector) MigrationPrechecks(ctx context.Context, needCDC bool) []domain.PrecheckItem {
	return c.inner.MigrationPrechecks(ctx, needCDC)
}

// DeleteByKey and ReadByKey expose a deliberately non-transactional target CDC
// apply path for GBase 8a. Each individual operation is retry-idempotent: upserts
// use the already-qualified HASH staging+MERGE path and deletes use the stable
// migration key. QMigration does not expose TransactionalCDCApplyConnector here
// because GBase 8a MPP source-transaction atomicity is not portable/qualified.
func (c *Connector) DeleteByKey(ctx context.Context, req connector.DeleteByKeyRequest) error {
	if !targetCDCEnabled() {
		return errors.New("GBase 8a target CDC apply requires QMIGRATION_EXPERIMENTAL_GBASE8A_TARGET_CDC=1")
	}
	return c.inner.DeleteByKey(ctx, req)
}

func (c *Connector) ReadByKey(ctx context.Context, req connector.ReadByKeyRequest) ([]connector.Value, bool, error) {
	if !targetCDCEnabled() {
		return nil, false, errors.New("GBase 8a target CDC point lookup requires QMIGRATION_EXPERIMENTAL_GBASE8A_TARGET_CDC=1")
	}
	return c.inner.ReadByKey(ctx, req)
}

func (c *Connector) BeginCDCTransaction(ctx context.Context) error {
	if !transactionalTargetCDCEnabled() {
		return errors.New("GBase 8a transactional CDC apply requires QMIGRATION_EXPERIMENTAL_GBASE8A_TRANSACTIONAL_TARGET_CDC=1")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.inTransaction {
		return errors.New("GBase 8a CDC transaction already active")
	}
	if err := c.inner.BeginCDCTransaction(ctx); err != nil {
		return err
	}
	c.inTransaction = true
	return nil
}
func (c *Connector) CommitCDCTransaction(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.inTransaction {
		return errors.New("GBase 8a CDC transaction is not active")
	}
	if err := c.inner.CommitCDCTransaction(ctx); err != nil {
		return err
	}
	c.inTransaction = false
	return nil
}
func (c *Connector) RollbackCDCTransaction(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.inTransaction {
		return nil
	}
	err := c.inner.RollbackCDCTransaction(ctx)
	if err == nil {
		c.inTransaction = false
	}
	return err
}

// DropQualificationTable is intentionally not part of the Connector SPI. It is
// used only by the destructive real-instance qualification command to clean up
// its own temporary table.
func (c *Connector) DropQualificationTable(ctx context.Context, schema, table string) error {
	return c.inner.ExecDDL(ctx, schema, "DROP TABLE IF EXISTS `"+strings.ReplaceAll(schema, "`", "``")+"`.`"+strings.ReplaceAll(table, "`", "``")+"`")
}

var _ connector.DataConnector = (*Connector)(nil)
var _ connector.KeysetBoundaryConnector = (*Connector)(nil)
var _ connector.CompositeSchemaConnector = (*Connector)(nil)
var _ connector.MigrationPrecheckConnector = (*Connector)(nil)
var _ connector.CDCApplyConnector = (*Connector)(nil)
var _ connector.PointLookupConnector = (*Connector)(nil)
var _ connector.TransactionalCDCApplyConnector = (*Connector)(nil)
