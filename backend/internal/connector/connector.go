package connector

import (
	"context"
	"errors"
	"fmt"
	"qmigration/backend/internal/domain"
	"regexp"
	"sort"
	"strings"
)

var ErrMetadataUnavailable = errors.New("connector metadata unavailable; a QMigration native connector implementation is required")

var ErrCapabilityUnavailable = errors.New("connector capability is not available")

var numericLiteralRE = regexp.MustCompile(`^[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?$`)

// ValidateNumericLiteral rejects migration values that would change SQL syntax
// when a native connector still needs to render a numeric scalar in a SQL
// batch. Decimal values are intentionally validated lexically rather than
// parsed through float64 so arbitrary precision is preserved.
func ValidateNumericLiteral(raw []byte, allowSpecialFloat bool) error {
	v := strings.TrimSpace(string(raw))
	if v == "" || len(v) > 512 {
		return fmt.Errorf("invalid numeric literal length")
	}
	if allowSpecialFloat {
		switch strings.ToLower(v) {
		case "nan", "infinity", "+infinity", "-infinity", "inf", "+inf", "-inf":
			return nil
		}
	}
	if !numericLiteralRE.MatchString(v) {
		return fmt.Errorf("invalid numeric literal %q", v)
	}
	return nil
}

type Capability string

type Maturity string

const (
	MaturityNative         Maturity = "NATIVE"
	MaturityNativeFullOnly Maturity = "NATIVE_FULL_ONLY"
	MaturityExperimental   Maturity = "EXPERIMENTAL"
	MaturityProbeOnly      Maturity = "PROBE_ONLY"
)

const (
	CapabilityProtocolProbe      Capability = "protocol-probe"
	CapabilityMetadata           Capability = "metadata"
	CapabilityFullRead           Capability = "full-read"
	CapabilityFullWrite          Capability = "full-write"
	CapabilityKeysetBoundary     Capability = "keyset-boundary"
	CapabilityPartition          Capability = "partition"
	CapabilityTopology           Capability = "topology"
	CapabilityRuntimeLoad        Capability = "runtime-load"
	CapabilitySchemaCreate       Capability = "schema-create"
	CapabilitySchemaObjects      Capability = "schema-objects"
	CapabilityPostLoadSchema     Capability = "post-load-schema"
	CapabilityCDCPosition        Capability = "cdc-position"
	CapabilityCDCCheckpoint      Capability = "cdc-checkpoint"
	CapabilityCDCRead            Capability = "cdc-read"
	CapabilityCDCApply           Capability = "cdc-apply"
	CapabilityCDCTransactional   Capability = "cdc-transactional-apply"
	CapabilityDDLApply           Capability = "ddl-apply"
	CapabilityPointLookup        Capability = "point-lookup"
	CapabilitySequenceState      Capability = "sequence-state"
	CapabilitySequenceBinding    Capability = "sequence-binding"
	CapabilityMigrationPrecheck  Capability = "migration-precheck"
	CapabilityValidationSnapshot Capability = "validation-snapshot"
)

// Descriptor is the stable QMigration Connector SPI contract advertised by a
// native connector family.  Planner/Worker code uses capabilities instead of
// database-name branches, so a new database can enter the unified engine by
// implementing the same roles without adding another migration runtime.
type Descriptor struct {
	Type                  domain.DataSourceType `json:"type"`
	Protocol              string                `json:"protocol"`
	Capabilities          []Capability          `json:"capabilities"`
	Native                bool                  `json:"native"`
	Maturity              Maturity              `json:"maturity,omitempty"`
	QualificationRequired bool                  `json:"qualification_required,omitempty"`
	Note                  string                `json:"note,omitempty"`
}

func (d Descriptor) Has(c Capability) bool {
	for _, item := range d.Capabilities {
		if item == c {
			return true
		}
	}
	return false
}

// CapabilityFactory lets one protocol implementation advertise different
// feature sets for compatible database products (for example PostgreSQL vs an
// openGauss-compatible full-load connection).  Factory.New remains the runtime
// constructor to preserve the small Connector SPI.
type CapabilityFactory interface {
	Capabilities(domain.DataSourceType) Descriptor
}

type Value struct {
	Null bool
	Raw  []byte
}

type RowBatch struct {
	Rows    [][]Value
	LastPK  int64
	LastKey []Value
	Bytes   int64
}

type ReadBatchRequest struct {
	Schema      string
	Table       string
	PrimaryKey  string
	PrimaryKeys []string
	Columns     []domain.ColumnInfo
	Cursor      []Value
	LowerBound  []Value
	UpperBound  []Value
	UseKeyset   bool
	StartPK     int64
	EndPK       int64
	AfterPK     int64
	HasAfter    bool
	Limit       int
	Partition   string
	HashBucket  int
	HashBuckets int
	CustomWhere string
}

type KeysetBoundaryRequest struct {
	Schema     string
	Table      string
	Keys       []string
	Columns    []domain.ColumnInfo
	Partitions int
	// LowerBound/UpperBound optionally restrict planning to an existing
	// durable keyset chunk. They use the same [lower, upper) semantics as
	// ReadBatch so adaptive refinement can split only unclaimed work.
	LowerBound []Value
	UpperBound []Value
}

type WriteBatchRequest struct {
	Schema      string
	Table       string
	Columns     []domain.ColumnInfo
	Rows        [][]Value
	PrimaryKey  string
	PrimaryKeys []string
}

type ReadByKeyRequest struct {
	Schema      string
	Table       string
	PrimaryKeys []string
	KeyColumns  []domain.ColumnInfo
	KeyValues   []Value
	Columns     []domain.ColumnInfo
}

type DeleteByKeyRequest struct {
	Schema      string
	Table       string
	PrimaryKey  string
	Column      domain.ColumnInfo
	Value       Value
	PrimaryKeys []string
	Columns     []domain.ColumnInfo
	Values      []Value
}

type Connector interface {
	TestConnection(context.Context) error
	GetVersion(context.Context) (string, error)
	ListSchemas(context.Context) ([]domain.SchemaInfo, error)
	ListTables(context.Context, string) ([]domain.TableInfo, error)
	GetTableMetadata(context.Context, string, string) (*domain.TableMetadata, error)
	Close() error
}

type DataConnector interface {
	Connector
	ReadBatch(context.Context, ReadBatchRequest) (*RowBatch, error)
	WriteBatch(context.Context, WriteBatchRequest) (int64, error)
}

// ValidationSnapshotLevel tells the migration service how strongly a source can
// bind validation reads to a durable CDC watermark. EXACT_HISTORICAL is the only
// level that may be used as a historical snapshot. FENCED_CURRENT means the
// connector can prove a quiescent/current read fence but cannot time-travel.
type ValidationSnapshotLevel string

const (
	ValidationSnapshotExactHistorical ValidationSnapshotLevel = "EXACT_HISTORICAL"
	ValidationSnapshotFencedCurrent   ValidationSnapshotLevel = "FENCED_CURRENT"
	ValidationSnapshotUnavailable     ValidationSnapshotLevel = "UNAVAILABLE"
)

type ValidationSnapshotCapability struct {
	Level         ValidationSnapshotLevel `json:"level"`
	PositionTypes []string                `json:"position_types,omitempty"`
	Note          string                  `json:"note,omitempty"`
}

type ValidationSnapshotCapabilityProvider interface {
	ValidationSnapshotCapability(context.Context, domain.CDCPosition) ValidationSnapshotCapability
}

// ValidationSnapshotConnector opens an independent read-only data connector pinned
// to the exact durable CDC watermark supplied by the migration service. The
// returned connector must keep that historical snapshot for all ReadBatch calls
// until Close. Implementations must fail closed when the source cannot prove an
// exact historical view at the requested position.
type ValidationSnapshotConnector interface {
	DataConnector
	OpenValidationSnapshot(context.Context, domain.CDCPosition) (DataConnector, error)
}

// KeysetBoundaryConnector exposes approximate ordered split points for a stable
// migration key. It is optional: callers must safely fall back to one durable
// keyset stream when a database version cannot produce ordered boundaries.
type KeysetBoundaryConnector interface {
	PlanKeysetBoundaries(context.Context, KeysetBoundaryRequest) ([][]Value, error)
}

// PartitionConnector exposes physical/logical table partitions that can be
// migrated independently without OFFSET scans. Implementations must return
// stable partition identifiers accepted by ReadBatch.Partition.
type PartitionConnector interface {
	Connector
	ListTablePartitions(context.Context, string, string) ([]string, error)
}

// TableTopologyConnector discovers vendor topology for advisory placement.
// The returned labels are soft hints only: logical SQL routing can differ from
// physical storage placement, so correctness must never depend on a match.
type TableTopologyConnector interface {
	DiscoverTableTopology(context.Context, string, string) ([]domain.TopologyPlacement, error)
}

// ChunkRelocationRequest describes a remainder that may move from one physical
// source ownership domain to another after a durable batch boundary. Direct-DN
// readers must prove replica identity/epoch/position continuity; coordinator-
// routed connectors may instead return RoutingTransparent=true.
type ChunkRelocationRequest struct {
	Schema, Table                string
	FromTopologyID, ToTopologyID string
	SplitType, CursorJSON        string
}

type ChunkRelocationProof struct {
	Safe               bool
	RoutingTransparent bool
	ShardID            string
	Epoch              string
	Position           string
	Reason             string
}

type ReplicaRelocationConnector interface {
	ProveChunkRelocation(context.Context, ChunkRelocationRequest) (ChunkRelocationProof, error)
}

// RuntimeLoadConnector exposes inexpensive server-side pressure signals used to
// adjust task-level effective parallelism. Unsupported metrics remain zero.
type RuntimeLoadConnector interface {
	SampleRuntimeLoad(context.Context) (domain.DatabaseRuntimeLoad, error)
}

// CDCApplyConnector is the target-side native apply contract. Upserts reuse
// WriteBatch while deletes need a primary-key delete primitive. Replaying the
// same event remains idempotent.
type CDCApplyConnector interface {
	DataConnector
	DeleteByKey(context.Context, DeleteByKeyRequest) error
}

// TruncateTableConnector applies a source TRUNCATE as a target-side table
// primitive. Implementations used by CDC must preserve the active target
// transaction: QMigration invokes TRUNCATE only as the final data operation
// before COMMIT because GBase 8s permits no later SQL in that transaction.
type TruncateTableConnector interface {
	Connector
	TruncateTable(context.Context, string, string) error
}

// PointLookupConnector reads the current target row by primary key inside the
// connector's active CDC transaction. LAST_WRITE_WINS uses it for version
// comparison before applying INSERT/UPDATE row images.
type PointLookupConnector interface {
	Connector
	ReadByKey(context.Context, ReadByKeyRequest) ([]Value, bool, error)
}

// TransactionalCDCApplyConnector lets the control plane apply a source
// transaction atomically on the target. Connectors are created per apply
// request, so BEGIN/COMMIT operate on the connector's dedicated session.
type TransactionalCDCApplyConnector interface {
	CDCApplyConnector
	BeginCDCTransaction(context.Context) error
	CommitCDCTransaction(context.Context) error
	RollbackCDCTransaction(context.Context) error
}

// DDLApplyConnector executes a source DDL statement in a selected schema. The
// migration service only uses this for explicitly enabled same-family replay;
// heterogeneous DDL remains a schema-conversion responsibility.
type DDLApplyConnector interface {
	Connector
	ExecDDL(context.Context, string, string) error
}

type CDCSource interface {
	Connector
	CurrentCDCPosition(context.Context) (*domain.CDCPosition, error)
}

// CDCSelectionPositionSource captures a restart position using the selected
// source table set. Some vendor CDC APIs (notably GBase 8s/Informix-style
// syscdcv1 sessions) can establish a precise restart sequence only after the
// capture tables have been registered. The migration service prefers this
// contract over the selection-independent CDCSource when available.
type CDCSelectionPositionSource interface {
	Connector
	CurrentCDCPositionForSelection(context.Context, []domain.TableMapping) (*domain.CDCPosition, error)
}

// CDCSelectionValidator validates source-side prerequisites that depend on the
// selected table set (for example SQL Server CDC capture instances). It runs
// before Full Load starts so a missing capture configuration cannot be
// discovered only after hours of snapshot copying.
type CDCSelectionValidator interface {
	ValidateCDCSelection(context.Context, []domain.TableMapping) error
}

// CDCCheckpointSource can reserve a durable log checkpoint before full load.
// PostgreSQL uses a logical replication slot so WAL cannot be recycled before
// a managed CDC engine starts consuming it.
type CDCCheckpointSource interface {
	CDCSource
	CreateCDCCheckpoint(context.Context, string) (*domain.CDCPosition, error)
	DropCDCCheckpoint(context.Context, string) error
}

// RawCDCStream exposes raw vendor log events. The MySQL native connector uses
// this for COM_BINLOG_DUMP while decoding remains in the CDC package.
type RawCDCStream interface {
	Next(context.Context) ([]byte, error)
	Close() error
}

type MySQLBinlogSource interface {
	CDCSource
	CurrentBinlogPosition(context.Context) (*domain.CDCPosition, error)
	OpenBinlogStream(context.Context, string, uint32, uint32) (RawCDCStream, error)
	OpenBinlogGTIDStream(context.Context, string, uint32) (RawCDCStream, error)
}

type PostgreSQLLogicalStream interface {
	RawCDCStream
	Acknowledge(context.Context, string) error
}

type PostgreSQLLogicalSource interface {
	CDCCheckpointSource
	OpenLogicalReplicationStream(context.Context, string, string, string) (PostgreSQLLogicalStream, error)
}

type SchemaConnector interface {
	Connector
	EnsureSchema(context.Context, string) error
	CreateTable(context.Context, string, string, []domain.ColumnInfo, string) error
}

// CompositeSchemaConnector extends table creation with multi-column primary keys.
type CompositeSchemaConnector interface {
	SchemaConnector
	CreateTableWithPrimaryKeys(context.Context, string, string, []domain.ColumnInfo, []string) error
}

// SchemaObjectConnector discovers non-table schema objects such as views,
// sequences, triggers and routines. It is optional until each database family
// implements the corresponding QMigration native catalog adapter.
type SchemaObjectConnector interface {
	Connector
	ListSchemaObjects(context.Context, string) ([]domain.SchemaObject, error)
}

// SequenceStateConnector synchronizes the runtime position of a PostgreSQL-
// style sequence after its DDL has been created. Values are strings because a
// database sequence may approach the signed 64-bit boundary and should not be
// rounded through JSON numbers.
type SequenceStateConnector interface {
	Connector
	GetSequenceState(context.Context, string, string) (lastValue string, isCalled bool, err error)
	SetSequenceState(context.Context, string, string, string, bool) error
}

// SequenceBindingConnector restores SERIAL-style ownership/default semantics on
// PostgreSQL-family targets after the backing sequence has been created.
type SequenceBindingConnector interface {
	SequenceStateConnector
	BindSequence(context.Context, string, string, string, string) error
}

// MigrationPrecheckConnector exposes database-specific configuration checks that
// cannot be inferred from a successful TCP/login test alone.
type MigrationPrecheckConnector interface {
	Connector
	MigrationPrechecks(context.Context, bool) []domain.PrecheckItem
}

// PostLoadSchemaConnector applies secondary indexes and referential constraints
// after bulk data load. Implementations should leave primary-key creation to
// CreateTable/CreateTableWithPrimaryKeys.
type PostLoadSchemaConnector interface {
	Connector
	CreateIndex(context.Context, string, string, domain.IndexInfo) error
	CreateForeignKey(context.Context, string, string, domain.ForeignKeyInfo) error
}

// GeneratedValueStateConnector synchronizes database-managed generators after
// QMigration copied explicit source values. It is intentionally separate from
// schema creation: identity/sequence state is a cutover correctness concern and
// must run only after all concurrent Full Load chunks for the table have finished.
type GeneratedValueStateConnector interface {
	Connector
	SyncGeneratedValueState(context.Context, string, string, []domain.ColumnInfo) error
}

// CutoverGeneratedValueConnector restores production generation semantics after
// the migration writer no longer needs to propagate explicit generated values.
// Implementations must be safe to call repeatedly. QMigration invokes it for
// full-only tasks before FINISHED and for full+incremental tasks inside the
// cutover critical section after native CDC capture has stopped.
type CutoverGeneratedValueConnector interface {
	Connector
	FinalizeGeneratedValueModes(context.Context, string, string, []domain.ColumnInfo) error
}

type Factory interface {
	New(domain.DataSource) (Connector, error)
}

type Registry struct {
	factories map[domain.DataSourceType]Factory
}

func NewRegistry() *Registry                                    { return &Registry{factories: map[domain.DataSourceType]Factory{}} }
func (r *Registry) Register(t domain.DataSourceType, f Factory) { r.factories[t] = f }
func (r *Registry) Descriptor(t domain.DataSourceType) (Descriptor, error) {
	f, ok := r.factories[t]
	if !ok {
		return Descriptor{}, fmt.Errorf("unsupported datasource type: %s", t)
	}
	if cf, ok := f.(CapabilityFactory); ok {
		d := cf.Capabilities(t)
		if d.Type == "" {
			d.Type = t
		}
		return d, nil
	}
	return Descriptor{Type: t, Protocol: "unknown", Capabilities: []Capability{CapabilityMetadata}, Native: true}, nil
}
func (r *Registry) Descriptors() []Descriptor {
	types := make([]string, 0, len(r.factories))
	for t := range r.factories {
		types = append(types, string(t))
	}
	sort.Strings(types)
	out := make([]Descriptor, 0, len(types))
	for _, raw := range types {
		if d, err := r.Descriptor(domain.DataSourceType(raw)); err == nil {
			out = append(out, d)
		}
	}
	return out
}

func (r *Registry) Supports(t domain.DataSourceType, c Capability) bool {
	d, err := r.Descriptor(t)
	return err == nil && d.Has(c)
}
func (r *Registry) Require(t domain.DataSourceType, c Capability) error {
	f, ok := r.factories[t]
	if !ok {
		return fmt.Errorf("unsupported datasource type: %s", t)
	}
	// Custom connector factories created before the capability SPI remain usable.
	// All production built-in factories implement CapabilityFactory, so feature
	// gating remains strict for QMigration-supported datasource types.
	if _, ok := f.(CapabilityFactory); !ok {
		return nil
	}
	d, err := r.Descriptor(t)
	if err != nil {
		return err
	}
	if !d.Has(c) {
		return fmt.Errorf("%w: datasource %s does not provide %s", ErrCapabilityUnavailable, t, c)
	}
	return nil
}
func (r *Registry) New(d domain.DataSource) (Connector, error) {
	f, ok := r.factories[d.Type]
	if !ok {
		return nil, fmt.Errorf("unsupported datasource type: %s", d.Type)
	}
	return f.New(d)
}
