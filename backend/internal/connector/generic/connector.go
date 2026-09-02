package generic

import (
	"context"
	"fmt"
	"net"
	"qmigration/backend/internal/connector"
	"qmigration/backend/internal/domain"
	"time"
)

type Factory struct{}

func NewFactory() *Factory { return &Factory{} }

func (*Factory) Capabilities(t domain.DataSourceType) connector.Descriptor {
	return connector.Descriptor{
		Type: t, Protocol: "tcp-probe", Native: false,
		Capabilities: nil,
		Maturity:     connector.MaturityProbeOnly,
		Note:         "connection probe only; a QMigration native connector implementation is required before migration",
	}
}
func (*Factory) New(ds domain.DataSource) (connector.Connector, error) {
	return &Connector{ds: ds}, nil
}

type Connector struct{ ds domain.DataSource }

func (c *Connector) TestConnection(ctx context.Context) error {
	if c.ds.Host == "" || c.ds.Port <= 0 {
		return fmt.Errorf("host/port are required")
	}
	d := net.Dialer{Timeout: 5 * time.Second}
	conn, err := d.DialContext(ctx, "tcp", fmt.Sprintf("%s:%d", c.ds.Host, c.ds.Port))
	if err != nil {
		return err
	}
	return conn.Close()
}
func (*Connector) GetVersion(context.Context) (string, error) { return "native-connector-pending", nil }
func (*Connector) ListSchemas(context.Context) ([]domain.SchemaInfo, error) {
	return nil, connector.ErrMetadataUnavailable
}
func (*Connector) ListTables(context.Context, string) ([]domain.TableInfo, error) {
	return nil, connector.ErrMetadataUnavailable
}
func (*Connector) GetTableMetadata(context.Context, string, string) (*domain.TableMetadata, error) {
	return nil, connector.ErrMetadataUnavailable
}
func (*Connector) Close() error { return nil }
