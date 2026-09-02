package validation

import (
	"context"
	"qmigration/backend/internal/connector"
	"qmigration/backend/internal/domain"
	"strings"
	"testing"
)

type fake struct{ rows [][]connector.Value }

func (f *fake) TestConnection(context.Context) error                           { return nil }
func (f *fake) GetVersion(context.Context) (string, error)                     { return "", nil }
func (f *fake) ListSchemas(context.Context) ([]domain.SchemaInfo, error)       { return nil, nil }
func (f *fake) ListTables(context.Context, string) ([]domain.TableInfo, error) { return nil, nil }
func (f *fake) GetTableMetadata(context.Context, string, string) (*domain.TableMetadata, error) {
	return nil, nil
}
func (f *fake) Close() error                                                           { return nil }
func (f *fake) WriteBatch(context.Context, connector.WriteBatchRequest) (int64, error) { return 0, nil }
func (f *fake) ReadBatch(_ context.Context, r connector.ReadBatchRequest) (*connector.RowBatch, error) {
	start := 0
	if r.HasAfter {
		for start < len(f.rows) && string(f.rows[start][0].Raw) <= fmtInt(r.AfterPK) {
			start++
		}
	}
	if start >= len(f.rows) {
		return &connector.RowBatch{}, nil
	}
	end := start + r.Limit
	if end > len(f.rows) {
		end = len(f.rows)
	}
	last := int64(start + 1)
	if end > start {
		last = int64(end)
	}
	return &connector.RowBatch{Rows: f.rows[start:end], LastPK: last}, nil
}
func fmtInt(v int64) string {
	if v == 0 {
		return "0"
	}
	b := []byte{}
	for v > 0 {
		b = append([]byte{byte('0' + v%10)}, b...)
		v /= 10
	}
	return string(b)
}
func TestRangeDeterministic(t *testing.T) {
	f := &fake{rows: [][]connector.Value{{{Raw: []byte("1")}, {Raw: []byte("a")}}, {{Raw: []byte("2")}, {Raw: []byte("b")}}}}
	c1, e := Range(context.Background(), f, "s", "t", "id", []domain.ColumnInfo{{Name: "id"}, {Name: "v"}}, 1, 2, 100)
	if e != nil {
		t.Fatal(e)
	}
	c2, _ := Range(context.Background(), f, "s", "t", "id", []domain.ColumnInfo{{Name: "id"}, {Name: "v"}}, 1, 2, 1)
	if c1 != c2 {
		t.Fatalf("%+v != %+v", c1, c2)
	}
}

func TestCanonicalValueCrossDatabase(t *testing.T) {
	cases := []struct {
		col  domain.ColumnInfo
		a, b []byte
	}{
		{domain.ColumnInfo{DataType: "tinyint", ColumnType: "tinyint(1)"}, []byte("1"), []byte("true")},
		{domain.ColumnInfo{DataType: "decimal"}, []byte("1.00"), []byte("1")},
		{domain.ColumnInfo{DataType: "json"}, []byte(`{"b":2,"a":1}`), []byte(`{"a":1,"b":2}`)},
		{domain.ColumnInfo{DataType: "uuid"}, []byte("ABCDEF00-0000-0000-0000-000000000000"), []byte("abcdef00-0000-0000-0000-000000000000")},
	}
	for _, tc := range cases {
		if string(canonicalValue(tc.col, tc.a)) != string(canonicalValue(tc.col, tc.b)) {
			t.Fatalf("not canonicalized: %q != %q", tc.a, tc.b)
		}
	}
}

type keysetFake struct{ rows [][]connector.Value }

func (f *keysetFake) TestConnection(context.Context) error                           { return nil }
func (f *keysetFake) GetVersion(context.Context) (string, error)                     { return "", nil }
func (f *keysetFake) ListSchemas(context.Context) ([]domain.SchemaInfo, error)       { return nil, nil }
func (f *keysetFake) ListTables(context.Context, string) ([]domain.TableInfo, error) { return nil, nil }
func (f *keysetFake) GetTableMetadata(context.Context, string, string) (*domain.TableMetadata, error) {
	return nil, nil
}
func (f *keysetFake) Close() error { return nil }
func (f *keysetFake) WriteBatch(context.Context, connector.WriteBatchRequest) (int64, error) {
	return 0, nil
}
func (f *keysetFake) ReadBatch(_ context.Context, r connector.ReadBatchRequest) (*connector.RowBatch, error) {
	start := 0
	lower := r.LowerBound
	if len(r.Cursor) > 0 {
		lower = r.Cursor
	}
	if len(lower) > 0 {
		for start < len(f.rows) {
			cmp := strings.Compare(string(f.rows[start][0].Raw), string(lower[0].Raw))
			if (len(r.Cursor) > 0 && cmp > 0) || (len(r.Cursor) == 0 && cmp >= 0) {
				break
			}
			start++
		}
	}
	if start >= len(f.rows) {
		return &connector.RowBatch{}, nil
	}
	end := start
	for end < len(f.rows) && end-start < r.Limit {
		if len(r.UpperBound) > 0 && strings.Compare(string(f.rows[end][0].Raw), string(r.UpperBound[0].Raw)) >= 0 {
			break
		}
		end++
	}
	if end == start {
		return &connector.RowBatch{}, nil
	}
	last := f.rows[end-1][0]
	return &connector.RowBatch{Rows: f.rows[start:end], LastKey: []connector.Value{{Raw: append([]byte(nil), last.Raw...)}}}, nil
}

func TestKeysetCanonicalDeterministic(t *testing.T) {
	rows := [][]connector.Value{{{Raw: []byte("a")}, {Raw: []byte("1.00")}}, {{Raw: []byte("b")}, {Raw: []byte("2")}}, {{Raw: []byte("z")}, {Raw: []byte("3.0")}}}
	f := &keysetFake{rows: rows}
	cols := []domain.ColumnInfo{{Name: "id", DataType: "varchar", PrimaryKey: true}, {Name: "v", DataType: "decimal"}}
	a, err := KeysetCanonical(context.Background(), f, "s", "t", []string{"id"}, cols, cols, 1)
	if err != nil {
		t.Fatal(err)
	}
	b, err := KeysetCanonical(context.Background(), f, "s", "t", []string{"id"}, cols, cols, 10)
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatalf("%+v != %+v", a, b)
	}
	if a.Rows != 3 {
		t.Fatalf("rows=%d", a.Rows)
	}
}

func TestKeysetCanonicalBounded(t *testing.T) {
	rows := [][]connector.Value{
		{{Raw: []byte("a")}, {Raw: []byte("1")}},
		{{Raw: []byte("b")}, {Raw: []byte("2")}},
		{{Raw: []byte("c")}, {Raw: []byte("3")}},
		{{Raw: []byte("d")}, {Raw: []byte("4")}},
	}
	f := &keysetFake{rows: rows}
	cols := []domain.ColumnInfo{{Name: "id", DataType: "varchar", PrimaryKey: true}, {Name: "v", DataType: "integer"}}
	got, err := KeysetCanonicalBounded(context.Background(), f, "s", "t", []string{"id"}, cols, cols, []connector.Value{{Raw: []byte("b")}}, []connector.Value{{Raw: []byte("d")}}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if got.Rows != 2 {
		t.Fatalf("expected [b,d) to contain 2 rows, got %+v", got)
	}
}

func TestRC44UnorderedAccumulatorStreamsDescriptorsWithoutChangingChecksum(t *testing.T) {
	rows := [][]connector.Value{
		{{Raw: []byte("a")}, {Raw: []byte("1.00")}},
		{{Raw: []byte("b")}, {Raw: []byte("2")}},
		{{Raw: []byte("c")}, {Raw: []byte("3.0")}},
		{{Raw: []byte("d")}, {Raw: []byte("4")}},
	}
	f := &keysetFake{rows: rows}
	cols := []domain.ColumnInfo{{Name: "id", DataType: "varchar", PrimaryKey: true}, {Name: "v", DataType: "decimal"}}
	all, err := UnorderedCanonicalRequests(context.Background(), f, []connector.ReadBatchRequest{{Schema: "s", Table: "t", PrimaryKey: "id", PrimaryKeys: []string{"id"}, UseKeyset: true}}, cols, cols, 2)
	if err != nil {
		t.Fatal(err)
	}
	acc := NewUnorderedAccumulator()
	if err := acc.AddRequest(context.Background(), f, connector.ReadBatchRequest{Schema: "s", Table: "t", PrimaryKey: "id", PrimaryKeys: []string{"id"}, UseKeyset: true, LowerBound: []connector.Value{{Raw: []byte("a")}}, UpperBound: []connector.Value{{Raw: []byte("c")}}}, cols, cols, 1); err != nil {
		t.Fatal(err)
	}
	if err := acc.AddRequest(context.Background(), f, connector.ReadBatchRequest{Schema: "s", Table: "t", PrimaryKey: "id", PrimaryKeys: []string{"id"}, UseKeyset: true, LowerBound: []connector.Value{{Raw: []byte("c")}}}, cols, cols, 3); err != nil {
		t.Fatal(err)
	}
	streamed := acc.Checksum()
	if streamed != all {
		t.Fatalf("streamed descriptor checksum %+v != single request %+v", streamed, all)
	}
}
