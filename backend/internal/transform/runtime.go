package transform

import (
	"bytes"
	"encoding/json"
	"fmt"
	"qmigration/backend/internal/connector"
	"qmigration/backend/internal/domain"
	"qmigration/backend/internal/schema"
	"strings"
)

// ColumnPlan is a compiled value-level transform between one source column and
// one target column. DDL conversion and row-value conversion intentionally use
// the same Universal Schema Model so heterogeneous migrations do not depend on
// source-protocol formatting quirks.
type ColumnPlan struct {
	Source domain.ColumnInfo
	Target domain.ColumnInfo
	From   domain.UniversalDataType
	To     domain.UniversalDataType
	Rules  []domain.TransformRule
}

type Plan struct {
	Columns []ColumnPlan
}

func Compile(source, target []domain.ColumnInfo) (*Plan, error) {
	return CompileWithRules(source, target, nil, "", "")
}

// CompileWithRules compiles task-level declarative transform rules into the
// connector-neutral row pipeline. Empty rule schema/table fields are wildcards.
func CompileWithRules(source, target []domain.ColumnInfo, rules []domain.TransformRule, sourceSchema, sourceTable string) (*Plan, error) {
	if len(source) != len(target) {
		return nil, fmt.Errorf("transform requires one-to-one column mapping: source=%d target=%d", len(source), len(target))
	}
	if err := ValidateRules(rules); err != nil {
		return nil, err
	}
	p := &Plan{Columns: make([]ColumnPlan, len(source))}
	for i := range source {
		p.Columns[i] = ColumnPlan{
			Source: source[i], Target: target[i],
			From:  schema.NormalizeType(source[i]).Type,
			To:    schema.NormalizeType(target[i]).Type,
			Rules: matchingRules(rules, sourceSchema, sourceTable, source[i].Name),
		}
	}
	return p, nil
}

func ValidateRules(rules []domain.TransformRule) error {
	for i, r := range rules {
		if strings.TrimSpace(r.Column) == "" {
			return fmt.Errorf("transform_rules[%d].column is required", i)
		}
		switch r.Action {
		case domain.TransformTrim, domain.TransformLower, domain.TransformUpper,
			domain.TransformEmptyToNull, domain.TransformZeroDateToNull, domain.TransformJSONCompact:
		case domain.TransformNullToValue, domain.TransformZeroDateToValue:
			if r.Value == "" {
				return fmt.Errorf("transform_rules[%d].value is required for %s", i, r.Action)
			}
		case domain.TransformReplaceLiteral:
			if r.Match == "" {
				return fmt.Errorf("transform_rules[%d].match is required for %s", i, r.Action)
			}
		default:
			return fmt.Errorf("transform_rules[%d] unsupported action %q", i, r.Action)
		}
	}
	return nil
}

func matchingRules(rules []domain.TransformRule, schemaName, tableName, column string) []domain.TransformRule {
	out := make([]domain.TransformRule, 0)
	for _, r := range rules {
		if !strings.EqualFold(strings.TrimSpace(r.Column), strings.TrimSpace(column)) {
			continue
		}
		if v := strings.TrimSpace(r.SourceSchema); v != "" && !strings.EqualFold(v, schemaName) {
			continue
		}
		if v := strings.TrimSpace(r.SourceTable); v != "" && !strings.EqualFold(v, tableName) {
			continue
		}
		out = append(out, r)
	}
	return out
}

func (p *Plan) TransformRows(rows [][]connector.Value) ([][]connector.Value, error) {
	if p == nil {
		return nil, fmt.Errorf("nil transform plan")
	}
	out := make([][]connector.Value, len(rows))
	for ri, row := range rows {
		if len(row) != len(p.Columns) {
			return nil, fmt.Errorf("row %d has %d values; expected %d", ri, len(row), len(p.Columns))
		}
		out[ri] = make([]connector.Value, len(row))
		for ci, value := range row {
			v, err := transformValue(value, p.Columns[ci])
			if err != nil {
				return nil, fmt.Errorf("column %s row %d: %w", p.Columns[ci].Source.Name, ri, err)
			}
			out[ri][ci] = v
		}
	}
	return out, nil
}

func clone(v connector.Value) connector.Value {
	return connector.Value{Null: v.Null, Raw: append([]byte(nil), v.Raw...)}
}

func parseBool(raw []byte) (bool, error) {
	s := strings.ToLower(strings.TrimSpace(string(raw)))
	switch s {
	case "1", "t", "true", "y", "yes", "on":
		return true, nil
	case "0", "f", "false", "n", "no", "off":
		return false, nil
	default:
		return false, fmt.Errorf("cannot normalize boolean value %q", string(raw))
	}
}

func isIntegerType(t domain.UniversalDataType) bool {
	return t == domain.UniversalInteger || t == domain.UniversalBigInt
}

func zeroDate(raw []byte) bool {
	s := strings.TrimSpace(string(raw))
	return strings.HasPrefix(s, "0000-00-00")
}

func applyRule(v connector.Value, r domain.TransformRule) (connector.Value, error) {
	if v.Null {
		if r.Action == domain.TransformNullToValue {
			return connector.Value{Raw: []byte(r.Value)}, nil
		}
		return v, nil
	}

	s := string(v.Raw)
	switch r.Action {
	case domain.TransformTrim:
		v.Raw = []byte(strings.TrimSpace(s))
	case domain.TransformLower:
		v.Raw = []byte(strings.ToLower(s))
	case domain.TransformUpper:
		v.Raw = []byte(strings.ToUpper(s))
	case domain.TransformEmptyToNull:
		if len(v.Raw) == 0 {
			return connector.Value{Null: true}, nil
		}
	case domain.TransformReplaceLiteral:
		v.Raw = []byte(strings.ReplaceAll(s, r.Match, r.Value))
	case domain.TransformZeroDateToNull:
		if zeroDate(v.Raw) {
			return connector.Value{Null: true}, nil
		}
	case domain.TransformZeroDateToValue:
		if zeroDate(v.Raw) {
			v.Raw = []byte(r.Value)
		}
	case domain.TransformJSONCompact:
		var compact bytes.Buffer
		if err := json.Compact(&compact, v.Raw); err != nil {
			return connector.Value{}, fmt.Errorf("JSON_COMPACT: %w", err)
		}
		v.Raw = append([]byte(nil), compact.Bytes()...)
	case domain.TransformNullToValue:
		// Non-null input is intentionally unchanged.
	default:
		return connector.Value{}, fmt.Errorf("unsupported transform action %q", r.Action)
	}
	return v, nil
}

func transformValue(v connector.Value, c ColumnPlan) (connector.Value, error) {
	v = clone(v)
	for _, r := range c.Rules {
		var err error
		v, err = applyRule(v, r)
		if err != nil {
			return connector.Value{}, err
		}
	}
	if v.Null {
		return connector.Value{Null: true}, nil
	}

	// Boolean representations differ across MySQL/PostgreSQL protocol text
	// results. Normalize only when the target type needs it; do not rewrite
	// arbitrary integer columns because 2/3/etc. are valid integer values.
	if c.From == domain.UniversalBoolean && c.To == domain.UniversalBoolean {
		b, err := parseBool(v.Raw)
		if err != nil {
			return connector.Value{}, err
		}
		if b {
			return connector.Value{Raw: []byte("true")}, nil
		}
		return connector.Value{Raw: []byte("false")}, nil
	}
	if c.From == domain.UniversalBoolean && isIntegerType(c.To) {
		b, err := parseBool(v.Raw)
		if err != nil {
			return connector.Value{}, err
		}
		if b {
			return connector.Value{Raw: []byte("1")}, nil
		}
		return connector.Value{Raw: []byte("0")}, nil
	}
	if isIntegerType(c.From) && c.To == domain.UniversalBoolean {
		s := strings.TrimSpace(string(v.Raw))
		switch s {
		case "0":
			return connector.Value{Raw: []byte("false")}, nil
		case "1":
			return connector.Value{Raw: []byte("true")}, nil
		default:
			return connector.Value{}, fmt.Errorf("integer %q cannot be safely converted to boolean; only 0/1 are accepted", s)
		}
	}

	// JSON is transported as text by the current native protocols. Validate it
	// before handing it to a JSON/JSONB sink so bad source bytes fail the chunk
	// instead of being silently checkpointed after a lossy conversion.
	if c.To == domain.UniversalJSON {
		trimmed := bytes.TrimSpace(v.Raw)
		if !json.Valid(trimmed) {
			return connector.Value{}, fmt.Errorf("invalid JSON value")
		}
		return connector.Value{Raw: append([]byte(nil), trimmed...)}, nil
	}

	// PostgreSQL cannot represent MySQL's zero DATE/TIMESTAMP values. Failing is
	// deliberate unless the task declared ZERO_DATE_TO_NULL/ZERO_DATE_TO_VALUE.
	if (c.To == domain.UniversalDate || c.To == domain.UniversalTimestamp) && zeroDate(v.Raw) {
		return connector.Value{}, fmt.Errorf("zero date/time requires an explicit transform policy")
	}

	// Decimal/integer/string/UUID/binary values already use connector-neutral
	// byte images. Sink connectors render them according to target metadata.
	return clone(v), nil
}
