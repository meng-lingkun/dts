package sqlserverconnector

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"qmigration/backend/internal/connector"
	"qmigration/backend/internal/domain"
)

func sqlServerSchemaObjectType(raw string) (domain.SchemaObjectType, bool) {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case "V":
		return domain.SchemaObjectView, true
	case "SO":
		return domain.SchemaObjectSequence, true
	case "TR":
		return domain.SchemaObjectTrigger, true
	case "P", "PC", "X":
		return domain.SchemaObjectProcedure, true
	case "FN", "IF", "TF", "FS", "FT":
		return domain.SchemaObjectFunction, true
	default:
		return "", false
	}
}

func sqlServerSequenceDDL(schema, name, dataType, start, increment, minimum, maximum string, cycling, cached bool, cacheSize string) (string, error) {
	if strings.TrimSpace(schema) == "" || strings.TrimSpace(name) == "" {
		return "", errors.New("sequence schema/name is empty")
	}
	if err := connector.ValidateNumericLiteral([]byte(start), false); err != nil {
		return "", fmt.Errorf("sequence start: %w", err)
	}
	if err := connector.ValidateNumericLiteral([]byte(increment), false); err != nil {
		return "", fmt.Errorf("sequence increment: %w", err)
	}
	if strings.TrimSpace(minimum) != "" {
		if err := connector.ValidateNumericLiteral([]byte(minimum), false); err != nil {
			return "", fmt.Errorf("sequence minimum: %w", err)
		}
	}
	if strings.TrimSpace(maximum) != "" {
		if err := connector.ValidateNumericLiteral([]byte(maximum), false); err != nil {
			return "", fmt.Errorf("sequence maximum: %w", err)
		}
	}
	t := strings.ToLower(strings.TrimSpace(dataType))
	switch t {
	case "tinyint", "smallint", "int", "bigint":
	case "decimal", "numeric":
		t = "decimal(38,0)"
	default:
		t = "bigint"
	}
	var b strings.Builder
	b.WriteString("CREATE SEQUENCE ")
	b.WriteString(qIdentSafe(schema))
	b.WriteByte('.')
	b.WriteString(qIdentSafe(name))
	b.WriteString(" AS ")
	b.WriteString(t)
	b.WriteString(" START WITH ")
	b.WriteString(strings.TrimSpace(start))
	b.WriteString(" INCREMENT BY ")
	b.WriteString(strings.TrimSpace(increment))
	if strings.TrimSpace(minimum) == "" {
		b.WriteString(" NO MINVALUE")
	} else {
		b.WriteString(" MINVALUE ")
		b.WriteString(strings.TrimSpace(minimum))
	}
	if strings.TrimSpace(maximum) == "" {
		b.WriteString(" NO MAXVALUE")
	} else {
		b.WriteString(" MAXVALUE ")
		b.WriteString(strings.TrimSpace(maximum))
	}
	if cycling {
		b.WriteString(" CYCLE")
	} else {
		b.WriteString(" NO CYCLE")
	}
	if cached {
		if n, err := strconv.ParseInt(strings.TrimSpace(cacheSize), 10, 64); err == nil && n > 0 {
			b.WriteString(" CACHE ")
			b.WriteString(strconv.FormatInt(n, 10))
		} else {
			b.WriteString(" CACHE")
		}
	} else {
		b.WriteString(" NO CACHE")
	}
	return b.String(), nil
}

// ListSchemaObjects discovers SQL modules and standalone sequences. Routine and
// trigger DDL is intentionally discovered but remains manual in the migration
// planner; identity-mapped same-family views can be auto-applied only when the
// dependency catalog was read successfully.
func (c *Connector) ListSchemaObjects(ctx context.Context, schema string) ([]domain.SchemaObject, error) {
	p, err := c.get(ctx)
	if err != nil {
		return nil, err
	}
	q := `SELECT CONVERT(nvarchar(10),o.type),CONVERT(nvarchar(4000),o.name),
COALESCE(CONVERT(nvarchar(4000),OBJECT_SCHEMA_NAME(o.parent_object_id)),N''),
COALESCE(CONVERT(nvarchar(4000),OBJECT_NAME(o.parent_object_id)),N''),
COALESCE(CONVERT(nvarchar(max),m.definition),N''),
COALESCE(CONVERT(nvarchar(4000),TYPE_NAME(seq.user_type_id)),N''),
COALESCE(CONVERT(nvarchar(4000),seq.start_value),N''),
COALESCE(CONVERT(nvarchar(4000),seq.increment),N''),
COALESCE(CONVERT(nvarchar(4000),seq.minimum_value),N''),
COALESCE(CONVERT(nvarchar(4000),seq.maximum_value),N''),
COALESCE(CONVERT(nvarchar(10),seq.is_cycling),N'0'),
COALESCE(CONVERT(nvarchar(10),seq.is_cached),N'0'),
COALESCE(CONVERT(nvarchar(4000),seq.cache_size),N'')
FROM sys.objects o
JOIN sys.schemas s ON s.schema_id=o.schema_id
LEFT JOIN sys.sql_modules m ON m.object_id=o.object_id
LEFT JOIN sys.sequences seq ON seq.object_id=o.object_id
WHERE s.name=` + qStr(schema) + ` AND o.is_ms_shipped=0 AND o.type IN (N'V',N'SO',N'TR',N'P',N'PC',N'X',N'FN',N'IF',N'TF',N'FS',N'FT')
ORDER BY o.type,o.name`
	rows, _, _, err := p.query(ctx, q)
	if err != nil {
		return nil, err
	}
	out := make([]domain.SchemaObject, 0, len(rows))
	byName := map[string]int{}
	for _, row := range rows {
		if len(row) < 13 {
			continue
		}
		typ, ok := sqlServerSchemaObjectType(string(row[0]))
		if !ok {
			continue
		}
		name := string(row[1])
		obj := domain.SchemaObject{Schema: schema, Name: name, Type: typ, BindingKnown: typ != domain.SchemaObjectSequence}
		parentSchema, parentName := string(row[2]), string(row[3])
		if parentName != "" {
			if parentSchema == "" || strings.EqualFold(parentSchema, schema) {
				obj.RelatedTo = parentName
			} else {
				obj.RelatedTo = parentSchema + "." + parentName
			}
		}
		definition := strings.TrimSpace(string(row[4]))
		if typ == domain.SchemaObjectSequence {
			ddl, e := sqlServerSequenceDDL(schema, name, string(row[5]), string(row[6]), string(row[7]), string(row[8]), string(row[9]), parseBoolText(row[10]), parseBoolText(row[11]), string(row[12]))
			if e != nil {
				return nil, fmt.Errorf("build sequence %s.%s DDL: %w", schema, name, e)
			}
			obj.DDL = ddl
			obj.Definition = "STANDALONE"
		} else {
			obj.DDL = definition
			obj.Definition = definition
		}
		byName[strings.ToLower(name)] = len(out)
		out = append(out, obj)
	}

	depQ := `SELECT CONVERT(nvarchar(4000),o.name),COALESCE(CONVERT(nvarchar(4000),d.referenced_schema_name),N''),COALESCE(CONVERT(nvarchar(4000),d.referenced_entity_name),N'')
FROM sys.sql_expression_dependencies d
JOIN sys.objects o ON o.object_id=d.referencing_id
JOIN sys.schemas s ON s.schema_id=o.schema_id
WHERE s.name=` + qStr(schema) + ` ORDER BY o.name,d.referenced_schema_name,d.referenced_entity_name`
	depRows, _, _, depErr := p.query(ctx, depQ)
	if depErr == nil {
		for i := range out {
			if out[i].Type != domain.SchemaObjectSequence {
				out[i].DependenciesKnown = true
			}
		}
		for _, row := range depRows {
			if len(row) < 3 || strings.TrimSpace(string(row[2])) == "" {
				continue
			}
			i, ok := byName[strings.ToLower(string(row[0]))]
			if !ok {
				continue
			}
			refSchema, refName := strings.TrimSpace(string(row[1])), strings.TrimSpace(string(row[2]))
			dep := refName
			if refSchema != "" {
				dep = refSchema + "." + refName
			}
			out[i].Dependencies = append(out[i].Dependencies, dep)
		}
		for i := range out {
			sort.Strings(out[i].Dependencies)
		}
	}
	return out, nil
}

var _ connector.SchemaObjectConnector = (*Connector)(nil)
