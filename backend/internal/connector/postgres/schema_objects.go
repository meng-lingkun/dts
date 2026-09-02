package postgresconnector

import (
	"context"
	"fmt"
	"qmigration/backend/internal/connector"
	"qmigration/backend/internal/domain"
	"strconv"
	"strings"
)

func (c *Connector) ListSchemaObjects(ctx context.Context, schema string) ([]domain.SchemaObject, error) {
	p, err := c.get(ctx)
	if err != nil {
		return nil, err
	}
	if schema == "" {
		return nil, fmt.Errorf("schema is required")
	}
	out := []domain.SchemaObject{}
	viewDeps := map[string][]string{}
	deps, depErr := p.query(ctx, "SELECT view_name,table_schema,table_name FROM information_schema.view_table_usage WHERE view_schema="+pgString(schema)+" ORDER BY view_name,table_schema,table_name")
	if depErr == nil {
		for _, row := range deps.rows {
			if len(row) < 3 {
				continue
			}
			viewDeps[string(row[0])] = append(viewDeps[string(row[0])], string(row[1])+"."+string(row[2]))
		}
	}

	views, err := p.query(ctx, "SELECT table_name,COALESCE(view_definition,'') FROM information_schema.views WHERE table_schema="+pgString(schema)+" ORDER BY table_name")
	if err != nil {
		return nil, err
	}
	for _, row := range views.rows {
		if len(row) < 1 {
			continue
		}
		name := string(row[0])
		def := ""
		if len(row) > 1 {
			def = string(row[1])
		}
		ddl := ""
		if def != "" {
			ddl = "CREATE OR REPLACE VIEW " + pgIdent(schema) + "." + pgIdent(name) + " AS " + def
		}
		out = append(out, domain.SchemaObject{Schema: schema, Name: name, Type: domain.SchemaObjectView, DDL: ddl, Definition: def, Dependencies: append([]string(nil), viewDeps[name]...), DependenciesKnown: depErr == nil})
	}

	type sequenceBinding struct {
		related string
		kind    string
	}
	sequenceBindings := map[string]sequenceBinding{}
	bindings, bindingErr := p.query(ctx, `SELECT seq.relname,tbl.relname,att.attname,COALESCE(att.attidentity,'')
FROM pg_class seq
JOIN pg_namespace ns ON ns.oid=seq.relnamespace
LEFT JOIN pg_depend dep ON dep.objid=seq.oid AND dep.classid='pg_class'::regclass AND dep.refclassid='pg_class'::regclass AND dep.deptype IN ('a','i')
LEFT JOIN pg_class tbl ON tbl.oid=dep.refobjid
LEFT JOIN pg_attribute att ON att.attrelid=tbl.oid AND att.attnum=dep.refobjsubid
WHERE ns.nspname=`+pgString(schema)+` AND seq.relkind='S' ORDER BY seq.relname`)
	if bindingErr == nil {
		for _, row := range bindings.rows {
			if len(row) < 4 || len(row[1]) == 0 || len(row[2]) == 0 {
				continue
			}
			kind := "OWNED"
			if strings.TrimSpace(string(row[3])) != "" {
				kind = "IDENTITY"
			}
			sequenceBindings[string(row[0])] = sequenceBinding{related: string(row[1]) + "." + string(row[2]), kind: kind}
		}
	}

	seqs, err := p.query(ctx, "SELECT sequencename,COALESCE(start_value,1),COALESCE(increment_by,1),COALESCE(min_value,1),COALESCE(max_value,9223372036854775807),COALESCE(cache_size,1),cycle FROM pg_sequences WHERE schemaname="+pgString(schema)+" ORDER BY sequencename")
	if err != nil {
		return nil, err
	}
	for _, row := range seqs.rows {
		if len(row) < 7 {
			continue
		}
		name := string(row[0])
		cycle := strings.EqualFold(string(row[6]), "t") || strings.EqualFold(string(row[6]), "true")
		ddl := "CREATE SEQUENCE " + pgIdent(schema) + "." + pgIdent(name) + " START WITH " + string(row[1]) + " INCREMENT BY " + string(row[2]) + " MINVALUE " + string(row[3]) + " MAXVALUE " + string(row[4]) + " CACHE " + string(row[5])
		if cycle {
			ddl += " CYCLE"
		} else {
			ddl += " NO CYCLE"
		}
		binding := sequenceBindings[name]
		out = append(out, domain.SchemaObject{Schema: schema, Name: name, Type: domain.SchemaObjectSequence, DDL: ddl, RelatedTo: binding.related, Definition: binding.kind, BindingKnown: bindingErr == nil})
	}

	routines, err := p.query(ctx, `SELECT p.proname,p.prokind,pg_get_functiondef(p.oid) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname=`+pgString(schema)+` AND p.prokind IN ('f','p') ORDER BY p.proname`)
	if err != nil {
		return nil, err
	}
	for _, row := range routines.rows {
		if len(row) < 3 {
			continue
		}
		typ := domain.SchemaObjectFunction
		if string(row[1]) == "p" {
			typ = domain.SchemaObjectProcedure
		}
		out = append(out, domain.SchemaObject{Schema: schema, Name: string(row[0]), Type: typ, DDL: string(row[2]), Definition: string(row[2])})
	}

	triggers, err := p.query(ctx, `SELECT c.relname,t.tgname,pg_get_triggerdef(t.oid,true) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=`+pgString(schema)+` AND NOT t.tgisinternal ORDER BY c.relname,t.tgname`)
	if err != nil {
		return nil, err
	}
	for _, row := range triggers.rows {
		if len(row) < 3 {
			continue
		}
		ddl := string(row[2])
		if ddl != "" && !strings.HasSuffix(strings.TrimSpace(ddl), ";") {
			ddl += ";"
		}
		out = append(out, domain.SchemaObject{Schema: schema, Name: string(row[1]), Type: domain.SchemaObjectTrigger, RelatedTo: string(row[0]), DDL: ddl, Definition: ddl})
	}

	return out, nil
}

var _ connector.SchemaObjectConnector = (*Connector)(nil)

// GetSequenceState returns the transactional state required to continue a
// PostgreSQL sequence without generating duplicate values after cutover.
func (c *Connector) GetSequenceState(ctx context.Context, schema, name string) (string, bool, error) {
	p, err := c.get(ctx)
	if err != nil {
		return "", false, err
	}
	r, err := p.query(ctx, "SELECT last_value,is_called FROM "+pgIdent(schema)+"."+pgIdent(name))
	if err != nil {
		return "", false, err
	}
	if len(r.rows) == 0 || len(r.rows[0]) < 2 {
		return "", false, fmt.Errorf("sequence %s.%s returned no state", schema, name)
	}
	called := strings.EqualFold(string(r.rows[0][1]), "t") || strings.EqualFold(string(r.rows[0][1]), "true")
	return string(r.rows[0][0]), called, nil
}

func (c *Connector) SetSequenceState(ctx context.Context, schema, name, lastValue string, isCalled bool) error {
	value, err := strconv.ParseInt(strings.TrimSpace(lastValue), 10, 64)
	if err != nil {
		return fmt.Errorf("invalid PostgreSQL sequence value %q: %w", lastValue, err)
	}
	p, err := c.get(ctx)
	if err != nil {
		return err
	}
	regclass := pgIdent(schema) + "." + pgIdent(name)
	return p.exec(ctx, "SELECT setval("+pgString(regclass)+"::regclass,"+strconv.FormatInt(value, 10)+","+strconv.FormatBool(isCalled)+")")
}

func (c *Connector) BindSequence(ctx context.Context, schema, sequence, table, column string) error {
	p, err := c.get(ctx)
	if err != nil {
		return err
	}
	if strings.TrimSpace(schema) == "" || strings.TrimSpace(sequence) == "" || strings.TrimSpace(table) == "" || strings.TrimSpace(column) == "" {
		return fmt.Errorf("schema, sequence, table and column are required")
	}
	qualifiedSeq := pgIdent(schema) + "." + pgIdent(sequence)
	qualifiedTable := pgIdent(schema) + "." + pgIdent(table)
	if err := p.exec(ctx, "ALTER SEQUENCE "+qualifiedSeq+" OWNED BY "+qualifiedTable+"."+pgIdent(column)); err != nil {
		return err
	}
	regclass := pgIdent(schema) + "." + pgIdent(sequence)
	return p.exec(ctx, "ALTER TABLE "+qualifiedTable+" ALTER COLUMN "+pgIdent(column)+" SET DEFAULT nextval("+pgString(regclass)+"::regclass)")
}

var _ connector.SequenceBindingConnector = (*Connector)(nil)

var _ connector.SequenceStateConnector = (*Connector)(nil)
