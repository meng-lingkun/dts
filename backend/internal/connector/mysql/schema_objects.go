package mysqlconnector

import (
	"context"
	"fmt"
	"qmigration/backend/internal/connector"
	"qmigration/backend/internal/domain"
	"strings"
)

func firstCreateDDL(row [][]byte) string {
	for _, v := range row {
		s := strings.TrimSpace(string(v))
		if strings.HasPrefix(strings.ToUpper(s), "CREATE ") {
			return s
		}
	}
	return ""
}

func (c *Connector) showCreate(ctx context.Context, kind, schema, name string) string {
	p, err := c.get(ctx)
	if err != nil {
		return ""
	}
	q := "SHOW CREATE " + kind + " " + quoteIdent(schema) + "." + quoteIdent(name)
	r, err := p.query(ctx, q)
	if err != nil || len(r.rows) == 0 {
		return ""
	}
	return firstCreateDDL(r.rows[0])
}

// ListSchemaObjects discovers MySQL-compatible views, triggers and routines.
// SHOW CREATE is used when possible so the assessment UI can display the
// original object definition without attempting lossy cross-database rewriting.
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
	deps, depErr := p.query(ctx, "SELECT VIEW_NAME,TABLE_SCHEMA,TABLE_NAME FROM information_schema.VIEW_TABLE_USAGE WHERE VIEW_SCHEMA="+quoteSQLString(schema)+" ORDER BY VIEW_NAME,TABLE_SCHEMA,TABLE_NAME")
	if depErr == nil {
		for _, row := range deps.rows {
			if len(row) < 3 {
				continue
			}
			viewDeps[string(row[0])] = append(viewDeps[string(row[0])], string(row[1])+"."+string(row[2]))
		}
	}

	views, err := p.query(ctx, "SELECT TABLE_NAME,COALESCE(VIEW_DEFINITION,'') FROM information_schema.VIEWS WHERE TABLE_SCHEMA="+quoteSQLString(schema)+" ORDER BY TABLE_NAME")
	if err != nil {
		return nil, err
	}
	for _, row := range views.rows {
		if len(row) == 0 {
			continue
		}
		name := string(row[0])
		def := ""
		if len(row) > 1 {
			def = strings.TrimSpace(string(row[1]))
		}
		// Build a portable same-family view DDL without copying a source DEFINER.
		// SHOW CREATE VIEW commonly embeds account-specific DEFINER/SQL SECURITY
		// clauses that can fail or accidentally elevate privileges on the target.
		ddl := ""
		if def != "" {
			ddl = "CREATE OR REPLACE VIEW " + quoteIdent(schema) + "." + quoteIdent(name) + " AS " + def
		}
		out = append(out, domain.SchemaObject{Schema: schema, Name: name, Type: domain.SchemaObjectView, DDL: ddl, Definition: def, Dependencies: append([]string(nil), viewDeps[name]...), DependenciesKnown: depErr == nil})
	}

	triggers, err := p.query(ctx, "SELECT TRIGGER_NAME,EVENT_OBJECT_TABLE,ACTION_STATEMENT FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA="+quoteSQLString(schema)+" ORDER BY TRIGGER_NAME")
	if err != nil {
		return nil, err
	}
	for _, row := range triggers.rows {
		if len(row) < 2 {
			continue
		}
		def := ""
		if len(row) > 2 {
			def = string(row[2])
		}
		name := string(row[0])
		out = append(out, domain.SchemaObject{Schema: schema, Name: name, Type: domain.SchemaObjectTrigger, RelatedTo: string(row[1]), DDL: c.showCreate(ctx, "TRIGGER", schema, name), Definition: def})
	}

	routines, err := p.query(ctx, "SELECT ROUTINE_NAME,ROUTINE_TYPE,COALESCE(ROUTINE_DEFINITION,'') FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA="+quoteSQLString(schema)+" ORDER BY ROUTINE_TYPE,ROUTINE_NAME")
	if err != nil {
		return nil, err
	}
	for _, row := range routines.rows {
		if len(row) < 2 {
			continue
		}
		typ := domain.SchemaObjectProcedure
		showKind := "PROCEDURE"
		if strings.EqualFold(string(row[1]), "FUNCTION") {
			typ = domain.SchemaObjectFunction
			showKind = "FUNCTION"
		}
		def := ""
		if len(row) > 2 {
			def = string(row[2])
		}
		name := string(row[0])
		out = append(out, domain.SchemaObject{Schema: schema, Name: name, Type: typ, DDL: c.showCreate(ctx, showKind, schema, name), Definition: def})
	}
	return out, nil
}

var _ connector.SchemaObjectConnector = (*Connector)(nil)
