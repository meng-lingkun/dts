package main

import (
	"fmt"
	"go/ast"
	"go/types"
	"reflect"
	"strconv"
	"strings"
)

func textSchema() object                      { return object{"type": "string"} }
func media(kind string, schema object) object { return object{kind: object{"schema": schema}} }
func ref(name string) object                  { return object{"$ref": "#/components/schemas/" + name} }

func (s *source) schema(t types.Type) object {
	switch v := types.Unalias(t).(type) {
	case *types.Pointer:
		return object{"anyOf": []object{s.schema(v.Elem()), {"type": "null"}}}
	case *types.Named:
		if v.Obj().Pkg() == nil {
			return s.schema(v.Underlying())
		}
		switch v.Obj().Pkg().Path() + "." + v.Obj().Name() {
		case "time.Time":
			return object{"type": "string", "format": "date-time"}
		case "encoding/json.RawMessage":
			return object{}
		}
		if _, ok := v.Underlying().(*types.Struct); !ok {
			return s.schema(v.Underlying())
		}
		name := v.Obj().Pkg().Name() + "." + v.Obj().Name()
		if _, ok := s.schemas[name]; !ok {
			s.schemas[name] = object{}
			s.schemas[name] = s.schema(v.Underlying())
		}
		return ref(name)
	case *types.Interface:
		return object{}
	case *types.Basic:
		switch {
		case v.Info()&types.IsString != 0:
			return textSchema()
		case v.Info()&types.IsBoolean != 0:
			return object{"type": "boolean"}
		case v.Info()&types.IsInteger != 0:
			return object{"type": "integer"}
		case v.Info()&types.IsFloat != 0:
			return object{"type": "number"}
		}
	case *types.Slice:
		if b, ok := v.Elem().Underlying().(*types.Basic); ok && b.Kind() == types.Byte {
			return object{"type": []string{"string", "null"}, "contentEncoding": "base64"}
		}
		return object{"type": []string{"array", "null"}, "items": s.schema(v.Elem())}
	case *types.Array:
		return object{"type": "array", "items": s.schema(v.Elem()), "minItems": v.Len(), "maxItems": v.Len()}
	case *types.Map:
		return object{"type": []string{"object", "null"}, "additionalProperties": s.schema(v.Elem())}
	case *types.Struct:
		fields := object{}
		s.fields(v, fields)
		return object{"type": "object", "properties": fields}
	}
	panic(fmt.Sprintf("unsupported API type %v", t))
}

func (s *source) fields(v *types.Struct, props object) {
	for i := 0; i < v.NumFields(); i++ {
		f := v.Field(i)
		if !f.Exported() {
			continue
		}
		name, opts, _ := strings.Cut(reflect.StructTag(v.Tag(i)).Get("json"), ",")
		if name == "-" {
			continue
		}
		if f.Embedded() && name == "" {
			typ := f.Type()
			if p, ok := typ.(*types.Pointer); ok {
				typ = p.Elem()
			}
			if nested, ok := typ.Underlying().(*types.Struct); ok {
				s.fields(nested, props)
				continue
			}
		}
		if name == "" {
			name = f.Name()
		}
		if _, ok := props[name]; ok {
			panic("ambiguous JSON field " + name)
		}
		if strings.Contains(","+opts+",", ",string,") {
			props[name] = textSchema()
		} else {
			props[name] = s.schema(f.Type())
		}
	}
}

// Refine literal map[string]any responses using the static type of each value.
func (s *source) expression(expr ast.Expr) object {
	if v, ok := expr.(*ast.CompositeLit); ok {
		if _, ok := s.info.TypeOf(expr).Underlying().(*types.Map); ok && len(v.Elts) > 0 {
			fields := object{}
			for _, elt := range v.Elts {
				kv, ok := elt.(*ast.KeyValueExpr)
				if !ok || str(kv.Key) == "" {
					return s.schema(s.info.TypeOf(expr))
				}
				fields[str(kv.Key)] = s.schema(s.info.TypeOf(kv.Value))
			}
			return object{"type": "object", "properties": fields}
		}
	}
	return s.schema(s.info.TypeOf(expr))
}

func str(expr ast.Expr) string {
	if lit, ok := expr.(*ast.BasicLit); ok {
		result, _ := strconv.Unquote(lit.Value)
		return result
	}
	return ""
}

func nameOf(expr ast.Expr) string {
	switch v := expr.(type) {
	case *ast.SelectorExpr:
		return v.Sel.Name
	case *ast.Ident:
		return v.Name
	default:
		return ""
	}
}

func (s *source) model(pkg, name string) object {
	for _, p := range s.pkg.Imports() {
		if p.Name() == pkg {
			return s.schema(p.Scope().Lookup(name).Type())
		}
	}
	panic("unknown package " + pkg)
}
