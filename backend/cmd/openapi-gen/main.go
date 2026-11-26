// Command openapi-gen extracts the HTTP contract from the Go 1.22 ServeMux
// registrations and the JSON values passed to each handler's decoder/writer.
// Run from backend with: go run ./cmd/openapi-gen
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"dts/backend/internal/version"
)

type object = map[string]any

type source struct {
	info    *types.Info
	pkg     *types.Package
	methods map[string]*ast.FuncDecl
	schemas object
}

func main() {
	check := flag.Bool("check", false, "verify the checked-in specification")
	flag.Parse()
	if err := run(*check); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// Use the Go tool's selected source files and export data so dependency types,
// build constraints and local toolchain settings agree with the server binary.
func load() (*source, error) {
	cmd := exec.Command("go", "list", "-deps", "-export", "-json", "./internal/api")
	cmd.Stderr = os.Stderr
	raw, err := cmd.Output()
	if err != nil {
		return nil, err
	}
	var files []*ast.File
	fs := token.NewFileSet()
	exports := map[string]string{}
	d := json.NewDecoder(bytes.NewReader(raw))
	const path = "dts/backend/internal/api"
	for {
		var p struct {
			ImportPath, Dir, Export string
			GoFiles                 []string
		}
		if err := d.Decode(&p); err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		exports[p.ImportPath] = p.Export
		if p.ImportPath != path {
			continue
		}
		for _, file := range p.GoFiles {
			f, err := parser.ParseFile(fs, filepath.Join(p.Dir, file), nil, 0)
			if err != nil {
				return nil, err
			}
			files = append(files, f)
		}
	}
	imp := importer.ForCompiler(fs, "gc", func(path string) (io.ReadCloser, error) {
		return os.Open(exports[path])
	})
	config := types.Config{Importer: imp}
	s := &source{info: &types.Info{Types: map[ast.Expr]types.TypeAndValue{}, Uses: map[*ast.Ident]types.Object{}}, methods: map[string]*ast.FuncDecl{}, schemas: object{}}
	s.pkg, err = config.Check(path, fs, files, s.info)
	if err != nil {
		return nil, err
	}
	for _, file := range files {
		for _, decl := range file.Decls {
			if f, ok := decl.(*ast.FuncDecl); ok {
				s.methods[f.Name.Name] = f
			}
		}
	}
	return s, nil
}

func run(check bool) error {
	s, err := load()
	if err != nil {
		return err
	}
	paths := map[string]object{}
	s.routes(func(method, path, name string, body *ast.BlockStmt) {
		if paths[path] == nil {
			paths[path] = object{}
		}
		paths[path][strings.ToLower(method)] = s.operation(method, path, name, body)
	})
	if len(paths) == 0 {
		return fmt.Errorf("no ServeMux registrations found")
	}
	doc := object{
		"openapi": "3.1.0",
		"info": object{"title": "DTS Control and Worker API", "version": version.Version,
			"description": "Generated from the server's HTTP routes and Go wire types. Authenticated requests use a configured role token or login session. Cookie-authenticated writes require a matching Origin and X-DTS-Browser: 1. Worker POST routes use the worker credential when configured and otherwise use normal user authorization."},
		"paths": paths, "security": userAuth,
		"components": object{
			"schemas":   s.schemas,
			"responses": object{"Error": object{"description": "Request failed", "headers": object{"X-Request-ID": object{"schema": textSchema()}}, "content": media("application/json", object{"type": "object", "required": []string{"error", "code", "request_id"}, "properties": object{"error": textSchema(), "code": textSchema(), "request_id": textSchema()}})}},
			"securitySchemes": object{
				"Bearer":         object{"type": "http", "scheme": "bearer", "description": "User session, configured machine token or a dts_pat_ personal token. Personal tokens require this header and are excluded from WebSocket, worker writes, session management and OIDC-only mode."},
				"MetricsBearer":  object{"type": "http", "scheme": "bearer"},
				"APIToken":       object{"type": "apiKey", "in": "header", "name": "X-DTS-API-Token"},
				"BrowserSession": object{"type": "apiKey", "in": "cookie", "name": "dts_session"},
				"WorkerToken":    object{"type": "apiKey", "in": "header", "name": "X-DTS-Worker-Token"},
			},
		},
	}
	raw, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	path := filepath.Join("..", "docs", "openapi.json")
	if !check {
		return os.WriteFile(path, raw, 0644)
	}
	existing, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if !bytes.Equal(bytes.ReplaceAll(existing, []byte("\r\n"), []byte("\n")), raw) {
		return fmt.Errorf("API specification drift: from backend run go run ./cmd/openapi-gen")
	}
	return nil
}

func (s *source) routes(fn func(method, path, name string, body *ast.BlockStmt)) {
	ast.Inspect(s.methods["Handler"].Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || nameOf(call.Fun) != "HandleFunc" {
			return true
		}
		pattern := str(call.Args[0])
		method, path, ok := strings.Cut(pattern, " ")
		if !ok {
			panic("unsupported mux pattern: " + pattern)
		}
		h := call.Args[1]
		if inline, ok := h.(*ast.FuncLit); ok {
			fn(method, path, strings.Trim(path, "/"), inline.Body)
		} else {
			name := nameOf(h)
			fn(method, path, name, s.methods[name].Body)
		}
		return false
	})
}
