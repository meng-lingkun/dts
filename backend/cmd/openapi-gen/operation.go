package main

import (
	"fmt"
	"go/ast"
	"go/constant"
	"net/http"
	"reflect"
	"strconv"
	"strings"

	"dts/backend/internal/auth"
)

var userAuth = []object{{"Bearer": []string{}}, {"APIToken": []string{}}, {"BrowserSession": []string{}}}
var errorRef = object{"$ref": "#/components/responses/Error"}

func parameter(name, in string, required bool, schema object) object {
	return object{"in": in, "name": name, "required": required, "schema": schema}
}
func response(code int, content object) object {
	r := object{"description": http.StatusText(code), "headers": object{"X-Request-ID": object{"schema": textSchema()}}}
	if content != nil {
		r["content"] = content
	}
	return r
}

func (s *source) operation(method, path, id string, handler *ast.BlockStmt) object {
	responses := object{}
	params := []object{parameter("X-Request-ID", "header", false, object{"type": "string", "pattern": "^[A-Za-z0-9._-]{1,96}$", "description": "Optional correlation identifier. The server generates an identifier if omitted or invalid."})}
	op := object{"operationId": id, "responses": responses}
	if strings.HasPrefix(path, "/api/v1/users/{id}/sessions") {
		op["operationId"] = id + "ForUser"
	}
	segments := strings.Split(strings.TrimPrefix(path, "/api/v1/"), "/")
	for _, v := range segments {
		if strings.HasPrefix(v, "{") {
			params = append(params, parameter(strings.Trim(v, "{}"), "path", true, textSchema()))
		}
	}
	if strings.HasPrefix(path, "/api/v1/") {
		op["tags"] = []string{segments[0]}
	} else {
		op["tags"] = []string{"monitoring"}
	}
	var roles []string
	for _, role := range []auth.Role{auth.RoleAdmin, auth.RoleDBA, auth.RoleOperator, auth.RoleViewer} {
		if auth.Allowed(role, method, path) {
			roles = append(roles, string(role))
		}
	}
	switch {
	case !strings.HasPrefix(path, "/api/v1/") || id == "login" || id == "browserLogout" || id == "startOIDC" || id == "finishOIDC" || id == "authProviders":
		op["security"] = []object{}
	default:
		op["x-dts-allowed-roles"] = roles
		responses["401"], responses["403"] = errorRef, errorRef
		if method == "POST" && segments[0] == "workers" {
			op["security"] = append([]object{{"WorkerToken": []string{}}}, userAuth...)
			if len(segments) >= 5 && (segments[2] == "chunks" || segments[2] == "engine-jobs") {
				params = append(params, parameter("X-DTS-Lease-Generation", "header", true, object{"type": "integer", "minimum": 1}))
				responses["428"] = errorRef
			}
		}
	}
	// These handlers delegate HTTP decoding and encoding to shared helpers.
	switch id {
	case "confirmCutoverFreeze", "confirmRollbackFreeze":
		handler = s.methods["confirmFreeze"].Body
		params = append(params, parameter("Idempotency-Key", "header", false, object{"type": "string", "description": "8–128 UTF-8 bytes"}))
		op["description"] = "Requires external write freeze, operator evidence and an idempotency key in the body or header. The header takes precedence."
	case "applyCanalEvents", "applyDebeziumEvents":
		handler = s.methods["applyCompatibilityCDC"].Body
		op["requestBody"] = object{"required": true, "description": "Native Canal JSON / Debezium envelope(s), up to 16 MiB.", "content": media("application/json", object{})}
	}
	variants := map[int][]object{}
	queries := map[string]bool{}
	ast.Inspect(handler, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		switch nameOf(call.Fun) {
		case "decode", "decodeOptional", "Decode":
			value := call.Args[len(call.Args)-1]
			addr, ok := value.(*ast.UnaryExpr)
			if !ok {
				panic("unrecognized request decoder: " + id)
			}
			schema := s.schema(s.info.TypeOf(addr.X))
			switch id {
			case "createPersonalToken":
				schema["required"] = []string{"name", "expires_at"}
				schema["properties"] = object{"name": object{"type": "string", "minLength": 1, "maxLength": 80}, "expires_at": object{"type": "string", "format": "date-time", "description": "Future expiry, at most 365 days from issuance."}}
			case "createDataSource":
				schema["required"] = []string{"name", "host", "username"}
			case "createMigration":
				schema["required"] = []string{"name", "source_datasource_id", "target_datasource_id"}
			case "createUser":
				schema["required"] = []string{"username", "password", "role"}
			case "login":
				schema["required"] = []string{"username", "password"}
			case "changeUserPassword":
				schema["required"] = []string{"password"}
			case "confirmCutoverFreeze", "confirmRollbackFreeze":
				schema["required"] = []string{"writes_frozen", "evidence"}
				schema["properties"] = object{"writes_frozen": object{"const": true}, "evidence": object{"type": "string", "description": "At least 10 UTF-8 bytes after trimming and at most 2000 bytes before trimming."}}
			}
			op["requestBody"] = object{"required": nameOf(call.Fun) != "decodeOptional" && id != "renewChunk", "content": media("application/json", schema)}
			if nameOf(call.Fun) != "Decode" {
				responses["413"] = errorRef
			}
		case "Get":
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				break
			}
			query, ok := sel.X.(*ast.CallExpr)
			if ok && nameOf(query.Fun) == "Query" {
				name := str(call.Args[0])
				if queries[name] {
					break
				}
				queries[name] = true
				qs := textSchema()
				if name == "format" {
					qs["enum"] = []string{"json", "html", "pdf"}
					qs["default"] = "html"
				}
				if name == "limit" {
					qs = object{"type": "integer"}
				}
				params = append(params, parameter(name, "query", name == "schema" || name == "table", qs))
			}
		case "writeJSON", "apiError", "WriteHeader":
			method := nameOf(call.Fun)
			arg := 0
			if method != "WriteHeader" {
				arg = 1
			}
			status := s.info.Types[call.Args[arg]].Value
			if status == nil {
				panic("dynamic response code in " + id)
			}
			code64, ok := constant.Int64Val(status)
			if !ok {
				panic("invalid status in " + id)
			}
			code, key := int(code64), strconv.Itoa(int(code64))
			switch method {
			case "writeJSON":
				schema := s.expression(call.Args[2])
				duplicate := false
				for _, v := range variants[code] {
					if reflect.DeepEqual(v, schema) {
						duplicate = true
						break
					}
				}
				if !duplicate {
					variants[code] = append(variants[code], schema)
				}
			case "apiError":
				responses[key] = errorRef
			case "WriteHeader":
				responses[key] = response(code, nil)
			}
		}
		return true
	})
	for code, forms := range variants {
		schema := forms[0]
		if len(forms) > 1 {
			schema = object{"anyOf": forms}
		}
		responses[strconv.Itoa(code)] = response(code, media("application/json", schema))
	}
	if strings.HasPrefix(path, "/api/v1/") {
		responses["default"] = errorRef
	}
	s.special(id, responses, op, &params)
	if len(params) > 0 {
		op["parameters"] = params
	}
	for k := range responses {
		if strings.HasPrefix(k, "2") || k == "101" || k == "302" || k == "303" {
			return op
		}
	}
	panic(fmt.Sprintf("%s %s: no documented success response", method, path))
}

// Explicit wire contracts for raw attachments, protocol upgrades, browser
// sessions and other responses not encoded directly via writeJSON.
func (s *source) special(id string, responses, op object, params *[]object) {
	report := func(name string) object { return media("application/json", s.model("validationreport", name)) }
	switch id {
	case "createPersonalToken", "listPersonalTokens", "revokePersonalToken":
		op["description"] = "Requires a signed-in user session. Each user manages their own personal API tokens. Issuance returns the random bearer secret once. OIDC-only mode disables this operation."
	case "startOIDC":
		responses["302"] = response(302, nil)
	case "finishOIDC":
		responses["303"] = response(303, nil)
	case "metrics":
		responses["200"] = response(200, media("text/plain", textSchema()))
		op["description"] = "When DTS_METRICS_TOKEN (or DTS_METRICS_TOKEN_FILE) is configured, supply that credential as a bearer token."
		op["security"] = []object{{}, {"MetricsBearer": []string{}}}
	case "websocket":
		responses["101"] = response(101, nil)
		op["description"] = "WebSocket upgrade (version 13). Optional dts.v1 subprotocol. Authenticate using the normal credentials or an auth.<base64url-token> subprotocol. Cookie clients require matching Origin."
	case "validationReportPublicKey":
		responses["200"] = response(200, report("PublicKeyDocument"))
	case "validationReportKeyTransition":
		responses["201"] = response(201, report("KeyTransitionCertificate"))
	case "validationReportKeyRevocation":
		responses["201"] = response(201, report("KeyRevocationCertificate"))
	case "validationReportManifest":
		responses["200"] = response(200, report("Manifest"))
	case "validationReport":
		content := report("Report")
		content["text/html"] = object{"schema": textSchema()}
		content["application/pdf"] = object{"schema": object{"type": "string", "format": "binary"}}
		responses["200"] = response(200, content)
	case "login", "browserLogout":
		browser := parameter("X-DTS-Browser", "header", false, object{"type": "string", "enum": []string{"1"}})
		*params = append(*params, browser, parameter("Origin", "header", false, textSchema()))
		responses["403"] = errorRef
		if id == "login" {
			props := object{"expires_at": object{"type": "string", "format": "date-time"}, "user": s.model("domain", "User"), "token": textSchema()}
			responses["200"] = response(200, media("application/json", object{"type": "object", "required": []string{"expires_at", "user"}, "properties": props}))
			op["description"] = "Browser mode requires X-DTS-Browser: 1 and matching Origin, sets the HttpOnly dts_session cookie, and omits token from JSON. CLI clients receive token in the response."
		} else {
			op["description"] = "Revokes the current signed session. Cookie clients require X-DTS-Browser: 1 and matching Origin. CLI clients can log out using their bearer session token."
		}
	case "confirmCutoverFreeze", "confirmRollbackFreeze", "readyCutover", "readyRollback", "cutover", "rollbackMigration":
		responses["409"] = errorRef
	}
}
