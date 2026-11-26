package api

import (
	"encoding/json"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
)

// Exercise the public HTTP handler and compare wire responses with the
// checked-in contract. This catches decoder/encoder changes a route count
// alone would miss, including accidental disclosure in datasource responses.
func TestOpenAPIWireContract(t *testing.T) {
	doc, err := os.ReadFile("../../../docs/openapi.json")
	if err != nil {
		t.Fatal(err)
	}
	var spec struct {
		Paths map[string]map[string]struct {
			Responses map[string]struct {
				Ref     string `json:"$ref"`
				Content map[string]struct {
					Schema struct {
						Ref string `json:"$ref"`
					}
				}
			}
		}
		Components struct {
			Schemas map[string]struct{ Properties map[string]json.RawMessage }
		}
	}
	if err := json.Unmarshal(doc, &spec); err != nil {
		t.Fatal(err)
	}
	h := newAuthTestServer(t)
	const path = "/api/v1/datasources"
	for _, tc := range []struct {
		body any
		code int
	}{
		{map[string]string{"name": "incomplete"}, 400},
		{map[string]string{"name": "contract", "host": "localhost", "username": "test", "password": "example"}, 201},
	} {
		w := requestJSON(t, h, http.MethodPost, path, tc.body, "")
		response, ok := spec.Paths[path]["post"].Responses[strconv.Itoa(w.Code)]
		if w.Code != tc.code || !ok {
			t.Fatalf("expected documented HTTP %d, got %d", tc.code, w.Code)
		}
		var result map[string]json.RawMessage
		if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		if tc.code == 400 {
			if response.Ref != "#/components/responses/Error" || result["error"] == nil {
				t.Fatal("error response contract mismatch")
			}
			continue
		}
		name := strings.TrimPrefix(response.Content["application/json"].Schema.Ref, "#/components/schemas/")
		properties := spec.Components.Schemas[name].Properties
		if properties["id"] == nil || properties["password"] != nil || result["password"] != nil {
			t.Fatal("invalid datasource response contract")
		}
		for field := range result {
			if properties[field] == nil {
				t.Errorf("response field %q missing in schema %s", field, name)
			}
		}
	}
}
