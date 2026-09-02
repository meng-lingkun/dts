package validationreport

import "encoding/json"

func jsonUnmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }
func marshalIndentedNewline(v any) ([]byte, error) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}
