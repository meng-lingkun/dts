package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
)

const maxJSONRequestBytes int64 = 64 << 20

var errJSONRequestTooLarge = errors.New("JSON request body exceeds 64 MiB")

func decode(r *http.Request, value any) error {
	if r.ContentLength > maxJSONRequestBytes {
		return errJSONRequestTooLarge
	}
	limited := &io.LimitedReader{R: r.Body, N: maxJSONRequestBytes + 1}
	decoder := json.NewDecoder(limited)
	if err := decoder.Decode(value); err != nil {
		if limited.N == 0 {
			return errJSONRequestTooLarge
		}
		return err
	}
	var extra json.RawMessage
	err := decoder.Decode(&extra)
	if limited.N == 0 {
		return errJSONRequestTooLarge
	}
	if err != io.EOF {
		if err == nil {
			return errors.New("request body must contain exactly one JSON value")
		}
		return fmt.Errorf("invalid trailing JSON request data: %w", err)
	}
	return nil
}

// Lifecycle requests historically allow an empty body. Preserve that contract,
// while rejecting partial JSON instead of interpreting it as an empty success.
func decodeOptional(r *http.Request, value any) error {
	err := decode(r, value)
	if err == io.EOF {
		return nil
	}
	return err
}
