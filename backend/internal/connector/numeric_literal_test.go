package connector

import "testing"

func TestValidateNumericLiteral(t *testing.T) {
	for _, v := range []string{"0", "-1", "+12", "123.45", ".5", "5.", "1e10", "-1.25E-8"} {
		if err := ValidateNumericLiteral([]byte(v), false); err != nil {
			t.Fatalf("%q should be valid: %v", v, err)
		}
	}
	for _, v := range []string{"", "1;DROP TABLE x", "1 OR 1=1", "--1", "0x10", "1 2", "NaN"} {
		if err := ValidateNumericLiteral([]byte(v), false); err == nil {
			t.Fatalf("%q should be rejected", v)
		}
	}
	for _, v := range []string{"NaN", "Infinity", "-Infinity"} {
		if err := ValidateNumericLiteral([]byte(v), true); err != nil {
			t.Fatalf("special float %q should be allowed: %v", v, err)
		}
	}
}
