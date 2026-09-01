package config

import "testing"

func TestStringFallsBackOnEmpty(t *testing.T) {
	t.Setenv("CONFIG_TEST_PLAIN", "")
	if got := String("CONFIG_TEST_PLAIN", "fallback"); got != "fallback" {
		t.Fatalf("String() with empty value = %q, want fallback", got)
	}
}

func TestStringAllowEmptyDistinguishesEmptyFromUnset(t *testing.T) {
	t.Setenv("CONFIG_TEST_EMPTY", "")
	if got := StringAllowEmpty("CONFIG_TEST_EMPTY", "fallback"); got != "" {
		t.Fatalf("StringAllowEmpty() with empty value = %q, want empty string", got)
	}
	if got := StringAllowEmpty("CONFIG_TEST_UNSET", "fallback"); got != "fallback" {
		t.Fatalf("StringAllowEmpty() with unset key = %q, want fallback", got)
	}
}
