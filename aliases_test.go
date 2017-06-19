package meter_test

import (
	"testing"

	m "github.com/alxarch/go-meter"
)

func Test_NewAliases(t *testing.T) {
	aliases := m.NewAliases()
	if aliases.Alias("foo") != "foo" {
		t.Error("Invalid empty aliases")
	}
	aliases.Set("foo", "FOO")
	if aliases.Alias("FOO") != "foo" {
		t.Error("Invalid substitution")
	}
	aliases.Set("foo", "FOO", "Foo")
	if aliases.Alias("Foo") != "foo" {
		t.Error("Invalid substitution")
	}
}
