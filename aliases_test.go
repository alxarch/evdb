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
	aliases.Set("FOO", "foo")
	if aliases.Alias("FOO") != "foo" {
		t.Error("Invalid substitution")
	}
	aliases.Set("FOO", "foo", "Foo", "foo")
	if aliases.Alias("Foo") != "foo" {
		t.Error("Invalid substitution")
	}
}
