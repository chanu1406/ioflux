package strace

import (
	"reflect"
	"testing"
)

func TestSplitCall(t *testing.T) {
	cases := []struct {
		in              string
		name, args, ret string
		ok              bool
	}{
		{`openat(AT_FDCWD, "/data/x", O_RDONLY) = 3 <0.000045>`, "openat", `AT_FDCWD, "/data/x", O_RDONLY`, "3 <0.000045>", true},
		{`close(3) = 0`, "close", "3", "0", true},
		{`read(3, "ab"..., 4096) = 4096 <0.0002>`, "read", `3, "ab"..., 4096`, "4096 <0.0002>", true},
		// `=` and `)` inside a struct/quoted arg must not split the call.
		{`newfstatat(AT_FDCWD, "f", {st_mode=S_IFREG|0644, st_size=10}, 0) = 0`, "newfstatat", `AT_FDCWD, "f", {st_mode=S_IFREG|0644, st_size=10}, 0`, "0", true},
		// A completed result section is required (the contract is name(args) = ret).
		{`brk(NULL)`, "", "", "", false},
		{`stat("/x", {st_mode=S_IFREG})`, "", "", "", false},
		{`not a call`, "", "", "", false},
	}
	for _, c := range cases {
		name, args, ret, ok := splitCall(c.in)
		if ok != c.ok || name != c.name || args != c.args || ret != c.ret {
			t.Errorf("splitCall(%q) = (%q,%q,%q,%v), want (%q,%q,%q,%v)",
				c.in, name, args, ret, ok, c.name, c.args, c.ret, c.ok)
		}
	}
}

func TestStructField(t *testing.T) {
	cases := []struct {
		s, field, want string
		ok             bool
	}{
		{`{flags=O_WRONLY|O_APPEND, mode=0, resolve=0}`, "flags", "O_WRONLY|O_APPEND", true},
		{`{flags=O_RDONLY|O_DIRECTORY, mode=0}`, "flags", "O_RDONLY|O_DIRECTORY", true},
		{`{flags=O_RDONLY}`, "flags", "O_RDONLY", true},
		{`{mode=0, resolve=0}`, "flags", "", false},
	}
	for _, c := range cases {
		got, ok := structField(c.s, c.field)
		if got != c.want || ok != c.ok {
			t.Errorf("structField(%q,%q) = (%q,%v), want (%q,%v)", c.s, c.field, got, ok, c.want, c.ok)
		}
	}
}

func TestSplitArgs(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{`AT_FDCWD, "/data/x", O_RDONLY`, []string{"AT_FDCWD", `"/data/x"`, "O_RDONLY"}},
		// comma inside a quoted string must not split.
		{`3, "a,b,c"..., 8`, []string{"3", `"a,b,c"...`, "8"}},
		// escaped quote inside a string.
		{`3, "he said \"hi\"", 5`, []string{"3", `"he said \"hi\""`, "5"}},
		// nested struct with commas.
		{`AT_FDCWD, "f", {st_mode=S_IFREG, st_size=10}, 0`, []string{"AT_FDCWD", `"f"`, "{st_mode=S_IFREG, st_size=10}", "0"}},
		{``, nil},
	}
	for _, c := range cases {
		got := splitArgs(c.in)
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("splitArgs(%q) = %#v, want %#v", c.in, got, c.want)
		}
	}
}

func TestParseQuoted(t *testing.T) {
	cases := []struct {
		in   string
		want string
		ok   bool
	}{
		{`"/data/x"`, "/data/x", true},
		{`"ab"...`, "ab", true},
		{`"a\tb"`, "a\tb", true},
		{`"quote\"here"`, `quote"here`, true},
		{`"oct\101"`, "octA", true}, // \101 = 'A'
		{`AT_FDCWD`, "", false},
	}
	for _, c := range cases {
		got, ok := parseQuoted(c.in)
		if got != c.want || ok != c.ok {
			t.Errorf("parseQuoted(%q) = (%q,%v), want (%q,%v)", c.in, got, ok, c.want, c.ok)
		}
	}
}

func TestParseLeadingInt(t *testing.T) {
	cases := []struct {
		in string
		v  int64
		ok bool
	}{
		{"3", 3, true},
		{"3</data/x>", 3, true}, // -y fd decoration
		{"-1 ENOENT (No such file)", -1, true},
		{"4096 <0.0002>", 4096, true},
		{"AT_FDCWD", 0, false},
		{"", 0, false},
	}
	for _, c := range cases {
		v, ok := parseLeadingInt(c.in)
		if v != c.v || ok != c.ok {
			t.Errorf("parseLeadingInt(%q) = (%d,%v), want (%d,%v)", c.in, v, ok, c.v, c.ok)
		}
	}
}
