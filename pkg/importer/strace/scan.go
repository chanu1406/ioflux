package strace

import (
	"strconv"
	"strings"
)

// splitCall splits a completed syscall line like
// `openat(AT_FDCWD, "x", O_RDONLY) = 3 <0.1>` into the syscall name, the raw
// argument list (the text between the outermost parens), and the result text
// (everything after `) =`). ok is false unless the line is a well-formed
// `name(args) = ret` call with a non-empty result section; callers handle
// unfinished/resumed fragments before reaching here.
//
// It relies on findMatchingParen to locate the argument-closing paren, so commas,
// parens, and `=` inside quoted strings or nested {}/[] do not confuse it.
func splitCall(s string) (name, args, ret string, ok bool) {
	open := strings.IndexByte(s, '(')
	if open <= 0 {
		return "", "", "", false
	}
	close := findMatchingParen(s, open)
	if close < 0 {
		return "", "", "", false
	}
	name = strings.TrimSpace(s[:open])
	args = s[open+1 : close]
	tail := strings.TrimSpace(s[close+1:])
	if !strings.HasPrefix(tail, "=") {
		return "", "", "", false // no result section: not a completed call
	}
	ret = strings.TrimSpace(tail[1:])
	return name, args, ret, name != "" && ret != ""
}

// findMatchingParen returns the index of the ')' that matches the '(' at openIdx,
// skipping quoted strings (with backslash escapes) and counting nested
// (), [], {} together. Returns -1 if the brackets are unbalanced.
func findMatchingParen(s string, openIdx int) int {
	depth := 0
	inStr := false
	for i := openIdx; i < len(s); i++ {
		c := s[i]
		if inStr {
			switch c {
			case '\\':
				i++ // skip the escaped byte
			case '"':
				inStr = false
			}
			continue
		}
		switch c {
		case '"':
			inStr = true
		case '(', '[', '{':
			depth++
		case ')', ']', '}':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

// splitArgs splits a syscall argument list into its top-level arguments,
// respecting quoted strings and nested brackets. Each returned argument is
// trimmed of surrounding whitespace. An empty input yields no arguments.
func splitArgs(args string) []string {
	args = strings.TrimSpace(args)
	if args == "" {
		return nil
	}
	var out []string
	depth := 0
	inStr := false
	start := 0
	for i := 0; i < len(args); i++ {
		c := args[i]
		if inStr {
			switch c {
			case '\\':
				i++
			case '"':
				inStr = false
			}
			continue
		}
		switch c {
		case '"':
			inStr = true
		case '(', '[', '{':
			depth++
		case ')', ']', '}':
			depth--
		case ',':
			if depth == 0 {
				out = append(out, strings.TrimSpace(args[start:i]))
				start = i + 1
			}
		}
	}
	out = append(out, strings.TrimSpace(args[start:]))
	return out
}

// parseQuoted extracts a C-style quoted string argument, returning its decoded
// contents. A trailing `...` (strace's truncated-buffer marker) is ignored. ok
// is false if arg does not begin with a double quote.
func parseQuoted(arg string) (string, bool) {
	if len(arg) < 2 || arg[0] != '"' {
		return "", false
	}
	var b strings.Builder
	i := 1
	for i < len(arg) {
		c := arg[i]
		if c == '"' {
			return b.String(), true
		}
		if c == '\\' && i+1 < len(arg) {
			n, consumed := decodeEscape(arg[i+1:])
			b.WriteString(n)
			i += 1 + consumed
			continue
		}
		b.WriteByte(c)
		i++
	}
	// Unterminated quote: return what we have.
	return b.String(), true
}

// decodeEscape decodes a single C escape sequence following a backslash and
// returns the decoded text plus the number of input bytes consumed (excluding
// the backslash).
func decodeEscape(s string) (string, int) {
	switch s[0] {
	case 'n':
		return "\n", 1
	case 't':
		return "\t", 1
	case 'r':
		return "\r", 1
	case '0':
		// Could be \0 or an octal escape \ooo.
		return decodeOctal(s)
	case '\\':
		return "\\", 1
	case '"':
		return "\"", 1
	case 'x':
		if len(s) >= 3 {
			if v, err := strconv.ParseUint(s[1:3], 16, 8); err == nil {
				return string([]byte{byte(v)}), 3
			}
		}
		return "x", 1
	default:
		if s[0] >= '1' && s[0] <= '7' {
			return decodeOctal(s)
		}
		return string(s[0]), 1
	}
}

// decodeOctal decodes up to three octal digits into a byte.
func decodeOctal(s string) (string, int) {
	n := 0
	for n < 3 && n < len(s) && s[n] >= '0' && s[n] <= '7' {
		n++
	}
	if n == 0 {
		return "", 1
	}
	v, err := strconv.ParseUint(s[:n], 8, 16)
	if err != nil {
		return "", n
	}
	return string([]byte{byte(v)}), n
}

// splitFdArg parses an fd argument, returning the descriptor number and any
// strace path decoration (`3</data/file>` from -y/-yy). The decoration is
// returned only when it names a filesystem path (begins with '/'); socket/pipe
// decorations like `<socket:[12345]>` are reported as an empty path. ok is
// false if no fd integer is present.
func splitFdArg(s string) (fd int64, path string, ok bool) {
	fd, ok = parseLeadingInt(s)
	if !ok {
		return 0, "", false
	}
	if i := strings.IndexByte(s, '<'); i >= 0 {
		if j := strings.LastIndexByte(s, '>'); j > i {
			if d := s[i+1 : j]; strings.HasPrefix(d, "/") {
				path = d
			}
		}
	}
	return fd, path, true
}

// parseLeadingInt parses an optional sign and the leading run of decimal digits
// from s (e.g. "3</path>" -> 3, "-1 ENOENT (...)" -> -1). It also strips an
// strace fd path decoration (`3</path>`) since the integer comes first. ok is
// false if no integer is present.
func parseLeadingInt(s string) (int64, bool) {
	s = strings.TrimSpace(s)
	i := 0
	if i < len(s) && (s[i] == '-' || s[i] == '+') {
		i++
	}
	d := i
	for d < len(s) && s[d] >= '0' && s[d] <= '9' {
		d++
	}
	if d == i {
		return 0, false
	}
	v, err := strconv.ParseInt(s[:d], 10, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}
