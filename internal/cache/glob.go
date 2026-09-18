package cache

import (
	"math"
	"strconv"
)

// GlobMatch implements Redis-style glob matching: '*' any run, '?' one byte,
// '[abc]' / '[a-z]' / '[^abc]' classes, '\\' escapes the next byte.
//
// This is not path.Match: that treats '/' as a separator that '*' must not
// cross, which would silently break patterns over namespaced keys like
// "user:1/profile". The loop is iterative with a backtrack point so a pattern
// such as "*a*a*a*b" stays linear instead of exponential.
func GlobMatch(pattern, s string) bool {
	if pattern == "" {
		return s == ""
	}
	if pattern == "*" {
		return true
	}
	var (
		p, i          int
		star          = -1
		resume        int
		matchedAtStar bool
	)
	for i < len(s) {
		if p < len(pattern) {
			switch pattern[p] {
			case '*':
				star, resume, matchedAtStar = p, i, true
				p++
				continue
			case '?':
				p++
				i++
				continue
			case '[':
				if end, ok := matchClass(pattern, p, s[i]); ok {
					p = end
					i++
					continue
				}
			case '\\':
				if p+1 < len(pattern) {
					if pattern[p+1] == s[i] {
						p += 2
						i++
						continue
					}
				} else if pattern[p] == s[i] {
					p++
					i++
					continue
				}
			default:
				if pattern[p] == s[i] {
					p++
					i++
					continue
				}
			}
		}
		if !matchedAtStar {
			return false
		}
		// Backtrack: let the last '*' swallow one more byte.
		resume++
		p, i = star+1, resume
	}
	for p < len(pattern) && pattern[p] == '*' {
		p++
	}
	return p == len(pattern)
}

// matchClass evaluates a '[...]' class starting at pattern[p] ('['), returning
// the index just past the class and whether c matched.
func matchClass(pattern string, p int, c byte) (int, bool) {
	p++ // past '['
	negate := false
	if p < len(pattern) && (pattern[p] == '^' || pattern[p] == '!') {
		negate = true
		p++
	}
	matched := false
	for p < len(pattern) && pattern[p] != ']' {
		switch {
		case pattern[p] == '\\' && p+1 < len(pattern):
			p++
			if pattern[p] == c {
				matched = true
			}
		case p+2 < len(pattern) && pattern[p+1] == '-' && pattern[p+2] != ']':
			lo, hi := pattern[p], pattern[p+2]
			if lo > hi {
				lo, hi = hi, lo
			}
			if c >= lo && c <= hi {
				matched = true
			}
			p += 2
		default:
			if pattern[p] == c {
				matched = true
			}
		}
		p++
	}
	if p < len(pattern) {
		p++ // past ']'
	}
	return p, matched != negate
}

func isInfOrNaN(f float64) bool { return math.IsInf(f, 0) || math.IsNaN(f) }

// formatFloat matches Redis INCRBYFLOAT output: plain decimal, no trailing
// zeros, never scientific notation.
//
// Precision -1 asks for the shortest string that round-trips. Formatting a
// fixed 17 decimals instead would surface binary representation noise —
// 10.5 + 0.1 would print as 10.59999999999999964 where Redis (long double)
// prints 10.6 — which is a difference clients would see.
func formatFloat(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}
