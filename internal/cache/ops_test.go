package cache

import (
	"errors"
	"testing"
	"time"
)

func newTestCache() *Cache {
	return NewWithLimits(Limits{MaxItems: 1000, MaxBytes: 1 << 20, MaxValue: 4096, MaxKey: 256})
}

func TestSetWithOptionsNXXX(t *testing.T) {
	c := newTestCache()
	res, err := c.SetWithOptions("k", []byte("v1"), SetOptions{NX: true})
	if err != nil || !res.Stored {
		t.Fatalf("NX on absent key should store: %v %+v", err, res)
	}
	res, _ = c.SetWithOptions("k", []byte("v2"), SetOptions{NX: true})
	if res.Stored {
		t.Fatal("NX on existing key must not store")
	}
	if v, _ := c.Get("k"); string(v) != "v1" {
		t.Fatalf("value changed to %q", v)
	}
	res, _ = c.SetWithOptions("absent", []byte("v"), SetOptions{XX: true})
	if res.Stored {
		t.Fatal("XX on absent key must not store")
	}
	res, _ = c.SetWithOptions("k", []byte("v3"), SetOptions{XX: true, Get: true})
	if !res.Stored || !res.PrevFound || string(res.Prev) != "v1" {
		t.Fatalf("XX+GET: %+v", res)
	}
}

// KEEPTTL must not resurrect an expiry the plain form would have cleared.
func TestSetKeepTTL(t *testing.T) {
	c := newTestCache()
	if _, err := c.SetWithOptions("k", []byte("v"), SetOptions{TTL: time.Hour}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SetWithOptions("k", []byte("v2"), SetOptions{KeepTTL: true}); err != nil {
		t.Fatal(err)
	}
	if _, _, hasTTL := c.TTL("k"); !hasTTL {
		t.Fatal("KEEPTTL dropped the expiry")
	}
	if _, err := c.SetWithOptions("k", []byte("v3"), SetOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, _, hasTTL := c.TTL("k"); hasTTL {
		t.Fatal("plain SET must clear the expiry")
	}
}

func TestIncrBy(t *testing.T) {
	c := newTestCache()
	if n, err := c.IncrBy("n", 1); err != nil || n != 1 {
		t.Fatalf("first INCR: %d %v", n, err)
	}
	if n, _ := c.IncrBy("n", 41); n != 42 {
		t.Fatalf("INCRBY = %d, want 42", n)
	}
	if n, _ := c.IncrBy("n", -50); n != -8 {
		t.Fatalf("DECRBY = %d, want -8", n)
	}
	_ = c.Set("s", []byte("abc"), 0)
	if _, err := c.IncrBy("s", 1); !errors.Is(err, ErrNotInteger) {
		t.Fatalf("INCR on non-numeric: %v", err)
	}
	_ = c.Set("big", []byte("9223372036854775807"), 0)
	if _, err := c.IncrBy("big", 1); !errors.Is(err, ErrOverflow) {
		t.Fatalf("overflow not caught: %v", err)
	}
}

// INCR must not reset a TTL: a rate-limit counter would never expire otherwise.
func TestIncrPreservesTTL(t *testing.T) {
	c := newTestCache()
	_ = c.Set("n", []byte("1"), time.Hour)
	if _, err := c.IncrBy("n", 1); err != nil {
		t.Fatal(err)
	}
	if _, _, hasTTL := c.TTL("n"); !hasTTL {
		t.Fatal("INCR cleared the expiry")
	}
}

func TestIncrByFloat(t *testing.T) {
	c := newTestCache()
	got, err := c.IncrByFloat("f", 10.5)
	if err != nil || got != "10.5" {
		t.Fatalf("IncrByFloat = %q %v", got, err)
	}
	if got, _ = c.IncrByFloat("f", 0.1); got != "10.6" {
		t.Fatalf("IncrByFloat = %q, want 10.6", got)
	}
	if got, _ = c.IncrByFloat("f", -10.6); got != "0" {
		t.Fatalf("IncrByFloat = %q, want 0 (no trailing zeros)", got)
	}
}

func TestAppendAndStrLen(t *testing.T) {
	c := newTestCache()
	if n, _ := c.Append("k", []byte("foo")); n != 3 {
		t.Fatalf("append to absent key = %d", n)
	}
	if n, _ := c.Append("k", []byte("bar")); n != 6 {
		t.Fatalf("append = %d", n)
	}
	if v, _ := c.Get("k"); string(v) != "foobar" {
		t.Fatalf("value = %q", v)
	}
	if c.StrLen("k") != 6 || c.StrLen("absent") != 0 {
		t.Fatal("StrLen wrong")
	}
	// An append past the per-value cap must be rejected, not silently truncated.
	big := make([]byte, 4096)
	if _, err := c.Append("k", big); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("oversized append: %v", err)
	}
}

func TestTTLAndExpire(t *testing.T) {
	c := newTestCache()
	if _, exists, _ := c.TTL("absent"); exists {
		t.Fatal("absent key reported as existing")
	}
	_ = c.Set("k", []byte("v"), 0)
	if _, exists, hasTTL := c.TTL("k"); !exists || hasTTL {
		t.Fatal("key without TTL misreported")
	}
	if !c.Expire("k", time.Now().Add(time.Hour), ExpireOptions{}) {
		t.Fatal("Expire returned false")
	}
	if c.Expire("k", time.Now().Add(2*time.Hour), ExpireOptions{NX: true}) {
		t.Fatal("NX must not overwrite an existing expiry")
	}
	if !c.Expire("k", time.Now().Add(2*time.Hour), ExpireOptions{GT: true}) {
		t.Fatal("GT should extend a shorter expiry")
	}
	if c.Expire("k", time.Now().Add(time.Minute), ExpireOptions{GT: true}) {
		t.Fatal("GT must not shorten an expiry")
	}
	if !c.Persist("k") {
		t.Fatal("Persist returned false")
	}
	if c.Persist("k") {
		t.Fatal("Persist on a key without TTL must return false")
	}
	// An expiry in the past deletes the key, as in Redis.
	if !c.Expire("k", time.Now().Add(-time.Second), ExpireOptions{}) || c.Exists("k") {
		t.Fatal("past expiry should delete the key")
	}
}

func TestGetDelAndGetEx(t *testing.T) {
	c := newTestCache()
	_ = c.Set("k", []byte("v"), time.Hour)
	if v, ok := c.GetDel("k"); !ok || string(v) != "v" {
		t.Fatalf("GetDel = %q %v", v, ok)
	}
	if c.Exists("k") {
		t.Fatal("GetDel left the key behind")
	}
	_ = c.Set("k2", []byte("v"), time.Hour)
	if _, ok := c.GetEx("k2", 0, true); !ok {
		t.Fatal("GetEx miss")
	}
	if _, _, hasTTL := c.TTL("k2"); hasTTL {
		t.Fatal("GETEX PERSIST did not clear the expiry")
	}
}

// A key present for the whole scan must be returned exactly once, regardless of
// how many calls the cursor takes.
func TestScanVisitsEveryKeyOnce(t *testing.T) {
	c := newTestCache()
	const n = 250
	for i := 0; i < n; i++ {
		if err := c.Set(keyN(i), []byte("v"), 0); err != nil {
			t.Fatal(err)
		}
	}
	seen := make(map[string]int)
	cursor := uint64(0)
	for iter := 0; ; iter++ {
		if iter > n+10 {
			t.Fatal("SCAN did not terminate")
		}
		next, keys := c.Scan(cursor, "*", 17)
		for _, k := range keys {
			seen[k]++
		}
		if next == 0 {
			break
		}
		cursor = next
	}
	if len(seen) != n {
		t.Fatalf("saw %d distinct keys, want %d", len(seen), n)
	}
	for k, count := range seen {
		if count != 1 {
			t.Fatalf("key %s returned %d times", k, count)
		}
	}
}

func TestScanMatch(t *testing.T) {
	c := newTestCache()
	_ = c.Set("user:1", []byte("a"), 0)
	_ = c.Set("user:2", []byte("b"), 0)
	_ = c.Set("post:1", []byte("c"), 0)
	var got []string
	for cursor := uint64(0); ; {
		next, keys := c.Scan(cursor, "user:*", 10)
		got = append(got, keys...)
		if next == 0 {
			break
		}
		cursor = next
	}
	if len(got) != 2 {
		t.Fatalf("MATCH user:* returned %v", got)
	}
}

func TestFlushAndLen(t *testing.T) {
	c := newTestCache()
	for i := 0; i < 10; i++ {
		_ = c.Set(keyN(i), []byte("v"), 0)
	}
	if c.Len() != 10 {
		t.Fatalf("Len = %d", c.Len())
	}
	if n := c.Flush(); n != 10 {
		t.Fatalf("Flush = %d", n)
	}
	if c.Len() != 0 || c.Stats().Bytes != 0 {
		t.Fatalf("flush left %d items / %d bytes", c.Len(), c.Stats().Bytes)
	}
	// The cache must still work after a flush.
	if err := c.Set("after", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	if !c.Exists("after") {
		t.Fatal("set after flush lost")
	}
}

func TestExpiredKeysAreInvisible(t *testing.T) {
	c := newTestCache()
	_ = c.Set("k", []byte("v"), time.Millisecond)
	time.Sleep(5 * time.Millisecond)
	if c.Exists("k") {
		t.Fatal("expired key still exists")
	}
	if _, _, keys := 0, 0, c.Keys("*", 0); len(keys) != 0 {
		t.Fatalf("KEYS returned expired key: %v", keys)
	}
	if _, keys := c.Scan(0, "*", 10); len(keys) != 0 {
		t.Fatalf("SCAN returned expired key: %v", keys)
	}
}

func TestGlobMatch(t *testing.T) {
	for _, tc := range []struct {
		pattern, s string
		want       bool
	}{
		{"*", "anything", true},
		{"*", "", true},
		{"user:*", "user:1", true},
		{"user:*", "post:1", false},
		{"h?llo", "hello", true},
		{"h?llo", "hllo", false},
		{"h[ae]llo", "hallo", true},
		{"h[ae]llo", "hillo", false},
		{"h[^e]llo", "hallo", true},
		{"h[^e]llo", "hello", false},
		{"h[a-c]llo", "hbllo", true},
		{"h[a-c]llo", "hdllo", false},
		{"a*b*c", "axxbyyc", true},
		{"a*b*c", "axxc", false},
		{`\*literal`, "*literal", true},
		// path.Match would refuse to let '*' cross a '/', which would silently
		// break namespaced keys. Redis globs have no path semantics.
		{"user:*", "user:1/profile", true},
		{"*a*a*a*b", "aaaaaaaaaaaaaaaaab", true},
		{"*a*a*a*b", "aaaaaaaaaaaaaaaaa", false},
	} {
		if got := GlobMatch(tc.pattern, tc.s); got != tc.want {
			t.Errorf("GlobMatch(%q, %q) = %v, want %v", tc.pattern, tc.s, got, tc.want)
		}
	}
}

func keyN(i int) string {
	return "key:" + string(rune('a'+i%26)) + ":" + itoa(i)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}
