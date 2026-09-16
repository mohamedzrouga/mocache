package cache

import (
	"sync"
	"testing"
	"time"
)

func TestSetGetDelete(t *testing.T) {
	c := New(10)
	c.Set("a", []byte("1"), 0)
	v, ok := c.Get("a")
	if !ok || string(v) != "1" {
		t.Fatalf("got %q %v", v, ok)
	}
	c.Delete("a")
	if _, ok := c.Get("a"); ok {
		t.Fatal("expected miss after delete")
	}
	c.Delete("missing") // idempotent
}

func TestTTLExpiry(t *testing.T) {
	c := New(10)
	c.Set("a", []byte("1"), 30*time.Millisecond)
	if _, ok := c.Get("a"); !ok {
		t.Fatal("expected hit before expiry")
	}
	time.Sleep(40 * time.Millisecond)
	if _, ok := c.Get("a"); ok {
		t.Fatal("expected miss after expiry")
	}
	st := c.Stats()
	if st.Misses == 0 {
		t.Fatal("expiry should count as miss")
	}
}

func TestLRUEviction(t *testing.T) {
	c := New(2)
	c.Set("a", []byte("a"), 0)
	c.Set("b", []byte("b"), 0)
	c.Get("a") // a is now most recently used
	c.Set("c", []byte("c"), 0)
	if _, ok := c.Get("b"); ok {
		t.Fatal("b should have been evicted")
	}
	if _, ok := c.Get("a"); !ok {
		t.Fatal("a should remain")
	}
	if _, ok := c.Get("c"); !ok {
		t.Fatal("c should remain")
	}
	if c.Stats().Evictions != 1 {
		t.Fatalf("evictions=%d", c.Stats().Evictions)
	}
}

func TestSetExistingDoesNotEvict(t *testing.T) {
	c := New(1)
	c.Set("a", []byte("1"), 0)
	c.Set("a", []byte("2"), 0)
	v, ok := c.Get("a")
	if !ok || string(v) != "2" {
		t.Fatalf("got %q %v", v, ok)
	}
	if c.Stats().Evictions != 0 {
		t.Fatal("update must not evict")
	}
}

func TestGetCopiesValue(t *testing.T) {
	c := New(1)
	c.Set("a", []byte("xyz"), 0)
	v, _ := c.Get("a")
	v[0] = 'Q'
	v2, _ := c.Get("a")
	if string(v2) != "xyz" {
		t.Fatal("caller mutation leaked into cache")
	}
}

func TestConcurrent(t *testing.T) {
	c := New(1000)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				k := string(rune('a' + (n+j)%26))
				c.Set(k, []byte{byte(j)}, 0)
				c.Get(k)
				if j%7 == 0 {
					c.Delete(k)
				}
			}
		}(i)
	}
	wg.Wait()
}
