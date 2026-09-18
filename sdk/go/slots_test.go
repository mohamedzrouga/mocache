package mocache

import "testing"

// These vectors are duplicated verbatim in sdk/python/tests/test_slots.py. The
// two SDKs must agree with each other *and* with internal/cluster, or a key
// written through one path is read as a miss through another. Changing any of
// them means changing all three.
var slotVectors = map[string]int{
	"hello":          866,
	"abc":            7638,
	"foo":            12182,
	"user:1":         10778, // the tag below is literally this key
	"{user:1}:name":  10778,
	"{user:1}:email": 10778,
	"":               0,
	"{}":             15257,
	"{a":             10276,
	"{a}b":           15495,
}

func TestCRC16CheckValue(t *testing.T) {
	// The standard CRC-16/XMODEM check value. Redis hard-codes this variant and
	// every client computes slots itself, so a change here misroutes silently
	// rather than failing.
	if got := crc16([]byte("123456789")); got != 0x31C3 {
		t.Fatalf("crc16(\"123456789\") = %#04x, want 0x31C3", got)
	}
}

func TestKeySlotVectors(t *testing.T) {
	for key, want := range slotVectors {
		if got := KeySlot(key); got != want {
			t.Errorf("KeySlot(%q) = %d, want %d", key, got, want)
		}
	}
}

func TestHashTagGroupsKeys(t *testing.T) {
	// A tag is what forces related keys onto one node.
	if KeySlot("{user:1}:name") != KeySlot("{user:1}:email") {
		t.Fatal("tagged keys landed on different slots")
	}
	// An empty or unterminated tag is not a tag: the whole key is hashed.
	if HashTag("{}") != "{}" || HashTag("{a") != "{a" || HashTag("no-tag") != "no-tag" {
		t.Fatal("an empty or unterminated tag was treated as a tag")
	}
	if HashTag("{a}b") != "a" {
		t.Fatalf("HashTag(\"{a}b\") = %q", HashTag("{a}b"))
	}
}

func TestSlotRouterPlacesKeys(t *testing.T) {
	r, err := newSlotRouter(map[string][]SlotRange{
		"http://a:8090": {{Start: 0, End: 5460}},
		"http://b:8090": {{Start: 5461, End: 10922}},
		"http://c:8090": {{Start: 10923, End: 16383}},
	})
	if err != nil {
		t.Fatalf("newSlotRouter: %v", err)
	}
	for key, want := range map[string]string{
		"hello": "http://a:8090",
		"abc":   "http://b:8090",
		"foo":   "http://c:8090",
	} {
		if got := r.node(key); got != want {
			t.Errorf("node(%q) = %s, want %s", key, got, want)
		}
	}
}

// A map with a gap or an overlap is refused at construction: routing part of
// the keyspace nowhere, or one key to two nodes depending on map order, is
// worse than failing.
func TestSlotRouterRejectsBadMaps(t *testing.T) {
	gap := map[string][]SlotRange{"http://a:8090": {{Start: 0, End: 16382}}}
	if _, err := newSlotRouter(gap); err == nil {
		t.Fatal("accepted a map with an uncovered slot")
	}
	overlap := map[string][]SlotRange{
		"http://a:8090": {{Start: 0, End: 10000}},
		"http://b:8090": {{Start: 9000, End: 16383}},
	}
	if _, err := newSlotRouter(overlap); err == nil {
		t.Fatal("accepted a map with two owners for one slot")
	}
}

func TestParseSlotRanges(t *testing.T) {
	got, err := ParseSlotRanges("5461-10922, 12000-12100 ,15000")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	want := []SlotRange{{5461, 10922}, {12000, 12100}, {15000, 15000}}
	if len(got) != len(want) {
		t.Fatalf("parsed %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("parsed %v, want %v", got, want)
		}
	}
	for _, bad := range []string{"", "abc", "10-5", "0-99999", "-1-5"} {
		if _, err := ParseSlotRanges(bad); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
}

// A client built with a bad slot map must fail loudly on every key-routed call
// rather than route to the wrong node, since New has no error to return.
func TestClientReportsBadSlotMap(t *testing.T) {
	c := New([]string{"http://a:8090"}, WithSlots(map[string]string{"http://a:8090": "0-99"}))
	defer c.Close()
	if err := c.Set("hello", []byte("v"), 0); err == nil {
		t.Fatal("Set succeeded with an incomplete slot map")
	}
	if _, _, err := c.Get("hello"); err == nil {
		t.Fatal("Get succeeded with an incomplete slot map")
	}
	if err := c.Delete("hello"); err == nil {
		t.Fatal("Delete succeeded with an incomplete slot map")
	}
}

// Without WithSlots the ring stays in charge, so an existing deployment's keys
// do not move when it upgrades.
func TestRingRemainsTheDefault(t *testing.T) {
	nodes := []string{"http://a:8090", "http://b:8090", "http://c:8090"}
	c := New(nodes)
	defer c.Close()
	ring := newHashRing(nodes, 100)
	for _, key := range []string{"hello", "abc", "foo", "user:1"} {
		if c.route.node(key) != ring.node(key) {
			t.Fatalf("default routing for %q changed", key)
		}
	}
}
