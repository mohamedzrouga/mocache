package cluster

import "testing"

// The CRC-16/XMODEM check value over "123456789" is 0x31C3. If this fails, the
// hash no longer matches Redis and every client would route to the wrong node.
func TestCRC16CheckValue(t *testing.T) {
	if got := crc16([]byte("123456789")); got != 0x31C3 {
		t.Fatalf("crc16(\"123456789\") = %#04x, want 0x31c3", got)
	}
}

// Slots taken from Redis's own CLUSTER KEYSLOT output.
func TestKeySlotMatchesRedis(t *testing.T) {
	for _, tc := range []struct {
		key  string
		slot int
	}{
		{"foo", 12182},
		{"bar", 5061},
		{"hello", 866},
		{"user:1000", 1649},
		{"somekey", 11058},
		{"", 0},
	} {
		if got := KeySlot(tc.key); got != tc.slot {
			t.Errorf("KeySlot(%q) = %d, want %d", tc.key, got, tc.slot)
		}
	}
}

func TestHashTag(t *testing.T) {
	for _, tc := range []struct{ key, tag string }{
		{"{user1000}.following", "user1000"},
		{"{user1000}.followers", "user1000"},
		{"foo{}{bar}", "foo{}{bar}"}, // empty first tag: whole key, as in Redis
		{"foo{{bar}}zap", "{bar"},
		{"foo{bar}{zap}", "bar"},
		{"nothashed", "nothashed"},
		{"{unclosed", "{unclosed"},
	} {
		if got := HashTag(tc.key); got != tc.tag {
			t.Errorf("HashTag(%q) = %q, want %q", tc.key, got, tc.tag)
		}
	}
}

// Keys sharing a hash tag must land on one slot, or multi-key commands break.
func TestHashTagCoLocates(t *testing.T) {
	a, b := KeySlot("{user:1}:name"), KeySlot("{user:1}:email")
	if a != b {
		t.Fatalf("tagged keys on different slots: %d vs %d", a, b)
	}
	if a != KeySlot("user:1") {
		t.Fatalf("tag slot %d does not match the untagged key slot %d", a, KeySlot("user:1"))
	}
}

func threeNodes() []string {
	return []string{
		"10.0.0.1:6379=0-5460",
		"10.0.0.2:6379=5461-10922",
		"10.0.0.3:6379=10923-16383",
	}
}

func TestParseTopology(t *testing.T) {
	topo, err := Parse(threeNodes(), "10.0.0.2:6379")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if !topo.Enabled() {
		t.Fatal("cluster should be enabled")
	}
	if got := topo.Myself().Addr(); got != "10.0.0.2:6379" {
		t.Fatalf("Myself = %s", got)
	}
	if topo.AssignedSlots() != SlotCount {
		t.Fatalf("assigned %d slots, want %d", topo.AssignedSlots(), SlotCount)
	}
	if topo.State() != "ok" {
		t.Fatalf("state = %s, want ok", topo.State())
	}
	if !topo.Mine(5461) || topo.Mine(5460) {
		t.Fatal("slot ownership boundary is wrong")
	}
	if owner := topo.OwnerOf(0); owner.Addr() != "10.0.0.1:6379" {
		t.Fatalf("slot 0 owner = %s", owner.Addr())
	}
	if len(topo.Myself().ID) != 40 {
		t.Fatalf("node ID %q is not 40 hex chars", topo.Myself().ID)
	}
	// IDs must be stable: a restart that renamed a node would move its slots.
	if NodeID("10.0.0.2:6379") != topo.Myself().ID {
		t.Fatal("node ID is not derived deterministically from the address")
	}
}

func TestParseRejectsBadTopology(t *testing.T) {
	for name, peers := range map[string][]string{
		"overlapping slots": {"10.0.0.1:6379=0-100", "10.0.0.2:6379=50-200"},
		"slot out of range": {"10.0.0.1:6379=0-99999"},
		"missing slots":     {"10.0.0.1:6379="},
		"no equals":         {"10.0.0.1:6379"},
		"bad port":          {"10.0.0.1:notaport=0-10"},
		"unknown primary":   {"10.0.0.1:6379=0-16383", "10.0.0.9:6379=replica-of:10.0.0.7:6379"},
	} {
		if _, err := Parse(peers, "10.0.0.1:6379"); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
	if _, err := Parse(threeNodes(), "10.9.9.9:6379"); err == nil {
		t.Error("announce outside the peer list should fail: the node cannot know which entry it is")
	}
}

// A partial slot map must report fail, so clients do not treat the cluster as
// usable while part of the keyspace has nowhere to go.
func TestIncompleteCoverageIsNotOK(t *testing.T) {
	topo, err := Parse([]string{"10.0.0.1:6379=0-100"}, "10.0.0.1:6379")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if topo.State() != "fail" {
		t.Fatalf("state = %s, want fail", topo.State())
	}
	if topo.OwnerOf(9000) != nil {
		t.Fatal("unassigned slot must have no owner")
	}
}

func TestReplicas(t *testing.T) {
	peers := append(threeNodes(), "10.0.0.4:6379=replica-of:10.0.0.1:6379")
	topo, err := Parse(peers, "10.0.0.4:6379")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	me := topo.Myself()
	if me.Role != RoleReplica {
		t.Fatal("node 4 should be a replica")
	}
	if p := topo.PrimaryOf(me); p == nil || p.Addr() != "10.0.0.1:6379" {
		t.Fatal("replica does not resolve to its primary")
	}
	// A replica owns no slots of its own: reads are served only after READONLY.
	if topo.Mine(0) {
		t.Fatal("a replica must not claim ownership of its primary's slots")
	}
	if reps := topo.ReplicasOf(topo.OwnerOf(0)); len(reps) != 1 {
		t.Fatalf("primary should have 1 replica, got %d", len(reps))
	}
}

func TestDisabledTopologyOwnsEverything(t *testing.T) {
	topo := Disabled("127.0.0.1", 6379)
	if topo.Enabled() {
		t.Fatal("standalone topology must report cluster disabled")
	}
	if !topo.Mine(0) || !topo.Mine(SlotCount-1) {
		t.Fatal("standalone node must serve every slot")
	}
}
