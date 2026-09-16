package mocache

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestRingWrapsAround(t *testing.T) {
	r := newHashRing([]string{"http://n0:8090", "http://n1:8090", "http://n2:8090"}, 100)
	if r.node("user:123") == "" {
		t.Fatal("empty node")
	}
	if r.node("user:123") != r.node("user:123") {
		t.Fatal("unstable")
	}
}

func TestRingMatchesPython(t *testing.T) {
	py := python313()
	if py == "" {
		t.Skip("python 3.13 not found")
	}
	root := repoRoot(t)
	nodes := []string{
		"http://cache-0.cache-headless.svc.cluster.local:8090",
		"http://cache-1.cache-headless.svc.cluster.local:8090",
		"http://cache-2.cache-headless.svc.cluster.local:8090",
	}
	keys := []string{"user:123", "a", "foo", "bar", "", "unicode- café", "k0", "k1", "k2"}
	r := newHashRing(nodes, 100)
	script := `
import sys
sys.path.insert(0, "sdk/python")
from mocache.client import HashRing
nodes = sys.argv[1].split("|")
keys = sys.argv[2].split("|")
ring = HashRing(nodes, 100)
for k in keys:
    print(ring.node(k))
`
	cmd := exec.Command(py, "-c", script, strings.Join(nodes, "|"), strings.Join(keys, "|"))
	cmd.Dir = root
	out, err := cmd.Output()
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			t.Fatalf("python: %s\n%s", err, ee.Stderr)
		}
		t.Fatal(err)
	}
	got := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(got) != len(keys) {
		t.Fatalf("python lines=%d want %d\n%s", len(got), len(keys), out)
	}
	for i, key := range keys {
		want := r.node(key)
		if got[i] != want {
			t.Errorf("key %q: python=%q go=%q", key, got[i], want)
		}
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		dir = filepath.Dir(dir)
	}
	t.Fatal("go.mod not found")
	return ""
}

func python313() string {
	for _, bin := range []string{
		"python3.13",
		"/Users/med/.local/share/uv/python/cpython-3.13-macos-aarch64-none/bin/python3.13",
	} {
		if err := exec.Command(bin, "-c", "import sys; assert sys.version_info[:2]==(3,13)").Run(); err == nil {
			return bin
		}
	}
	return ""
}
