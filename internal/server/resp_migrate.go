package server

import (
	"errors"
	"net"
	"strconv"
	"strings"
	"time"
)

// MIGRATE and ASKING: the data half of a live slot migration.
//
// MIGRATE is issued to the node that currently owns the slot, and that node
// pushes the keys to their new home over the cluster bus. It is what redis-cli
// --cluster reshard calls in a loop between CLUSTER SETSLOT MIGRATING and
// CLUSTER SETSLOT NODE, so its argument shape has to match Redis exactly even
// though what travels between the two nodes is ours.

// cmdAsking marks the next command as allowed against a slot this node is
// importing but does not own yet. The flag is consumed by that one command,
// wherever it ends up being routed.
func cmdAsking(c *respConn, args [][]byte) bool {
	if !c.s.view().Enabled() {
		c.w.Error("ERR This instance has cluster support disabled")
		return true
	}
	c.asking = true
	c.w.OK()
	return false
}

// cmdMigrate implements
//
//	MIGRATE host port key destination-db timeout [COPY] [REPLACE] [KEYS k...]
//
// Keys that are not here are skipped rather than failing the call: a reshard
// loops over CLUSTER GETKEYSINSLOT, and a key evicted or expired between the
// listing and the move is normal in a cache, not an error.
func cmdMigrate(c *respConn, args [][]byte) bool {
	if c.s.opt.Runtime == nil {
		c.w.Error("ERR This instance has cluster support disabled")
		return true
	}
	host := string(args[1])
	port, err := strconv.Atoi(string(args[2]))
	if err != nil || port <= 0 || port > 65535 {
		c.w.Error("ERR Invalid target port")
		return true
	}
	if db, err := strconv.Atoi(string(args[4])); err != nil || db != 0 {
		c.w.Error("ERR DB index is out of range")
		return true
	}
	timeoutMs, err := strconv.Atoi(string(args[5]))
	if err != nil || timeoutMs < 0 {
		c.w.Error("ERR timeout is not an integer or out of range")
		return true
	}
	timeout := time.Duration(timeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = time.Second
	}

	keys, copyOnly, replace, perr := migrateOptions(args)
	if perr != nil {
		c.w.Errorf("ERR %v", perr)
		return true
	}

	entries := make([]MigratedKey, 0, len(keys))
	for _, k := range keys {
		v, exp, ok := c.s.cache.Export(k)
		if !ok {
			continue
		}
		entries = append(entries, MigratedKey{Key: k, Value: v, ExpireAt: exp})
	}
	if len(entries) == 0 {
		c.w.Simple("NOKEY")
		return false
	}

	addr := net.JoinHostPort(host, strconv.Itoa(port))
	if err := c.s.opt.Runtime.MigrateKeys(addr, entries, replace, timeout); err != nil {
		// BUSYKEY is the destination's own answer and carries its own code;
		// anything else is ours to label.
		if strings.HasPrefix(err.Error(), "BUSYKEY") {
			c.w.Error(err.Error())
		} else {
			c.w.Errorf("ERR Target instance replied with error: %v", err)
		}
		return true
	}
	// Only now: a key deleted before the destination confirmed it would be lost
	// outright, and a cache miss that nothing can recompute is still a bug.
	if !copyOnly {
		for _, e := range entries {
			c.s.cache.Delete(e.Key)
		}
	}
	c.w.OK()
	return false
}

func migrateOptions(args [][]byte) (keys []string, copyOnly, replace bool, err error) {
	keys = []string{string(args[3])}
	for i := 6; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "COPY":
			copyOnly = true
		case "REPLACE":
			replace = true
		case "KEYS":
			if len(args[3]) != 0 {
				return nil, false, false, errors.New("When using MIGRATE KEYS option, the key argument must be set to the empty string")
			}
			keys = nil
			for _, k := range args[i+1:] {
				keys = append(keys, string(k))
			}
			if len(keys) == 0 {
				return nil, false, false, errors.New("When using MIGRATE KEYS option, at least one key must be given")
			}
			return keys, copyOnly, replace, nil
		default:
			return nil, false, false, errors.New("Syntax error, try MIGRATE host port key dstdb timeout [COPY | REPLACE | KEYS key1 ... keyN]")
		}
	}
	return keys, copyOnly, replace, nil
}
