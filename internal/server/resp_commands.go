package server

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/mohamedzrouga/mocache/internal/cache"
	"github.com/mohamedzrouga/mocache/internal/cluster"
)

// cmdSpec is one Redis command: how many arguments it takes, where its keys
// are, and what it does. The key positions follow Redis's own convention
// (first/last/step, last = -1 meaning "to the end"), because they serve double
// duty: cluster routing here, and the COMMAND reply that clients introspect.
type cmdSpec struct {
	name   string
	arity  int // exact count, or negative for "at least |arity|"
	first  int // index of the first key, 0 = no keys
	last   int
	step   int
	write  bool
	noAuth bool // may run before AUTH (HELLO, AUTH, PING, QUIT, RESET)
	flags  []string
	run    func(c *respConn, args [][]byte) bool
}

func (s *cmdSpec) arityOK(n int) bool {
	if s.arity >= 0 {
		return n == s.arity
	}
	return n >= -s.arity
}

// keysOf extracts the key arguments used for slot routing.
func (s *cmdSpec) keysOf(args [][]byte) [][]byte {
	if s.first <= 0 || s.first >= len(args) {
		return nil
	}
	last := s.last
	if last < 0 {
		last = len(args) - 1
	}
	if last >= len(args) {
		last = len(args) - 1
	}
	step := s.step
	if step < 1 {
		step = 1
	}
	var keys [][]byte
	for i := s.first; i <= last; i += step {
		keys = append(keys, args[i])
	}
	return keys
}

var commands = map[string]*cmdSpec{}

func register(s *cmdSpec) { commands[s.name] = s }

func init() {
	// --- connection & server ------------------------------------------------
	register(&cmdSpec{name: "PING", arity: -1, noAuth: true, flags: []string{"fast"}, run: cmdPing})
	register(&cmdSpec{name: "ECHO", arity: 2, flags: []string{"fast"}, run: cmdEcho})
	register(&cmdSpec{name: "QUIT", arity: -1, noAuth: true, run: cmdQuit})
	register(&cmdSpec{name: "RESET", arity: 1, noAuth: true, run: cmdReset})
	register(&cmdSpec{name: "AUTH", arity: -2, noAuth: true, run: cmdAuth})
	register(&cmdSpec{name: "HELLO", arity: -1, noAuth: true, run: cmdHello})
	register(&cmdSpec{name: "SELECT", arity: 2, run: cmdSelect})
	register(&cmdSpec{name: "CLIENT", arity: -2, noAuth: true, run: cmdClient})
	register(&cmdSpec{name: "COMMAND", arity: -1, noAuth: true, run: cmdCommand})
	register(&cmdSpec{name: "CONFIG", arity: -2, run: cmdConfig})
	register(&cmdSpec{name: "INFO", arity: -1, run: cmdInfo})
	register(&cmdSpec{name: "DBSIZE", arity: 1, flags: []string{"fast"}, run: cmdDBSize})
	register(&cmdSpec{name: "TIME", arity: 1, flags: []string{"fast"}, run: cmdTime})
	register(&cmdSpec{name: "FLUSHDB", arity: -1, write: true, run: cmdFlush})
	register(&cmdSpec{name: "FLUSHALL", arity: -1, write: true, run: cmdFlush})
	register(&cmdSpec{name: "MEMORY", arity: -2, first: 2, last: 2, step: 1, run: cmdMemory})
	register(&cmdSpec{name: "READONLY", arity: 1, run: cmdReadonly})
	register(&cmdSpec{name: "READWRITE", arity: 1, run: cmdReadwrite})
	register(&cmdSpec{name: "CLUSTER", arity: -2, run: cmdCluster})
	register(&cmdSpec{name: "WAIT", arity: 3, run: cmdWait})
	register(&cmdSpec{name: "REPLICAOF", arity: 3, run: cmdReplicaOf})
	register(&cmdSpec{name: "SLAVEOF", arity: 3, run: cmdReplicaOf})

	// --- keyspace -----------------------------------------------------------
	register(&cmdSpec{name: "DEL", arity: -2, first: 1, last: -1, step: 1, write: true, run: cmdDel})
	register(&cmdSpec{name: "UNLINK", arity: -2, first: 1, last: -1, step: 1, write: true, run: cmdDel})
	register(&cmdSpec{name: "EXISTS", arity: -2, first: 1, last: -1, step: 1, flags: []string{"readonly", "fast"}, run: cmdExists})
	register(&cmdSpec{name: "TOUCH", arity: -2, first: 1, last: -1, step: 1, flags: []string{"readonly", "fast"}, run: cmdTouch})
	register(&cmdSpec{name: "TYPE", arity: 2, first: 1, last: 1, step: 1, flags: []string{"readonly", "fast"}, run: cmdType})
	register(&cmdSpec{name: "TTL", arity: 2, first: 1, last: 1, step: 1, flags: []string{"readonly", "fast"}, run: cmdTTL})
	register(&cmdSpec{name: "PTTL", arity: 2, first: 1, last: 1, step: 1, flags: []string{"readonly", "fast"}, run: cmdTTL})
	register(&cmdSpec{name: "EXPIRE", arity: -3, first: 1, last: 1, step: 1, write: true, run: cmdExpire})
	register(&cmdSpec{name: "PEXPIRE", arity: -3, first: 1, last: 1, step: 1, write: true, run: cmdExpire})
	register(&cmdSpec{name: "EXPIREAT", arity: -3, first: 1, last: 1, step: 1, write: true, run: cmdExpire})
	register(&cmdSpec{name: "PEXPIREAT", arity: -3, first: 1, last: 1, step: 1, write: true, run: cmdExpire})
	register(&cmdSpec{name: "PERSIST", arity: 2, first: 1, last: 1, step: 1, write: true, run: cmdPersist})
	register(&cmdSpec{name: "KEYS", arity: 2, flags: []string{"readonly"}, run: cmdKeys})
	register(&cmdSpec{name: "SCAN", arity: -2, flags: []string{"readonly"}, run: cmdScan})
	register(&cmdSpec{name: "RANDOMKEY", arity: 1, flags: []string{"readonly"}, run: cmdRandomKey})
	register(&cmdSpec{name: "RENAME", arity: 3, first: 1, last: 2, step: 1, write: true, run: cmdRename})
	register(&cmdSpec{name: "RENAMENX", arity: 3, first: 1, last: 2, step: 1, write: true, run: cmdRename})

	// --- strings ------------------------------------------------------------
	register(&cmdSpec{name: "GET", arity: 2, first: 1, last: 1, step: 1, flags: []string{"readonly", "fast"}, run: cmdGet})
	register(&cmdSpec{name: "SET", arity: -3, first: 1, last: 1, step: 1, write: true, run: cmdSet})
	register(&cmdSpec{name: "SETNX", arity: 3, first: 1, last: 1, step: 1, write: true, run: cmdSetNX})
	register(&cmdSpec{name: "SETEX", arity: 4, first: 1, last: 1, step: 1, write: true, run: cmdSetEx})
	register(&cmdSpec{name: "PSETEX", arity: 4, first: 1, last: 1, step: 1, write: true, run: cmdSetEx})
	register(&cmdSpec{name: "GETSET", arity: 3, first: 1, last: 1, step: 1, write: true, run: cmdGetSet})
	register(&cmdSpec{name: "GETDEL", arity: 2, first: 1, last: 1, step: 1, write: true, run: cmdGetDel})
	register(&cmdSpec{name: "GETEX", arity: -2, first: 1, last: 1, step: 1, write: true, run: cmdGetEx})
	register(&cmdSpec{name: "MGET", arity: -2, first: 1, last: -1, step: 1, flags: []string{"readonly", "fast"}, run: cmdMGet})
	register(&cmdSpec{name: "MSET", arity: -3, first: 1, last: -1, step: 2, write: true, run: cmdMSet})
	register(&cmdSpec{name: "MSETNX", arity: -3, first: 1, last: -1, step: 2, write: true, run: cmdMSetNX})
	register(&cmdSpec{name: "APPEND", arity: 3, first: 1, last: 1, step: 1, write: true, run: cmdAppend})
	register(&cmdSpec{name: "STRLEN", arity: 2, first: 1, last: 1, step: 1, flags: []string{"readonly", "fast"}, run: cmdStrLen})
	register(&cmdSpec{name: "INCR", arity: 2, first: 1, last: 1, step: 1, write: true, run: cmdIncr})
	register(&cmdSpec{name: "DECR", arity: 2, first: 1, last: 1, step: 1, write: true, run: cmdIncr})
	register(&cmdSpec{name: "INCRBY", arity: 3, first: 1, last: 1, step: 1, write: true, run: cmdIncr})
	register(&cmdSpec{name: "DECRBY", arity: 3, first: 1, last: 1, step: 1, write: true, run: cmdIncr})
	register(&cmdSpec{name: "INCRBYFLOAT", arity: 3, first: 1, last: 1, step: 1, write: true, run: cmdIncrByFloat})
	register(&cmdSpec{name: "GETRANGE", arity: 4, first: 1, last: 1, step: 1, flags: []string{"readonly"}, run: cmdGetRange})
	register(&cmdSpec{name: "SUBSTR", arity: 4, first: 1, last: 1, step: 1, flags: []string{"readonly"}, run: cmdGetRange})
}

// --- connection ------------------------------------------------------------

func cmdPing(c *respConn, args [][]byte) bool {
	switch len(args) {
	case 1:
		c.w.Simple("PONG")
	case 2:
		c.w.Bulk(args[1])
	default:
		c.w.Error("ERR wrong number of arguments for 'ping' command")
		return true
	}
	return false
}

func cmdEcho(c *respConn, args [][]byte) bool { c.w.Bulk(args[1]); return false }

func cmdQuit(c *respConn, _ [][]byte) bool {
	c.w.OK()
	c.quit = true
	return false
}

// RESET returns the connection to its post-handshake state (RESP2, no name, no
// READONLY). Clients use it to recycle a connection between users.
func cmdReset(c *respConn, _ [][]byte) bool {
	c.w.SetProtocol(2)
	c.name, c.readonly = "", false
	c.authed = c.s.opt.Password == ""
	c.w.Simple("RESET")
	return false
}

func cmdAuth(c *respConn, args [][]byte) bool {
	if c.s.opt.Password == "" {
		c.w.Error("ERR Client sent AUTH, but no password is set. Did you mean AUTH <username> <password>?")
		return true
	}
	// AUTH <password> or AUTH <user> <password>; only the default user exists.
	pass := string(args[len(args)-1])
	if len(args) == 3 && string(args[1]) != "default" {
		c.w.Error("WRONGPASS invalid username-password pair or user is disabled.")
		return true
	}
	if pass != c.s.opt.Password {
		c.w.Error("WRONGPASS invalid username-password pair or user is disabled.")
		return true
	}
	c.authed = true
	c.w.OK()
	return false
}

// HELLO negotiates the protocol version and returns the server handshake map.
// go-redis sends "HELLO 3" on every new connection, so this is usually the
// first command a modern client issues.
func cmdHello(c *respConn, args [][]byte) bool {
	proto := c.w.Protocol()
	i := 1
	if len(args) > 1 {
		v, err := strconv.Atoi(string(args[1]))
		if err != nil {
			c.w.Error("NOPROTO unsupported protocol version")
			return true
		}
		if v != 2 && v != 3 {
			c.w.Error("NOPROTO unsupported protocol version")
			return true
		}
		proto = v
		i = 2
	}
	for i < len(args) {
		switch strings.ToUpper(string(args[i])) {
		case "AUTH":
			if i+2 >= len(args) {
				c.w.Error("ERR Protocol error: unexpected end of HELLO AUTH")
				return true
			}
			if c.s.opt.Password != "" && string(args[i+2]) != c.s.opt.Password {
				c.w.Error("WRONGPASS invalid username-password pair or user is disabled.")
				return true
			}
			c.authed = true
			i += 3
		case "SETNAME":
			if i+1 >= len(args) {
				c.w.Error("ERR Protocol error: unexpected end of HELLO SETNAME")
				return true
			}
			c.name = string(args[i+1])
			i += 2
		default:
			c.w.Errorf("ERR Protocol error: unknown HELLO option '%s'", string(args[i]))
			return true
		}
	}
	if !c.authed {
		c.w.Error("NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time")
		return true
	}
	// Switch framing only after the reply header is chosen, so the reply itself
	// is encoded in the version the client just asked for.
	c.w.SetProtocol(proto)
	role := "master"
	if c.s.view().Myself().Role == cluster.RoleReplica {
		role = "replica"
	}
	c.w.Map(7)
	c.w.BulkString("server")
	c.w.BulkString("mocache")
	c.w.BulkString("version")
	c.w.BulkString(compatVersion)
	c.w.BulkString("proto")
	c.w.Int(int64(proto))
	c.w.BulkString("id")
	c.w.Int(int64(c.id))
	c.w.BulkString("mode")
	c.w.BulkString(clusterMode(c))
	c.w.BulkString("role")
	c.w.BulkString(role)
	c.w.BulkString("modules")
	c.w.Array(0)
	return false
}

// SELECT exists only so clients that blindly send "SELECT 0" connect. MoCache
// has a single keyspace; any other index is an error, as in cluster mode.
func cmdSelect(c *respConn, args [][]byte) bool {
	n, err := strconv.Atoi(string(args[1]))
	if err != nil {
		c.w.Error("ERR value is not an integer or out of range")
		return true
	}
	if n != 0 {
		c.w.Error("ERR DB index is out of range")
		return true
	}
	c.w.OK()
	return false
}

func cmdClient(c *respConn, args [][]byte) bool {
	switch strings.ToUpper(string(args[1])) {
	case "ID":
		c.w.Int(int64(c.id))
	case "GETNAME":
		if c.name == "" {
			c.w.Nil()
			return false
		}
		c.w.BulkString(c.name)
	case "SETNAME":
		if len(args) != 3 {
			c.w.Error("ERR wrong number of arguments for 'client|setname' command")
			return true
		}
		if strings.ContainsAny(string(args[2]), " \n\r") {
			c.w.Error("ERR Client names cannot contain spaces, newlines or special characters.")
			return true
		}
		c.name = string(args[2])
		c.w.OK()
	case "SETINFO":
		// redis-py and go-redis announce their library name/version here on
		// every connection; the values are only used for CLIENT INFO output.
		if len(args) != 4 {
			c.w.Error("ERR wrong number of arguments for 'client|setinfo' command")
			return true
		}
		switch strings.ToUpper(string(args[2])) {
		case "LIB-NAME":
			c.lib = string(args[3])
		case "LIB-VER":
			c.libVer = string(args[3])
		default:
			c.w.Errorf("ERR Unrecognized option '%s'", string(args[2]))
			return true
		}
		c.w.OK()
	case "INFO":
		c.w.BulkString(c.info())
	case "LIST":
		c.w.BulkString(c.info() + "\n")
	case "NO-EVICT", "NO-TOUCH", "UNPAUSE", "REPLY":
		c.w.OK()
	default:
		c.w.Errorf("ERR Unknown CLIENT subcommand or wrong number of arguments for '%s'", string(args[1]))
		return true
	}
	return false
}

func (c *respConn) info() string {
	var b strings.Builder
	b.WriteString("id=" + strconv.FormatUint(c.id, 10))
	b.WriteString(" addr=" + c.conn.RemoteAddr().String())
	b.WriteString(" name=" + c.name)
	b.WriteString(" resp=" + strconv.Itoa(c.w.Protocol()))
	b.WriteString(" lib-name=" + c.lib)
	b.WriteString(" lib-ver=" + c.libVer)
	b.WriteString(" db=0")
	return b.String()
}

// COMMAND is served from the same table that drives routing, so what clients
// introspect can never drift from what the server actually accepts.
func cmdCommand(c *respConn, args [][]byte) bool {
	sub := ""
	if len(args) > 1 {
		sub = strings.ToUpper(string(args[1]))
	}
	switch sub {
	case "":
		c.w.Array(len(commands))
		for _, spec := range commands {
			writeCommandInfo(c, spec)
		}
	case "COUNT":
		c.w.Int(int64(len(commands)))
	case "DOCS":
		// Documentation is optional; an empty map keeps redis-cli happy.
		c.w.Map(0)
	case "INFO":
		if len(args) == 2 {
			c.w.Array(len(commands))
			for _, spec := range commands {
				writeCommandInfo(c, spec)
			}
			return false
		}
		c.w.Array(len(args) - 2)
		for _, name := range args[2:] {
			spec, ok := commands[strings.ToUpper(string(name))]
			if !ok {
				c.w.NilArray()
				continue
			}
			writeCommandInfo(c, spec)
		}
	case "GETKEYS":
		if len(args) < 3 {
			c.w.Error("ERR Unknown subcommand or wrong number of arguments for 'GETKEYS'")
			return true
		}
		spec, ok := commands[strings.ToUpper(string(args[2]))]
		if !ok {
			c.w.Error("ERR Invalid command specified")
			return true
		}
		keys := spec.keysOf(args[2:])
		if len(keys) == 0 {
			c.w.Error("ERR The command has no key arguments")
			return true
		}
		c.w.Array(len(keys))
		for _, k := range keys {
			c.w.Bulk(k)
		}
	default:
		c.w.Errorf("ERR Unknown subcommand or wrong number of arguments for '%s'", string(args[1]))
		return true
	}
	return false
}

func writeCommandInfo(c *respConn, spec *cmdSpec) {
	c.w.Array(10) // Redis 7 shape: clients index into this positionally
	c.w.BulkString(strings.ToLower(spec.name))
	c.w.Int(int64(spec.arity))
	flags := spec.flags
	if spec.write {
		flags = append([]string{"write", "denyoom"}, flags...)
	}
	c.w.Strings(flags)
	c.w.Int(int64(spec.first))
	last := spec.last
	if last < 0 {
		last = -1
	}
	c.w.Int(int64(last))
	c.w.Int(int64(spec.step))
	c.w.Array(0) // ACL categories
	c.w.Array(0) // tips
	c.w.Array(0) // key specs
	c.w.Array(0) // subcommands
}

// --- keyspace --------------------------------------------------------------

func cmdGet(c *respConn, args [][]byte) bool {
	v, ok := c.s.cache.Get(string(args[1]))
	if !ok {
		c.w.Nil()
		return false
	}
	c.w.Bulk(v)
	return false
}

func cmdSet(c *respConn, args [][]byte) bool {
	opt := cache.SetOptions{}
	for i := 3; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "NX":
			opt.NX = true
		case "XX":
			opt.XX = true
		case "GET":
			opt.Get = true
		case "KEEPTTL":
			opt.KeepTTL = true
		case "EX", "PX", "EXAT", "PXAT":
			if i+1 >= len(args) {
				c.w.Error("ERR syntax error")
				return true
			}
			n, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				c.w.Error("ERR value is not an integer or out of range")
				return true
			}
			ttl, ok := ttlFrom(strings.ToUpper(string(args[i])), n)
			if !ok {
				c.w.Error("ERR invalid expire time in 'set' command")
				return true
			}
			opt.TTL = ttl
			i++
		default:
			c.w.Error("ERR syntax error")
			return true
		}
	}
	if opt.NX && opt.XX {
		c.w.Error("ERR syntax error")
		return true
	}
	res, err := c.s.cache.SetWithOptions(string(args[1]), args[2], opt)
	if err != nil {
		return c.cacheError(err)
	}
	switch {
	case opt.Get && res.PrevFound:
		c.w.Bulk(res.Prev)
	case opt.Get:
		c.w.Nil()
	case res.Stored:
		c.w.OK()
	default:
		c.w.Nil() // NX/XX rejected the write
	}
	return false
}

// ttlFrom converts a SET/GETEX expiry argument to a duration. Absolute forms
// (EXAT/PXAT) become a duration from now; a non-positive result is invalid,
// which is how Redis rejects "expire in the past" on SET.
func ttlFrom(unit string, n int64) (time.Duration, bool) {
	var d time.Duration
	switch unit {
	case "EX":
		d = time.Duration(n) * time.Second
	case "PX":
		d = time.Duration(n) * time.Millisecond
	case "EXAT":
		d = time.Until(time.Unix(n, 0))
	case "PXAT":
		d = time.Until(time.UnixMilli(n))
	}
	return d, d > 0
}

func cmdSetNX(c *respConn, args [][]byte) bool {
	res, err := c.s.cache.SetWithOptions(string(args[1]), args[2], cache.SetOptions{NX: true})
	if err != nil {
		return c.cacheError(err)
	}
	c.w.Int(boolInt(res.Stored))
	return false
}

func cmdSetEx(c *respConn, args [][]byte) bool {
	n, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		c.w.Error("ERR value is not an integer or out of range")
		return true
	}
	unit := "EX"
	if strings.ToUpper(string(args[0])) == "PSETEX" {
		unit = "PX"
	}
	ttl, ok := ttlFrom(unit, n)
	if !ok {
		c.w.Errorf("ERR invalid expire time in '%s' command", strings.ToLower(string(args[0])))
		return true
	}
	if _, err := c.s.cache.SetWithOptions(string(args[1]), args[3], cache.SetOptions{TTL: ttl}); err != nil {
		return c.cacheError(err)
	}
	c.w.OK()
	return false
}

func cmdGetSet(c *respConn, args [][]byte) bool {
	res, err := c.s.cache.SetWithOptions(string(args[1]), args[2], cache.SetOptions{Get: true})
	if err != nil {
		return c.cacheError(err)
	}
	if !res.PrevFound {
		c.w.Nil()
		return false
	}
	c.w.Bulk(res.Prev)
	return false
}

func cmdGetDel(c *respConn, args [][]byte) bool {
	v, ok := c.s.cache.GetDel(string(args[1]))
	if !ok {
		c.w.Nil()
		return false
	}
	c.w.Bulk(v)
	return false
}

func cmdGetEx(c *respConn, args [][]byte) bool {
	var (
		ttl     time.Duration
		persist bool
	)
	for i := 2; i < len(args); i++ {
		switch u := strings.ToUpper(string(args[i])); u {
		case "PERSIST":
			persist = true
		case "EX", "PX", "EXAT", "PXAT":
			if i+1 >= len(args) {
				c.w.Error("ERR syntax error")
				return true
			}
			n, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				c.w.Error("ERR value is not an integer or out of range")
				return true
			}
			d, ok := ttlFrom(u, n)
			if !ok {
				c.w.Error("ERR invalid expire time in 'getex' command")
				return true
			}
			ttl = d
			i++
		default:
			c.w.Error("ERR syntax error")
			return true
		}
	}
	v, ok := c.s.cache.GetEx(string(args[1]), ttl, persist)
	if !ok {
		c.w.Nil()
		return false
	}
	c.w.Bulk(v)
	return false
}

func cmdMGet(c *respConn, args [][]byte) bool {
	c.w.Array(len(args) - 1)
	for _, k := range args[1:] {
		if v, ok := c.s.cache.Get(string(k)); ok {
			c.w.Bulk(v)
		} else {
			c.w.Nil()
		}
	}
	return false
}

func cmdMSet(c *respConn, args [][]byte) bool {
	if len(args)%2 != 1 {
		c.w.Error("ERR wrong number of arguments for 'mset' command")
		return true
	}
	for i := 1; i < len(args); i += 2 {
		if err := c.s.cache.Set(string(args[i]), args[i+1], 0); err != nil {
			return c.cacheError(err)
		}
	}
	c.w.OK()
	return false
}

func cmdMSetNX(c *respConn, args [][]byte) bool {
	if len(args)%2 != 1 {
		c.w.Error("ERR wrong number of arguments for 'msetnx' command")
		return true
	}
	// All-or-nothing: check first, then write. Both steps are on one slot (the
	// router rejected cross-slot), but they are separate critical sections, so
	// this is not atomic against a concurrent writer to the same keys.
	for i := 1; i < len(args); i += 2 {
		if c.s.cache.Exists(string(args[i])) {
			c.w.Int(0)
			return false
		}
	}
	for i := 1; i < len(args); i += 2 {
		if err := c.s.cache.Set(string(args[i]), args[i+1], 0); err != nil {
			return c.cacheError(err)
		}
	}
	c.w.Int(1)
	return false
}

func cmdAppend(c *respConn, args [][]byte) bool {
	n, err := c.s.cache.Append(string(args[1]), args[2])
	if err != nil {
		return c.cacheError(err)
	}
	c.w.Int(int64(n))
	return false
}

func cmdStrLen(c *respConn, args [][]byte) bool {
	c.w.Int(int64(c.s.cache.StrLen(string(args[1]))))
	return false
}

func cmdIncr(c *respConn, args [][]byte) bool {
	delta := int64(1)
	name := strings.ToUpper(string(args[0]))
	if name == "INCRBY" || name == "DECRBY" {
		n, err := strconv.ParseInt(string(args[2]), 10, 64)
		if err != nil {
			c.w.Error("ERR value is not an integer or out of range")
			return true
		}
		delta = n
	}
	if strings.HasPrefix(name, "DECR") {
		if delta == -1<<63 {
			c.w.Error("ERR decrement would overflow")
			return true
		}
		delta = -delta
	}
	n, err := c.s.cache.IncrBy(string(args[1]), delta)
	if err != nil {
		return c.cacheError(err)
	}
	c.w.Int(n)
	return false
}

func cmdIncrByFloat(c *respConn, args [][]byte) bool {
	f, err := strconv.ParseFloat(string(args[2]), 64)
	if err != nil {
		c.w.Error("ERR value is not a valid float")
		return true
	}
	s, err := c.s.cache.IncrByFloat(string(args[1]), f)
	if err != nil {
		return c.cacheError(err)
	}
	c.w.BulkString(s)
	return false
}

func cmdGetRange(c *respConn, args [][]byte) bool {
	start, err1 := strconv.Atoi(string(args[2]))
	end, err2 := strconv.Atoi(string(args[3]))
	if err1 != nil || err2 != nil {
		c.w.Error("ERR value is not an integer or out of range")
		return true
	}
	v, ok := c.s.cache.Get(string(args[1]))
	if !ok {
		c.w.BulkString("")
		return false
	}
	c.w.Bulk(sliceRange(v, start, end))
	return false
}

// sliceRange implements Redis's inclusive range with negative offsets counting
// back from the end, clamped to the value.
func sliceRange(v []byte, start, end int) []byte {
	n := len(v)
	if start < 0 {
		start += n
	}
	if end < 0 {
		end += n
	}
	if start < 0 {
		start = 0
	}
	if end >= n {
		end = n - 1
	}
	if n == 0 || start > end || start >= n {
		return nil
	}
	return v[start : end+1]
}

func cmdDel(c *respConn, args [][]byte) bool {
	var n int64
	for _, k := range args[1:] {
		if c.s.cache.Exists(string(k)) {
			c.s.cache.Delete(string(k))
			n++
		}
	}
	c.w.Int(n)
	return false
}

func cmdExists(c *respConn, args [][]byte) bool {
	var n int64
	for _, k := range args[1:] {
		if c.s.cache.Exists(string(k)) {
			n++ // counts duplicates, as Redis does
		}
	}
	c.w.Int(n)
	return false
}

func cmdTouch(c *respConn, args [][]byte) bool {
	var n int64
	for _, k := range args[1:] {
		if c.s.cache.Touch(string(k)) {
			n++
		}
	}
	c.w.Int(n)
	return false
}

func cmdType(c *respConn, args [][]byte) bool {
	c.w.Simple(c.s.cache.Type(string(args[1])))
	return false
}

func cmdTTL(c *respConn, args [][]byte) bool {
	d, exists, hasTTL := c.s.cache.TTL(string(args[1]))
	switch {
	case !exists:
		c.w.Int(-2)
	case !hasTTL:
		c.w.Int(-1)
	case strings.ToUpper(string(args[0])) == "PTTL":
		c.w.Int(int64(d / time.Millisecond))
	default:
		// Redis rounds up: 1500ms of TTL reports as 2 seconds, never 1.
		c.w.Int(int64((d + time.Second - 1) / time.Second))
	}
	return false
}

func cmdExpire(c *respConn, args [][]byte) bool {
	n, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		c.w.Error("ERR value is not an integer or out of range")
		return true
	}
	var at time.Time
	switch strings.ToUpper(string(args[0])) {
	case "EXPIRE":
		at = time.Now().Add(time.Duration(n) * time.Second)
	case "PEXPIRE":
		at = time.Now().Add(time.Duration(n) * time.Millisecond)
	case "EXPIREAT":
		at = time.Unix(n, 0)
	case "PEXPIREAT":
		at = time.UnixMilli(n)
	}
	var opt cache.ExpireOptions
	for _, a := range args[3:] {
		switch strings.ToUpper(string(a)) {
		case "NX":
			opt.NX = true
		case "XX":
			opt.XX = true
		case "GT":
			opt.GT = true
		case "LT":
			opt.LT = true
		default:
			c.w.Errorf("ERR Unsupported option %s", string(a))
			return true
		}
	}
	if (opt.NX && (opt.XX || opt.GT || opt.LT)) || (opt.GT && opt.LT) {
		c.w.Error("ERR NX and XX, GT or LT options at the same time are not compatible")
		return true
	}
	c.w.Int(boolInt(c.s.cache.Expire(string(args[1]), at, opt)))
	return false
}

func cmdPersist(c *respConn, args [][]byte) bool {
	c.w.Int(boolInt(c.s.cache.Persist(string(args[1]))))
	return false
}

func cmdKeys(c *respConn, args [][]byte) bool {
	c.w.Strings(c.s.cache.Keys(string(args[1]), c.s.opt.KeysLimit))
	return false
}

func cmdScan(c *respConn, args [][]byte) bool {
	cursor, err := strconv.ParseUint(string(args[1]), 10, 64)
	if err != nil {
		c.w.Error("ERR invalid cursor")
		return true
	}
	pattern, count := "*", 10
	for i := 2; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "MATCH":
			if i+1 >= len(args) {
				c.w.Error("ERR syntax error")
				return true
			}
			pattern = string(args[i+1])
			i++
		case "COUNT":
			if i+1 >= len(args) {
				c.w.Error("ERR syntax error")
				return true
			}
			n, err := strconv.Atoi(string(args[i+1]))
			if err != nil || n < 1 {
				c.w.Error("ERR syntax error")
				return true
			}
			count = n
			i++
		case "TYPE":
			if i+1 >= len(args) {
				c.w.Error("ERR syntax error")
				return true
			}
			// Only one type exists here; anything else matches nothing.
			if t := strings.ToLower(string(args[i+1])); t != "string" {
				c.w.Array(2)
				c.w.BulkString("0")
				c.w.Array(0)
				return false
			}
			i++
		default:
			c.w.Error("ERR syntax error")
			return true
		}
	}
	next, keys := c.s.cache.Scan(cursor, pattern, count)
	c.w.Array(2)
	c.w.BulkString(strconv.FormatUint(next, 10))
	c.w.Strings(keys)
	return false
}

func cmdRandomKey(c *respConn, _ [][]byte) bool {
	k, ok := c.s.cache.RandomKey()
	if !ok {
		c.w.Nil()
		return false
	}
	c.w.BulkString(k)
	return false
}

func cmdRename(c *respConn, args [][]byte) bool {
	src, dst := string(args[1]), string(args[2])
	nx := strings.ToUpper(string(args[0])) == "RENAMENX"
	v, ok := c.s.cache.Get(src)
	if !ok {
		c.w.Error("ERR no such key")
		return true
	}
	if nx && c.s.cache.Exists(dst) {
		c.w.Int(0)
		return false
	}
	// TTL is carried over, as Redis does.
	d, _, hasTTL := c.s.cache.TTL(src)
	var ttl time.Duration
	if hasTTL {
		ttl = d
	}
	if err := c.s.cache.Set(dst, v, ttl); err != nil {
		return c.cacheError(err)
	}
	c.s.cache.Delete(src)
	if nx {
		c.w.Int(1)
		return false
	}
	c.w.OK()
	return false
}

func cmdDBSize(c *respConn, _ [][]byte) bool {
	c.w.Int(int64(c.s.cache.Len()))
	return false
}

func cmdFlush(c *respConn, args [][]byte) bool {
	for _, a := range args[1:] {
		switch strings.ToUpper(string(a)) {
		case "ASYNC", "SYNC": // both are synchronous here; the LRU is in memory
		default:
			c.w.Error("ERR syntax error")
			return true
		}
	}
	c.s.cache.Flush()
	c.w.OK()
	return false
}

func cmdTime(c *respConn, _ [][]byte) bool {
	now := time.Now()
	c.w.Array(2)
	c.w.BulkString(strconv.FormatInt(now.Unix(), 10))
	c.w.BulkString(strconv.FormatInt(int64(now.Nanosecond()/1000), 10))
	return false
}

func cmdMemory(c *respConn, args [][]byte) bool {
	if strings.ToUpper(string(args[1])) != "USAGE" || len(args) < 3 {
		c.w.Errorf("ERR Unknown subcommand or wrong number of arguments for '%s'", string(args[1]))
		return true
	}
	key := string(args[2])
	if !c.s.cache.Exists(key) {
		c.w.Nil()
		return false
	}
	c.w.Int(int64(len(key) + c.s.cache.StrLen(key) + entryOverheadEstimate))
	return false
}

// WAIT reports how many replicas have acknowledged everything written so far.
// It is the only way a client can tell whether its write survived the loss of
// this node — replication is asynchronous, so without WAIT the answer is
// "probably".
func cmdWait(c *respConn, args [][]byte) bool {
	n, err1 := strconv.Atoi(string(args[1]))
	ms, err2 := strconv.ParseInt(string(args[2]), 10, 64)
	if err1 != nil || err2 != nil || n < 0 || ms < 0 {
		c.w.Error("ERR value is not an integer or out of range")
		return true
	}
	if c.s.opt.Runtime == nil {
		c.w.Int(0) // no replication configured: nobody has acknowledged anything
		return false
	}
	timeout := time.Duration(ms) * time.Millisecond
	if ms == 0 {
		// Redis treats 0 as "wait forever"; a cache should not hold a
		// connection open indefinitely, so this is capped.
		timeout = 30 * time.Second
	}
	offset := c.s.opt.Runtime.MasterOffset()
	c.w.Int(int64(c.s.opt.Runtime.WaitAcked(offset, n, timeout)))
	return false
}

// REPLICAOF is refused in cluster mode, exactly as Redis refuses it: slot
// ownership is cluster state, so changing it from one connection would leave
// the rest of the cluster disagreeing. CLUSTER REPLICATE is the supported way.
func cmdReplicaOf(c *respConn, _ [][]byte) bool {
	if c.s.view().Enabled() {
		c.w.Error("ERR REPLICAOF not allowed in cluster mode. Use CLUSTER REPLICATE instead.")
		return true
	}
	c.w.Error("ERR MoCache replication requires cluster mode (-cluster-peer)")
	return true
}

func cmdReadonly(c *respConn, _ [][]byte) bool {
	c.readonly = true
	c.w.OK()
	return false
}

func cmdReadwrite(c *respConn, _ [][]byte) bool {
	c.readonly = false
	c.w.OK()
	return false
}

// cacheError maps an LRU error onto the Redis error a client expects. An entry
// that exceeds the node's caps is OOM: the same class of failure as Redis
// hitting maxmemory with no evictable key.
func (c *respConn) cacheError(err error) bool {
	switch {
	case errors.Is(err, cache.ErrTooLarge):
		c.w.Error("OOM command not allowed when used memory > 'maxmemory'.")
	case errors.Is(err, cache.ErrNotInteger):
		c.w.Error("ERR value is not an integer or out of range")
	case errors.Is(err, cache.ErrNotFloat):
		c.w.Error("ERR value is not a valid float")
	case errors.Is(err, cache.ErrOverflow):
		c.w.Error("ERR increment or decrement would overflow")
	default:
		c.w.Errorf("ERR %v", err)
	}
	return true
}

func boolInt(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

func itoa(n int) string { return strconv.Itoa(n) }

func quoteArgs(args [][]byte, max int) string {
	var b strings.Builder
	for i, a := range args {
		if i >= max {
			b.WriteString("...")
			break
		}
		if i > 0 {
			b.WriteByte(' ')
		}
		b.WriteString(strconv.Quote(string(a)))
	}
	return b.String()
}
