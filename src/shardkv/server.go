package shardkv

import (
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"paxos"
	"shardmaster"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type OpType string
type Args interface{}
type Result interface{}
type Op struct {
	Type OpType
	OpId int64
	Args Args
}

const (
	GET       = "Get"
	PUTAPPEND = "PutAppend"
	RE_CONFIG = "Re_Config"
)

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	// Your definitions here.
	ops             map[int]Op
	seen            map[int64]bool
	applied         map[int64]bool
	seenIdx         int
	doneIdx         int
	current_config  shardmaster.Config
	storage         map[string]string
	muSeq           map[string]*sync.Mutex
	cache           map[string]bool
	shards          []bool
	next_config_num int //-1 if we are not transitioning
}

// returns true iff the op with the given id has been applied
// false otherwise
func (kv *ShardKV) check_duplicates(OpId int64) bool {
	return kv.applied[OpId]
}

// generates and returns a unique shard identifier
func combine(x int, y int) string {
	return strconv.Itoa(x) + ":" + strconv.Itoa(y)
}


// proposes and evaluates a given op
// when this function returns, the op will have been applied on this server
func (kv *ShardKV) proposeEvaluate(op Op) Result {
	kv.propose(op)
	result := kv.evaluate()

	return result
}

// Get RPC handler
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// return if server is either uninitialized, or is transitioning to a new configuration
	if !kv.is_initialized() || kv.next_config_num != -1 {
		return nil
	}

	// generate a unique per key lock if one does not exist, and lock it
	if _, ok := kv.muSeq[args.Key]; !ok {
		kv.muSeq[args.Key] = &sync.Mutex{}
	}
	kv.muSeq[args.Key].Lock()
	defer kv.muSeq[args.Key].Unlock()

	// format Op struct
	op := Op{GET, nrand(), *args}

	// propose and evaluate op
	result := kv.proposeEvaluate(op)

	// format reply struct
	reply.Err = result.(GetReply).Err
	reply.Value = result.(GetReply).Value

	return nil
}

// PutAppend RPC handler
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// return if server is either uninitialized, or is transitioning to a new configuration
	if !kv.is_initialized() || kv.next_config_num != -1 {
		return nil
	}

	// generate a unique per key lock if one does not exist, and lock it
	if _, ok := kv.muSeq[args.Key]; !ok {
		kv.muSeq[args.Key] = &sync.Mutex{}
	}
	kv.muSeq[args.Key].Lock()
	defer kv.muSeq[args.Key].Unlock()

	// format Op struct
	op := Op{PUTAPPEND, nrand(), *args}

	// propose and evaluate op
	result := kv.proposeEvaluate(op)

	// format reply struct
	reply.Err = result.(PutAppendReply).Err

	return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.next_config_num == -1 {
		// case 1: server is not transitioning to a new config
		if kv.current_config.Num == 0 {
			// case 1a: server has not been initialized

			// get the current config
			config := kv.sm.Query(1)

			if config.Num == 1 {
				// initialize state based on the 1st config
				kv.current_config = config
				kv.assign_shards()
			}
		} else {
			// case 1b: server has been initialized
			// check for a new config
			config := kv.sm.Query(-1)
			if config.Num > kv.current_config.Num {
				// there is a higher config
				// propose reconfiguration
				args := ReconfigArgs{kv.current_config.Num + 1}
				op := Op{RE_CONFIG, nrand(), args}
				kv.proposeEvaluate(op)
			}
		}
	} else {
		// case 2: server is currently transitioning to a new config
		// send and receive shards
		kv.send_shards()
		sent := kv.all_sent()
		received := kv.all_received()

		if sent && received {
			// all shards have been sent and received
			// propose end to reconfiguration
			op := Op{"Re_Config_Done", nrand(), DummyArgs{}}
			kv.proposeEvaluate(op)
		}
	}
}

// sends all shards to their assigned groups
func (kv *ShardKV) send_shards() {
	for shard_index, _ := range kv.shards {
		if kv.shards[shard_index] == false {
			// we do not own this shard
			continue
		}

		if kv.shards[shard_index] && kv.current_config.Shards[shard_index] != kv.gid {
			// we own this shard but it belongs in a new group
			r_gid := kv.current_config.Shards[shard_index]

			// send the shard to the new group
			kv.send_shard(r_gid, shard_index)
		}
	}
}

// returns true iff the server's config is behind the current config
// false, otherwise
func (kv *ShardKV) is_behind(config_num int) bool {
	return kv.current_config.Num < config_num
}

// handles receive_shard op agreed upon by paxos
func (kv *ShardKV) receive_shard(args *SendShardArgs) Result {
	reply := SendShardReply{}

	if _, found := kv.cache[combine(args.Config_num, args.Shard_index)]; found {
		// there is a duplicate, format reply and return
		reply.Err = OK
		return reply
	}

	if kv.is_uptodate(args.Config_num) {
		// the shard is up to date with the current config
		// add the shard to server state
		for _, pair := range args.Storage {
			kv.storage[pair.Key] = pair.Value
		}
		for _, id := range args.Applied {
			kv.applied[id] = true
		}

		// update state to show we own shard, and that this shard operation has been applied
		kv.shards[args.Shard_index] = true
		kv.cache[combine(args.Config_num, args.Shard_index)] = true

		// format reply and return
		reply.Err = OK
		return reply
	}

	if kv.is_behind(args.Config_num) {
		// the shard is not up to date with the current config
		// format reply and return
		reply.Err = Waiting
		return reply
	}

	// default case
	// update state to show we own shard
	kv.cache[combine(args.Config_num, args.Shard_index)] = true

	// format reply and return
	reply.Err = OK
	return reply
}

// sends the shard with shard_index to the group with id r_gid
func (kv *ShardKV) send_shard(r_gid int64, shard_index int) {
	var storage_toSend []Pair
	var applied []int64

	// create list of Pair structs with shard data on them
	for key, value := range kv.storage {
		if key2shard(key) == shard_index {
			storage_toSend = append(storage_toSend, Pair{key, value})
		}
	}

	// copy list of applied ops
	for id, _ := range kv.applied {
		applied = append(applied, id)
	}

	for _, srv := range kv.current_config.Groups[r_gid] {
		// form RPC args struct
		opId := nrand()
		args := &SendShardArgs{}
		args.Storage = storage_toSend
		args.Config_num = kv.next_config_num
		args.Shard_index = shard_index
		args.Applied = applied
		args.OpId = opId

		// create RPC reply struct
		var reply SendShardReply

		// call RPC
		ok := call(srv, "ShardKV.Receive_shard", args, &reply)

		if ok && reply.Err == OK {
			// RPC succeeded

			// format delete shard op, and propose and evaluate delete shard op
			deleteArgs := DeleteShardArgs{shard_index, kv.next_config_num}
			op := Op{"DeleteShard", nrand(), deleteArgs}
			kv.proposeEvaluate(op)
			return
		}
	}
}

// returns true iff all shards from this server have been sent to the group they belong to in the current config
// false, otherwise
func (kv *ShardKV) all_sent() bool {
	for shard_index, _ := range kv.shards {
		if kv.shards[shard_index] == true && kv.current_config.Shards[shard_index] != kv.gid {
			// there is a shard on this server that belongs in a different group in the current config
			return false
		}
	}
	return true
}

// returns true iff all of the shards that belong to this server according to the current config have been received
// false, otherwise
func (kv *ShardKV) all_received() bool {
	for shard_index, _ := range kv.shards {
		if kv.shards[shard_index] == false && kv.current_config.Shards[shard_index] == kv.gid {
			// this server does not have a shard it should have
			return false
		}
	}
	return true
}

// returns true iff this server's state has been initialized
// false, otherwise
func (kv *ShardKV) is_initialized() bool {
	return kv.current_config.Num > 0
}

// returns true iff config_num is up to date with the current config num
// false, otherwise
func (kv *ShardKV) is_uptodate(config_num int) bool {
	return kv.current_config.Num == config_num
}

// Receive_shard RPC handler
func (kv *ShardKV) Receive_shard(args *SendShardArgs, reply *SendShardReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if !kv.is_initialized() {
		// this server has not been initialized return
		return nil
	}

	// propose ReceiveShard to paxos
	// format op struct
	op := Op{"ReceiveShard", nrand(), *args}

	// propose and evaluate
	internalReply := kv.proposeEvaluate(op)

	// format reply
	reply.Err = internalReply.(SendShardReply).Err

	return nil
}

// assigns shards according to the current config
func (kv *ShardKV) assign_shards() {
	for shard_index, gid := range kv.current_config.Shards {
		if gid == kv.gid {
			// this server owns the shard
			kv.shards[shard_index] = true
		} else {
			// this server does not own the shard
			kv.shards[shard_index] = false
		}
	}
}

// returns a iff a > b, otherwise, b
func max(a, b int) (c int) {
	if a > b {
		c = a
	} else {
		c = b
	}

	return
}

// proposes the given op
// also learns what ops have been chosen, and garbage collects
func (kv *ShardKV) propose(op Op) {
	for {
		// propose the smallest value
		if kv.seen[op.OpId] {
			return
		}
		opNo := kv.seenIdx

		// attempt to propose the value
		// wait for this instance to make a decision
		kv.px.Start(opNo, op)
		to := 10 * time.Millisecond
		status, decision := kv.px.Status(opNo)
		for status == paxos.Pending {
			time.Sleep(to)
			if to < 10*time.Second {
				to *= 2
			}
			status, decision = kv.px.Status(opNo)
		}

		// continue if this instance has been forgotten
		if status == paxos.Forgotten {
			continue
		}

		curOp := decision.(Op)

		// garbage collect
		kv.px.Done(opNo)

		// only add op if it has not already been seen
		kv.ops[opNo] = curOp
		kv.seen[curOp.OpId] = true // mark op seen
		kv.seenIdx = max(kv.seenIdx, opNo+1)
	}
}

// evaluates all operations in the op log
func (kv *ShardKV) evaluate() Result {
	var result Result

	// apply all the ops in the log
	for ; kv.doneIdx < kv.seenIdx; kv.doneIdx++ {
		// get the earliest op that has not been executed
		op := kv.ops[kv.doneIdx]

		// garbage collect from ops
		delete(kv.ops, kv.doneIdx)

		// execute op
		switch op.Type {
		case "Get":
			var GetArgs = (op.Args).(GetArgs)
			result = kv.get(&GetArgs)
		case "PutAppend":
			var PutAppendArgs = op.Args.(PutAppendArgs)
			result = kv.put_append(&PutAppendArgs)
		case "Re_Config":
			var ReconfigArgs = op.Args.(ReconfigArgs)
			kv.re_config(&ReconfigArgs)
		case "ReceiveShard":
			var SendShardArgs = op.Args.(SendShardArgs)
			result = kv.receive_shard(&SendShardArgs)
		case "Re_Config_Done":
			kv.re_config_done()
		case "DeleteShard":
			var DeleteShardArgs = op.Args.(DeleteShardArgs)
			kv.delete_shard(&DeleteShardArgs)
		case "NOP":
		// do nothing
		}
	}
	return result
}

// deletes shard from server that it no longer owns when agreed upon by paxos
func (kv *ShardKV) delete_shard(args *DeleteShardArgs) {
	if kv.is_uptodate(args.Config_num) {
		// shard is up to date with current config
		for key, _ := range kv.storage {
			// remove each key in shard from server
			if key2shard(key) == args.Shard_index {
				delete(kv.storage, key)
			}
		}

	}
	kv.shards[args.Shard_index] = false
}

// ends reconfig when agreed upon by paxos
func (kv *ShardKV) re_config_done() {
	kv.next_config_num = -1
}

// begins reconfig when agreed upon by paxos
func (kv *ShardKV) re_config(args *ReconfigArgs) {
	if kv.is_uptodate(kv.next_config_num) || args.Config_num != kv.current_config.Num + 1 {
		// server is either up to date or this reconfig is not the next applicable reconfig
		return
	}

	// update the next_config to reflect state of next config
	next_config := kv.sm.Query(kv.current_config.Num + 1)
	for next_config.Num != kv.current_config.Num + 1 {
		next_config = kv.sm.Query(kv.current_config.Num + 1)
	}
	kv.assign_shards()
	kv.current_config = next_config
	kv.next_config_num = next_config.Num
}


// returns true iff this server currently owns the shard with the given key
// otherwise, false
func (kv *ShardKV) currently_has_shard(key string) bool {
	shard_index := key2shard(key)
	return kv.shards[shard_index]
}

// returns true iff we know that the server owns the shard with the given key in the next config
// otherwise, false
func (kv *ShardKV) has_shard(key string) bool {
	shard_index := key2shard(key)
	return kv.current_config.Shards[shard_index] == kv.gid
}

// handles get when agreed upon by paxos log
func (kv *ShardKV) get(args *GetArgs) Result {
	reply := GetReply{}

	if kv.has_shard(args.Key) && kv.currently_has_shard(args.Key) {
		// case 1: this server owns the shard and has the shard contents
		value, found := kv.storage[args.Key]

		// return the value associated with hte key
		if found {
			reply.Value = value
			reply.Err = OK
		} else {
			reply.Value = ""
			reply.Err = ErrNoKey
		}
		return reply
	}

	if kv.has_shard(args.Key) && !kv.currently_has_shard(args.Key) {
		// case 2: server is in transition
		reply.Err = Waiting

		return reply
	}

	// case 3: this server does not own the shard
	reply.Err = ErrWrongGroup
	return reply
}

// handles put_append when agreed upon by paxos log
func (kv *ShardKV) put_append(args *PutAppendArgs) Result {
	reply := PutAppendReply{}

	if kv.check_duplicates(args.OpId) {
		// case 1: a duplicate in the op log has been applied, return
		reply.Err = OK
		return reply
	}

	if kv.has_shard(args.Key) && kv.currently_has_shard(args.Key) {
		// case 2: this server owns the shard and has the shard contents
		kv.applied[args.OpId] = true

		if args.Op == "Put" {
			// case 2a: execute the put
			kv.storage[args.Key] = args.Value
			reply.Err = OK
		} else {
			// case 2b: execute the append
			kv.storage[args.Key] = kv.storage[args.Key] + args.Value
			reply.Err = OK
		}
		return reply
	}

	if kv.has_shard(args.Key) && !kv.currently_has_shard(args.Key) {
		// case 3: this server is in transition
		reply.Err = Waiting
		return reply
	}

	// case 4: this server does not own the shard
	reply.Err = ErrWrongGroup
	return reply
}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *ShardKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *ShardKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *ShardKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *ShardKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
servers []string, me int) *ShardKV {
	gob.Register(Op{})
	gob.Register(GetArgs{})
	gob.Register(GetReply{})
	gob.Register(PutAppendReply{})
	gob.Register(DummyArgs{})
	gob.Register(DeleteShardArgs{})
	gob.Register(Pair{})
	gob.Register(PutAppendArgs{})
	gob.Register(SendShardArgs{})
	gob.Register(ReconfigArgs{})
	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)
	kv.muSeq = make(map[string]*sync.Mutex)

	// Your initialization code here.
	kv.current_config = shardmaster.Config{} // represents the most up to date config
	kv.current_config.Groups = map[int64][]string{}

	kv.storage = map[string]string{} // stores key-value pairs
	kv.next_config_num = -1 // if in transition, has the next config number, otherwise -1
	kv.shards = make([]bool, shardmaster.NShards) // map of shards -> bools that indicate whether or not this server owns a shard
	kv.ops = map[int]Op{} // op log as agreed by paxos, ops should be executed in increasing order of key
	kv.seen = map[int64]bool{} // log of operations that have been decided by paxos
	kv.applied = map[int64]bool{} // log of operations that have been applied to this server
	kv.cache = map[string]bool{} // cache of shard operations that have been applied to this server
	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.isdead() == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}