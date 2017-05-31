package shardmaster

import (
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"syscall"

	"paxos"
	//	"golang.org/x/tools/godoc"
	"time"
)

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	configs []Config // indexed by config num
	ops     map[int]Op
	seen    map[int64]bool

	seenIdx int
	doneIdx int
}

const (
	JOIN  = "Join"
	LEAVE = "Leave"
	MOVE  = "Move"
	QUERY = "Query"
)

type OpType string
type Args interface{}
type Op struct {
	// Your data here.
	Type OpType
	OpId int64
	Args Args
}

func nrand() int64 {
	x := rand.Int63()
	return x
}

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
func (sm *ShardMaster) propose(op Op) {
	for {
		// propose the smallest value
		sm.mu.Lock()
		if sm.seen[op.OpId] {
			sm.mu.Unlock()
			return
		}
		opNo := sm.seenIdx
		sm.mu.Unlock()

		// attempt to propose the value
		// wait for this instance to make a decision
		sm.px.Start(opNo, op)
		to := 10 * time.Millisecond
		status, decision := sm.px.Status(opNo)
		for status == paxos.Pending {
			time.Sleep(to)
			if to < 10*time.Second {
				to *= 2
			}
			status, decision = sm.px.Status(opNo)
		}

		// continue if this instance has been forgotten
		if status == paxos.Forgotten {
			continue
		}

		curOp := decision.(Op)

		// garbage collect
		sm.px.Done(opNo)

		// only add op if it has not already been seen
		sm.mu.Lock()
		sm.ops[opNo] = curOp
		sm.seen[curOp.OpId] = true // mark op seen
		sm.seenIdx = max(sm.seenIdx, opNo+1)
		sm.mu.Unlock()
	}
}

func (cf *Config) add_rg(gid int64, servers []string) {
	cf.Num += 1
	cf.Groups[gid] = servers
}

//Returns a map , key : groupid, value: how many shards it has
func (cf *Config) loads() map[int64]int {
	rp_groups := make(map[int64]int)
	//Finds groups that are still up.
	for gid := range cf.Groups {
		rp_groups[gid] = 0
	}
	for _, gid := range cf.Shards {
		_, found := rp_groups[gid]
		if found {
			rp_groups[gid] += 1
		}
	}
	return rp_groups
}

//Returns the minimum loads gid and the amount
func (cf *Config) minimum_load() (int64, int) {
	rp_loads := cf.loads()
	var gid int64 = 0
	min_load := 9999999 //Can set this to Num+1
	for g, v := range rp_loads {
		if v < min_load {
			min_load = v
			gid = g
		}
	}
	return gid, min_load

}
func (cf *Config) maximum_load() (int64, int) {
	rp_loads := cf.loads()
	var gid int64 = 0
	max_load := 0 //Can set this to Num+1
	for g, v := range rp_loads {
		if v > max_load {
			max_load = v
			gid = g
		}
	}
	return gid, max_load

}
func (cf *Config) difference() int {
	_, maximum := cf.maximum_load()
	_, minimum := cf.minimum_load()
	return maximum - minimum
}

func (cf *Config) move_shard(s_index int, gid int64) {
	cf.Shards[s_index] = gid
}

func (cf *Config) assign_invalid_shards() {
	for shard, gid := range cf.Shards {
		if gid == 0 {
			gid1, _ := cf.minimum_load()
			cf.move_shard(shard, gid1)
		}

	}
}
func (cf *Config) balance() {
	for cf.difference() > 1 {
		gid1, _ := cf.maximum_load()
		gid2, _ := cf.minimum_load()
		//Find a shard in gid1 and move it to gid2
		for shard, gid := range cf.Shards {
			if gid == gid1 {
				cf.move_shard(shard, gid2)
				break

			}

		}

	}
}
func (sm *ShardMaster) evaluate() Config {
	var config Config

	sm.mu.Lock()
	defer sm.mu.Unlock()

	// apply all the ops in the log
	for ; sm.doneIdx < sm.seenIdx; sm.doneIdx++ {
		// get the earliest op that has not been executed
		op := sm.ops[sm.doneIdx]
		// garbage collect from ops
		delete(sm.ops, sm.doneIdx)

		// execute op
		switch op.Type {
		case "Join":
			var JoinArgs = (op.Args).(JoinArgs)
			sm.evaluateJoin(&JoinArgs)
		case "Leave":
			var LeaveArgs = op.Args.(LeaveArgs)
			sm.evaluateLeave(&LeaveArgs)
		case "Move":
			var MoveArgs = op.Args.(MoveArgs)
			sm.evaluateMove(&MoveArgs)
		case "Query":
			var QueryArgs = (op.Args).(QueryArgs)
			config = sm.evaluateQuery(&QueryArgs)
		}
	}
	return config
}

func (cf *Config) remove_group(gid int64) {
	cf.Num += 1
	delete(cf.Groups, gid)
}
func (cf *Config) assign_shards(gid int64) {
	for shard, gid1 := range cf.Shards {
		if gid == gid1 {
			min_gid, _ := cf.minimum_load()
			cf.move_shard(shard, min_gid)
		}

	}

}

func (cf *Config) copy_config() Config {
	var newConfig = Config{cf.Num, cf.Shards, make(map[int64][]string)}
	for key, value := range cf.Groups {
		newConfig.Groups[key] = value
	}
	return newConfig

}

func (sm *ShardMaster) evaluateJoin(args *JoinArgs) {
	previous_config := sm.configs[len(sm.configs)-1]
	new_config := previous_config.copy_config()
	new_config.add_rg(args.GID, args.Servers)
	new_config.assign_invalid_shards()
	new_config.balance()
	sm.configs = append(sm.configs, new_config)
}

func (sm *ShardMaster) evaluateLeave(args *LeaveArgs) {
	previous_config := sm.configs[len(sm.configs)-1]
	new_config := previous_config.copy_config()
	new_config.remove_group(args.GID)
	new_config.assign_shards(args.GID)
	new_config.balance()
	sm.configs = append(sm.configs, new_config)
}

func (sm *ShardMaster) evaluateMove(args *MoveArgs) {
	previous_config := sm.configs[len(sm.configs)-1]
	new_config := previous_config.copy_config()
	new_config.Num += 1
	new_config.move_shard(args.Shard, args.GID)
	sm.configs = append(sm.configs, new_config)
}

func (sm *ShardMaster) evaluateQuery(args *QueryArgs) Config {
	num := args.Num
	if num == -1 || num >= len(sm.configs) {
		return sm.configs[len(sm.configs)-1]
	}
	return sm.configs[num]

}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// create op
	op := Op{JOIN, nrand(), *args}

	// propose op
	sm.propose(op)

	// evaluate existing ops
	sm.evaluate()

	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// create op
	op := Op{LEAVE, nrand(), *args}

	// propose op
	sm.propose(op)

	// evaluate existing ops
	sm.evaluate()

	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// create op
	op := Op{MOVE, nrand(), *args}

	// propose op
	sm.propose(op)

	// evaluate existing ops
	sm.evaluate()

	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// create op
	op := Op{QUERY, nrand(), *args}

	// propose op
	sm.propose(op)

	reply.Config = sm.evaluate()
	return nil
}

// please don't change these two functions.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.l.Close()
	sm.px.Kill()
}

// call this to find out if the server is dead.
func (sm *ShardMaster) isdead() bool {
	return atomic.LoadInt32(&sm.dead) != 0
}

// please do not change these two functions.
func (sm *ShardMaster) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&sm.unreliable, 1)
	} else {
		atomic.StoreInt32(&sm.unreliable, 0)
	}
}

func (sm *ShardMaster) isunreliable() bool {
	return atomic.LoadInt32(&sm.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}
	sm.ops = map[int]Op{}
	sm.seen = map[int64]bool{}
	rpcs := rpc.NewServer()

	gob.Register(Op{})
	gob.Register(QueryArgs{})
	gob.Register(LeaveArgs{})
	gob.Register(MoveArgs{})
	gob.Register(JoinArgs{})
	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.isdead() == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.isdead() == false {
				if sm.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.isdead() == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
