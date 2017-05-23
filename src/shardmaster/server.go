package shardmaster

import (
	"crypto/rand"
	"math/big"
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"syscall"

	"paxos"
	"golang.org/x/tools/godoc"
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

	ops []Op
	seen map[int64]bool

	seenIdx int
	doneIdx int
}

const (
	JOIN = iota
	LEAVE
	MOVE
	QUERY
)


type OpType int

type Op struct {
	// Your data here.
	Type OpType
	OpId int64

	GID int64 // for join/leave/move

	// for join
	Servers []string

	// for move
	Shard int

	// for query
	Num int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
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
		opNo := len(sm.configs)
		sm.mu.Unlock()

		// attempt to propose the value
		// wait for this instance to make a decision
		sm.px.Start(opNo, op)
		to := 10 * time.Millisecond
		status, decision := sm.px.Status(opNo)
		for status == paxos.Pending {
			time.Sleep(to)
			if to < 10 * time.Second {
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
		// unlock this sequence number
		if op.OpId == curOp.OpId {
			sm.ops[opNo] = curOp
			sm.seen[curOp.OpId] = true // mark op seen
			sm.seenIdx = max(sm.seenIdx, opNo + 1)
		}
		sm.mu.Unlock()
	}
}

func (sm *ShardMaster) evaluate() {
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
		case JOIN:
			sm.evaluateJoin(op)
		case LEAVE:
			sm.evaluateLeave(op)
		case MOVE:
			sm.evaluateMove(op)
		case QUERY:
			sm.evaluateQuery(op)
		}
	}
}

func (sm *ShardMaster) evaluateJoin(op Op) {

}

func (sm *ShardMaster) evaluateLeave(op Op) {

}

func (sm *ShardMaster) evaluateMove(op Op) {

}

func (sm *ShardMaster) evaluateQuery(op Op) {

}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// check if group is already present
	sm.mu.Lock()
	if _, ok := sm.configs[len(sm.configs) - 1].Groups[args.GID]; ok {
		sm.mu.Unlock()
		return nil
	}
	sm.mu.Unlock()

	// create op
	op := Op{JOIN, nrand(), args.GID, args.Servers, -1, -1}

	// propose op
	sm.propose(op)

	// evaluate existing ops
	sm.evaluate()

	return nil
}



func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.

	// check if group is not present
	sm.mu.Lock()
	if _, ok := sm.configs[len(sm.configs) - 1].Groups[args.GID]; !ok {
		sm.mu.Unlock()
		return nil
	}
	sm.mu.Unlock()

	// create op
	op := Op{LEAVE, nrand(), args.GID, nil, -1, -1}

	// propose op
	sm.propose(op)

	// evaluate existing ops
	sm.evaluate()

	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.

	// create op
	op := Op{MOVE, nrand(), args.GID, nil, args.Shard, -1}

	// propose op
	sm.propose(op)

	// evaluate existing ops
	sm.evaluate()

	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.

	// create op
	op := Op{QUERY, -1, nil, -1, args.Num}

	// propose op
	sm.propose(op)

	// evaluate existing ops
	sm.evaluate()

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
	sm.ops = map[int64]bool{}

	rpcs := rpc.NewServer()

	gob.Register(Op{})
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
