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

// proposes the given op
// also learns what ops have been chosen, and garbage collects
func (sm *ShardMaster) proposeOp(op Op) {
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
		}
		sm.mu.Unlock()
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// check to see if this group has already joined

	// create op

	// propose op

	// evaluate existing ops

	return nil
}



func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.

	// check to see if this group is present

	// create op

	// propose op

	// evaluate existing ops

	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.

	// create op

	// propose op

	// evaluate existing ops

	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.

	// create op

	// propose op

	// evaluate existing op

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
