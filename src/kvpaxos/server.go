package kvpaxos

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
	//"crypto/tls"
	"time"
)

// TODO: implement concurrency and locks correctly

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType string
	Key    string
	Value  string // empty string if get operation

	// for duplicate detection
	Cid int64
	SeqNo int
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// Your definitions here.
	kvstore map[string]string
	ops     []Op
}

// Returns whether or not two operations are equal
// op1 == op2 iff they have the same client id and sequence number
func (op *Op) equals(other *Op) bool {
	if op.Cid != other.Cid && op.SeqNo != other.SeqNo {
		return false
	}

	return true
}

// Returns whether or not two operations are equal
// op1 == op2 iff they have the same client id and sequence number
func (op *Op) equals2(other Op) bool {
	if op.Cid != other.Cid && op.SeqNo != other.SeqNo {
		return false
	}

	return true
}

// returns true iff this op has already been assigned a sequence number
func (kv *KVPaxos) hasDuplicates(op *Op) bool {
	// TODO: this probably isn't sufficient
	for i := 0; i < len(kv.ops); i++ {
		if op.equals(&kv.ops[i]) {
			return true
		}
	}

	return false
}

// continues to propose an operation to paxos until it is finally accepted
// only returns after it has been accepted by some sequence number
func (kv *KVPaxos) propose(op *Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	opNo := kv.px.Min()
	for {
		// case 1: this instance is done, continue
		if fate, _ := kv.px.Status(opNo); fate == paxos.Decided {
			continue
		}

		// case 2: this instance is not done, propose the value
		kv.px.Start(opNo, op)

		// wait until it has been decided
		status, decision := kv.px.Status(opNo)
		for status != paxos.Decided {
			time.Sleep(time.Second)
			status, decision = kv.px.Status(opNo)
		}

		// if the decided value is the op we proposed, we're done
		if op.equals2(decision.(Op)) {
			return
		}

		// otherwise continue
		opNo++
	}
}

//	 Put and Append are probably wrong
func (kv *KVPaxos) garbageCollect() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// go through all the operations that have been decided (in order), and execute them, then delete their instances
	opNo := kv.px.Min()
	for status, decision := kv.px.Status(opNo); status == paxos.Decided; opNo++ {
		kv.ops[opNo] = decision.(Op)
	}

	kv.px.Done(opNo - 1)
}


func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// create and populate op struct
	op := new(Op)
	op.OpType = "Get"
	op.Key = args.Key
	op.Value = ""

	op.Cid = args.Cid
	op.SeqNo = args.SeqNo

	// TODO: figure out how to handle duplicates
	if kv.hasDuplicates(op) {
		return nil
	}

	kv.propose(op)
	kv.garbageCollect()

	// TODO: format reply

	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.

	// create and populate op struct
	op := new(Op)
	op.OpType = args.Op
	op.Key = args.Key
	op.Value = args.Value

	op.Cid = args.Cid
	op.SeqNo = args.SeqNo

	// TODO: figure out how to handle duplicates
	if kv.hasDuplicates(op) {
		return nil
	}

	kv.propose(op)
	kv.garbageCollect()

	// TODO: format reply

	return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.

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
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
