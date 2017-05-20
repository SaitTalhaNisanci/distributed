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
	//"net/http/httptrace"
	"strconv"
	"time"
	//"go/format"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type OpType int

const (
	GET = iota
	PUT
	APPEND
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	// for  identifying the operation instance
	Cid    int64
	SeqNo  int

	// for executing the operation
	Type   OpType
	Key    string
	Value  string
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

			 // Your definitions here.
	doneIdx    int   // all operations <= done have been executed
	knownIdx   int   // all operations <= knownIdx have been discovered
	ops        map[int]Op
	muSeq      map[int]*sync.Mutex

	kvstore    map[string]string
	seen 	   map[string]bool
	cache      map[int64]string
}

// returns a unique operation id for each operation based on the client id
// and sequence number
func (op Op) getOpId() string {
	return strconv.Itoa(int(op.Cid)) + strconv.Itoa(int(op.SeqNo))
}

// returns true iff an operation has been recorded before, false otherwise
// does not lock, any operation that calls this must surround it in locks
func (kv *KVPaxos) hasDuplicates(op Op) bool {
	_, seen := kv.seen[op.getOpId()]

	return seen
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
func (kv *KVPaxos) proposeOp(op Op) {
	for {
		kv.mu.Lock()
		// check if op has already been chosen
		if kv.hasDuplicates(op) {
			kv.mu.Unlock()
			return
		}

		// propose the smallest value
		opNo := kv.knownIdx

		// create a lock for this sequence number if there isn't one
		if _, ok := kv.muSeq[opNo]; !ok {
			kv.muSeq[opNo] = &sync.Mutex{}
		}
		kv.mu.Unlock()

		// lock this sequence number
		kv.muSeq[opNo].Lock()

		// attempt to propose the value
		// wait for this instance to make a decision
		kv.px.Start(opNo, op)
		to := 10 * time.Millisecond
		status, decision := kv.px.Status(opNo)
		for status != paxos.Decided {
			time.Sleep(to)
			if to < 10 * time.Second {
				to *= 2
			}
			status, decision = kv.px.Status(opNo)
		}
		curOp := decision.(Op)

		// garbage collect
		kv.px.Done(opNo)

		// unlock this sequence number
		kv.muSeq[opNo].Unlock()

		// only add op if it has not already been seen
		kv.mu.Lock()
		if !kv.hasDuplicates(curOp) {
			kv.ops[opNo] = curOp
			kv.seen[curOp.getOpId()] = true // mark op seen
			kv.knownIdx = max(kv.knownIdx, opNo + 1)
		}
		kv.mu.Unlock()
	}
}

// Get RPC handler
func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// format op struct
	op := Op{args.Cid, args.SeqNo, GET, args.Key, ""}
	reply.Err = OK

	kv.proposeOp(op)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// apply all the ops in the log
	for ; kv.doneIdx < kv.knownIdx; kv.doneIdx++ {
		// get the earliest op that has not been executed
		curOp := kv.ops[kv.doneIdx]

		// garbage collect from ops
		delete(kv.ops, kv.doneIdx)

		// execute op
		switch curOp.Type {
		case GET:
			// get behavior
			// mark done, save response
			kv.cache[curOp.Cid] = kv.kvstore[curOp.Key]
 		case PUT:
			// put behavior
			kv.kvstore[curOp.Key] = curOp.Value
		case APPEND:
			// append behavior
			kv.kvstore[curOp.Key] = kv.kvstore[curOp.Key] + curOp.Value
		}
	}

	// return value at the time at which op was executed
	reply.Value = kv.cache[op.Cid]

	return nil
}

// PutAppend RPC handler
func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// format op struct
	op := Op{}
	switch args.Op {
	case "Put":
		op = Op{args.Cid, args.SeqNo, PUT, args.Key, args.Value}
	case "Append":
		op = Op{args.Cid, args.SeqNo, APPEND, args.Key, args.Value}
	}
	reply.Err = OK

	kv.proposeOp(op)

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
	kv.doneIdx = 0
	kv.knownIdx = 0
	kv.ops = make(map[int]Op)
	kv.muSeq = make(map[int]*sync.Mutex)
	kv.kvstore = make(map[string]string)
	kv.seen = make(map[string]bool)
	kv.cache = make(map[int64]string)

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