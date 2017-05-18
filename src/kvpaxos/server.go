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

	// for after the ordering has been decided
	OpNo   int
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

	kvstore    map[string]string
	seen 	   map[string]bool
	done       map[string]string
}

// returns true iff the two operations are of the same instance
// false otherwise
func (op Op) equals(other Op) bool {
	if op.Cid == other.Cid && op.SeqNo == other.SeqNo {
		return true
	}

	return false
}

func (op Op) getOpId() string {
	return strconv.Itoa(int(op.Cid)) + strconv.Itoa(int(op.SeqNo))
}

// returns a formatted op struct
func formatOp(cid int64, seqNo int, opType OpType, key string, val string) (op Op) {
	op = Op{}

	// instance identifiers
	op.Cid = cid
	op.SeqNo = seqNo

	// operation unique information
	op.Type = opType
	op.Key = key
	op.Value = val

	// for identifying the state of the operation
	op.OpNo = -1

	return
}

// returns true iff an operation has been recorded before, false otherwise
// does not lock, any operation that calls this must surround it in locks
func (kv *KVPaxos) hasDuplicates(op Op) bool {
	_, seen := kv.seen[op.getOpId()]
	_, done := kv.done[op.getOpId()]

	return seen || done
}

// proposes the given op
// also learns what ops have been chosen, and garbage collects
func (kv *KVPaxos) proposeOp(op Op) (Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	opNo := kv.knownIdx
	for {
		// attempt to propose the value
		kv.px.Start(opNo, op)

		// wait for this instance to make a decision
		to := 10 * time.Millisecond
		status, decision := kv.px.Status(opNo)
		for status != paxos.Decided {
			time.Sleep(to)
			if to < 10 * time.Second {
				to *= 2
			}
			status, decision = kv.px.Status(opNo)
		}

		// garbage collect
		kv.px.Done(opNo)

		// put the decision in its place
		curOp := decision.(Op)
		curOp.OpNo = opNo

		// only add op if it has not already been seen
		if !kv.hasDuplicates(op) {
			kv.ops[opNo] = curOp
		}
		kv.seen[curOp.getOpId()] = true // mark op seen

		// check to see if our value was chosen
		if op.equals(curOp) {
			kv.knownIdx = opNo + 1
			return curOp
		}

		// value was not chosen, mark seen and continue
		opNo++
	}
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	op := formatOp(args.Cid, args.SeqNo, GET, args.Key, "")
	reply.Err = OK

	kv.mu.Lock()
	if !kv.hasDuplicates(op) {
		kv.mu.Unlock()
		op = kv.proposeOp(op)
	} else {
		kv.mu.Unlock()
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// execute all known ops in order
	for ; kv.doneIdx <= op.OpNo; kv.doneIdx++ {
		// get the earliest op that has not been executed
		curOp := kv.ops[kv.doneIdx]

		// garbage collect from ops
		delete(kv.ops, kv.doneIdx)

		// format the op id
		curId := curOp.getOpId()

		// execute op
		switch curOp.Type {
		case GET:
			// get behavior
			// mark done, save response
			kv.done[curId] = kv.kvstore[curOp.Key]
 		case PUT:
			// put behavior
			kv.kvstore[curOp.Key] = curOp.Value
		case APPEND:
			// append behavior
			kv.kvstore[curOp.Key] = kv.kvstore[curOp.Key] + curOp.Value
		}
	}

	// return value at the time at which op was executed
	reply.Value = kv.done[op.getOpId()]

	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	op := Op{}
	switch args.Op {
	case "Put":
		op = formatOp(args.Cid, args.SeqNo, PUT, args.Key, args.Value)
	case "Append":
		op = formatOp(args.Cid, args.SeqNo, APPEND, args.Key, args.Value)
	}
	reply.Err = OK

	kv.mu.Lock()
	if !kv.hasDuplicates(op) {
		kv.mu.Unlock()
		op = kv.proposeOp(op)
	} else {
		kv.mu.Unlock()
	}

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
	kv.kvstore = make(map[string]string)
	kv.ops = make(map[int]Op)
	kv.seen = make(map[string]bool)
	kv.done = make(map[string]string)



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