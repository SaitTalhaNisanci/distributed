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
	PUTAPPEND
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
	doneIdx       int   // all operations <= done have been executed
	ops        map[int]*Op

	kvstore    map[string]string
	seen 	   map[string]bool
	done       map[string]string
}

// returns true iff the two operations are of the same instance
// false otherwise
func (op *Op) equals(other *Op) bool {
	if op.Cid == other.Cid && op.SeqNo == other.SeqNo {
		return true
	}

	return false
}

// returns the unique op id for a given op
func (op *Op) getOpId() string {
	return strconv.Itoa(int(op.Cid)) + strconv.Itoa(op.SeqNo)
}

// returns a formatted op struct
func formatOp(cid int64, seqNo int, opType OpType, key string, val string) (op *Op) {
	op = new(Op)

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

// returns true iff this instance of kvpaxos has already seen or proposed this operation, false otherwise
//
// ensuring that this instance alone is not sufficient for ensuring that duplicate operations are not executed
//
// has the following side effect:
// if the operation has not been seen or proposed, it is marked seen
func (kv *KVPaxos) hasDuplicates(op *Op) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// create a string representation of the instance
	opId := strconv.Itoa(int(op.Cid)) + strconv.Itoa(op.SeqNo)

	if val, ok := kv.seen[opId]; ok {
		if val {
			// this operation has been already seen
			return true
		}
	}

	// this operation has not been seen; mark it seen
	kv.seen[opId] = true
	return false
}

// proposes the given op
// also learns what ops have been chosen, and garbage collects
func (kv *KVPaxos) proposeOp(op *Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	opNo := kv.doneIdx
	for {
		// attempt to propose the value
		kv.px.Start(opNo, op)

		// wait for this instance to make a decision
		status, decision := kv.px.Status(opNo)
		for status != paxos.Decided {
			time.Sleep(time.Second)
			status, decision = kv.px.Status(opNo)
		}

		// garbage collect
		kv.px.Done(opNo)

		// put the decision in its place
		curOp := &Op{}
		switch decision.(type) {
		case Op:
			curOp = formatOp(decision.(Op).Cid,
				decision.(Op).SeqNo,
				decision.(Op).Type,
				decision.(Op).Key,
				decision.(Op).Value)
		case *Op:
			curOp = decision.(*Op)
		}

		kv.ops[opNo] = curOp
		kv.ops[opNo].SeqNo = opNo

		// check to see if our value was chosen
		if op.equals(curOp) {
			return
		}

		// value was not chosen, mark seen and continue
		kv.seen[curOp.getOpId()] = true
		opNo++
	}
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	op := formatOp(args.Cid, args.SeqNo, GET, args.Key, "")
	reply.Err = OK

	fmt.Println("DOES THIS WORK")

	if !kv.hasDuplicates(op) {
		kv.proposeOp(op)
	}

	fmt.Println("HOW ABOUT THIS")

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// execute all known ops in order
	for ; kv.doneIdx <= op.OpNo; kv.doneIdx++ {
		// get the earliest op that has not been executed
		fmt.Println("AND THIS? DOES THIS FUCKING WORK")
		curOp := kv.ops[kv.doneIdx]

		// format the op id
		curId := curOp.getOpId()

		// a duplicate has been detected, skip it
		if _, ok := kv.done[curId]; ok {
			continue
		}

		// execute op
		fmt.Println("doneIdx: ", kv.doneIdx)
		switch curOp.Type {
		case GET:
			// get behavior
			kv.ops[kv.doneIdx].Value = kv.kvstore[curOp.Key]

			// mark done, save response
			kv.done[curId] = kv.kvstore[curOp.Key]

			fmt.Println("Get: ", curOp.Key, "\t-> ", kv.kvstore[curOp.Key])
 		case PUTAPPEND:
			// putappend behavior
			kv.kvstore[curOp.Key] = kv.kvstore[curOp.Key] + curOp.Value

			// mark done, save response (no response necessary for putappend)
			kv.done[curId] = ""

			fmt.Println("PutAppend(", curOp.Key, ", ", curOp.Value, ")\tnewVal: ", kv.kvstore[curOp.Key])
		}

		// garbage collect from ops
		delete(kv.ops, kv.doneIdx)
	}

	// return value at the time at which op was executed
	reply.Value = kv.done[op.getOpId()]

	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	op := formatOp(args.Cid, args.SeqNo, PUTAPPEND, args.Key, args.Value)
	reply.Err = OK

	if !kv.hasDuplicates(op) {
		kv.proposeOp(op)
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
	kv.kvstore = make(map[string]string)
	kv.ops = make(map[int]*Op)
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