package shardkv

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
	"time"

	"paxos"
	"shardmaster"
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
type Pair struct {
	Key string
	Value string
}
type Op struct {
	Type OpType
	OpId int64
	Args Args
}
const (
	GET ="Get"
  PUT = "Put"
  APPEND = "Append"
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
  ops map[int] Op
  seen map[int64] bool 
  seenIdx int
  doneIdx int
  current_config sharmaster.Config
  previous_config shardmaster.Config
  storage map[string]string
  muSeq map[string]*sync.Mutex
  shards []bool
  next_config_num int //-1 if we are not transitioning 
}
func (kv *ShardKV) check_duplicates(OpId int64) bool {
		return kv.seen[OpId]
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
  if kv.current_config.Num ==0 {
		return nil
	}
  if _,ok := kb.muSeq[args.Key] ; !ok {
   			kv.muSeq[args.Key] = &sync.Mutex{}
   }
  kv.muSeq[args.Key].Lock()
  defer kv.muSeq[args.Key].UnLock()
  op := { GET, op.OpId,*args)
  kv.proposeOp(op)
  kv.evaluate(op)
	return nil
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
// Your code here.
  if kv.current_config.Num ==0 {
		return nil
	}
  if check_duplicates(args.OpOd) {
     return nil
  }
  if _,ok := kb.muSeq[args.Key] ; !ok {
   			kv.muSeq[args.Key] = &sync.Mutex{}
   }
  kv.muSeq[args.Key].Lock()
  defer kv.muSeq[args.Key].UnLock()
  op := { PUT, op.OpId,*args)
  kv.proposeOp(op)
  kv.evaluate(op)
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
			//Initial case
     if kv.current_config.Num ==0 {
			   config := kv.sm.Query(-1)
         if config.Num == 1 {
		         kv.previous_config = kv.current_config
             kv.current_config = config
             kv.assign_shards()						
					}	
			}else {
        config := kv.sm.Query(-1)
        if config.Num > kv.current_config.Num{
				  //Reconfiguration	
          op := {RE_CONFIG,nrand(),DummyArgs{}}
          kv.proposeOp(op)
          kv.evaluate(op)
				}
			}

	}else {
     kv.send_shards()
     if kv.all_sent() && kv.all_received () {
				//end reconfiguration
		} 
	}
}
func (kv *ShardKV) send_shards(){
		for shard_index,_ := range kv.shards{
				if kv.shards[shard_index] ==false{
					continue
				}
				if kv.current_config.Shards[shard_index] != gid{
						//Current shard has been moved to another group.
						r_gid := kv.current_config.Shards[shard_index]
            kv.send_shard(r_gid,shard_index)	
				}
		}
}
func (kv *ShardKV) receive_shard() {
}
func (kv *ShardKV) send_shard(r_gid int64,shard_index int) {
     var storage_toSend []Pair
		 for key,value := range kv.storage{
			if key2shard(key) == shard_index{
			  	storage_toSend = append(storage_toSend,Pair{key,value}	
				}	
		 }
		 //TODO:: We need to send something for duplicates. , a state maybe.
		 for _,srv :=range self.current_config.Groups[r_gid] {
        //RPC call

			}
     
}
func (kv *ShardKV) all_sent(){
}
func (kv *ShardKV) all_received(){
}
func (kv *ShardKV) assign_shards(){
		for shard_index,gid := range kv.current_config.Shards{
				if gid = kv.gid {
					kv.shards[shard_index] = true
				}else{
					kv.shards[shard_index] =false
				}
			}
}
func nrand() int64 {
	return rand.Int63()
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
func (sm *ShardKV) propose(op Op) {
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
func (kv *ShardKV) evaluate() Result{
	var result Result 
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// apply all the ops in the log
	for ; kv.doneIdx < kv.seenIdx; kv.doneIdx++ {
		// get the earliest op that has not been executed
		op := kv.ops[kv.doneIdx]
		// garbage collect from ops
		delete(kv.ops, kv.doneIdx)

		// execute op
		switch op.Type {
		case "Get":
			var GetArgs= (op.Args).(GetArgs)
      result = kv.get(&GetArgs) 
		case "OutAppenArgs":
			var PutAppendArgs= op.Args.(PutAppendArgs)
      result = kv.put_append(&PutAppendArgs)
    case "Re_Config":
      kv.re_config()
		}
	}
	return result 
}
func (kv *ShardKV) re_config(){
		next_config := kv.sm.Query(kv.current_config.Num+1)
    kv.previous_config = kv.current_config
    kv.current_config = next_config
    kv.next_config_num = next_config.Num	
}


func (kv *ShardKV) currently_has_shard(key string) bool{
     shard_index := key2shard(key)
     return kv.shards[shard_index]
}
func (kv *ShardKV) has_shard(key string) bool{
     shard_index := key2shard(key)
     return kv.current_config.shards[shard_index] == kv.gid
}
//TODO:WE NEED TO CHECK DUPLICATES HERE WITH A CACHE
func(kv *ShardKV) get(args *GetArgs) Result {
   reply :=GetReply{}
   //Case 1: We have the shard  
   if kv.has_shard(args.Key) && kv.currently_has_shard(args.Key) {
      value,found := kv.storage[args.Key]
      if found {
 				reply.Value = value
        reply.Err =OK
      }else {
        reply.Value = ""
				reply.Err = ErrNoKey    
			}
	 }else if kv.has_shard(args.Key) && !kv.currently_has_shard(args.Key){
   //Case 2: we have the shard in the most current config but still didnt receive it.
        reply.Err = Waiting
		}else {
        reply.Err = ErrWrongServer
		} 
   return reply
}
func(kv *ShardKV) put_append(args *PutAppendArgs) Result {
   reply :=PutAppendReply{}
   //Case 1: We have the shard  
   if kv.has_shard(args.Key) && kv.currently_has_shard(args.Key) {
         if args.Op== "Put" {
						kv.storage[args.Key] = args.Value
            reply.Err= OK
					}else {
						kv.storage[args.Key] = kv.storage[args.Key] + args.Value
            reply.Err= OK
					}      
	 }else if kv.has_shard(args.Key) && !kv.currently_has_shard(args.Key){
   //Case 2: we have the shard in the most current config but still didnt receive it.
        reply.Err = Waiting
		}else {
        reply.Err = ErrWrongServer
		} 
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
  gob.Register(Pair{})
  gob.Register(PutAppendArgs{})
	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)
  kv.muSeq = make(map[string]*sync.Mutex)
	// Your initialization code here.
	// Don't call Join().

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


