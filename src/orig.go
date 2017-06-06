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
type Op struct {
	Type OpType
	OpId int64
	Args Args
}
const (
	GET ="Get"
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
  ops map[int] Op
  seen map[int64] bool 
  applied map[int64] bool
  seenIdx int
  doneIdx int
  current_config shardmaster.Config
  previous_config shardmaster.Config
  storage map[string]string
  muSeq map[string]*sync.Mutex
  cache map[int64] Err
  shards []bool
  next_config_num int //-1 if we are not transitioning 
}
func (kv *ShardKV) check_duplicates(OpId int64) bool {
		return kv.applied[OpId]
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
  kv.mu.Lock()
  defer kv.mu.Unlock()
  if !kv.is_initialized(){
		return nil
	}
  if _,ok := kv.muSeq[args.Key] ; !ok {
   			kv.muSeq[args.Key] = &sync.Mutex{}
   }
  kv.muSeq[args.Key].Lock()
  defer kv.muSeq[args.Key].Unlock()
	//OP number is already in args, need to change this.
  op := Op{ GET, nrand(),*args}
  kv.propose(op)
  result :=kv.evaluate()
  reply.Err= result.(GetReply).Err
  fmt.Println(reply.Err)
  reply.Value = result.(GetReply).Value
	return nil
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
// Your code here.
  kv.mu.Lock()
  defer kv.mu.Unlock()
  if !kv.is_initialized(){
		return nil
	}
  /*
  if kv.check_duplicates(args.OpId) {
     fmt.Println("for key : ", args.Key)
     reply.Err = OK
     return nil
  }
*/
  if _,ok := kv.muSeq[args.Key] ; !ok {
   			kv.muSeq[args.Key] = &sync.Mutex{}
   }
  kv.muSeq[args.Key].Lock()
  defer kv.muSeq[args.Key].Unlock()
	//This op number is already included in args, 
  op := Op{ PUTAPPEND, nrand(),*args}
  kv.propose(op)
  result :=kv.evaluate()
  reply.Err = result.(PutAppendReply).Err
  fmt.Println("PUT REPLY : " , reply.Err)
	return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
  kv.mu.Lock()
  defer kv.mu.Unlock()
  kv.evaluate()
  fmt.Println("TICK: ",kv.me," ",kv.next_config_num,"   --")
  if kv.next_config_num == -1 {
			//Initial case
     if kv.current_config.Num ==0 {
			   config := kv.sm.Query(1)
         if config.Num == 1 {
		         kv.previous_config = kv.current_config
             kv.current_config = config
             kv.assign_shards()						
					}	
			}else {
        config := kv.sm.Query(-1)
        if config.Num > kv.current_config.Num{
				  //Reconfiguration	
          fmt.Println("config num: " ,config.Num)
          op := Op{RE_CONFIG,nrand(),DummyArgs{}}
          kv.propose(op)
          kv.evaluate()
				}
			}

	}else {
     kv.send_shards()
     sent := kv.all_sent()
     received :=kv.all_received()
     fmt.Println("sent received ",kv.me , " " ,sent," " ,received)
     if sent&& received {
				//end reconfiguration
        op := Op{"Re_Config_Done",nrand(),DummyArgs{}}
        kv.propose(op)
        kv.evaluate()
		} 
	}
}
func (kv *ShardKV) send_shards(){
		for shard_index,_ := range kv.shards{
				if kv.shards[shard_index] ==false{
					continue
				}
				if kv.current_config.Shards[shard_index] != kv.gid{
						//Current shard has been moved to another group.
						r_gid := kv.current_config.Shards[shard_index]
            kv.send_shard(r_gid,shard_index)	
				}
		}
}
func (kv *ShardKV) is_behind(config_num int) bool{
		return kv.current_config.Num <config_num
}
func (kv *ShardKV) receive_shard(args *SendShardArgs) Result {
    reply := SendShardReply{}
    if kv.check_duplicates(args.OpId) {
				reply.Err = OK
				return reply 	
		}
    if kv.is_uptodate(args.Config_num) {
			for _,pair := range args.Storage {
				kv.storage[pair.Key]= pair.Value
			}	
      for _,id := range args.Applied {
				kv.applied[id] = true
			}
      kv.shards[args.Shard_index] = true
      kv.applied[args.OpId]= true
      reply.Err = OK 
		}else if kv.is_behind(args.Config_num) {
			reply.Err = Waiting
    }else {
      kv.applied[args.OpId]= true
      reply.Err = OK
		}
		return reply	
    	
}
func (kv *ShardKV) send_shard(r_gid int64,shard_index int) {
     fmt.Println("SENDINGGGGG")
     var storage_toSend []Pair
     var applied []int64
		 for key,value := range kv.storage{
			if key2shard(key) == shard_index{
			  	storage_toSend = append(storage_toSend,Pair{key,value}	)
				}	
		 }
     for id,_ :=range kv.applied{
			  applied = append(applied,id)	
			}
		 //TODO:: We need to send something for duplicates. , a state maybe.
     opId := nrand()
		 for _,srv :=range kv.current_config.Groups[r_gid] {
        //RPC call
        args := &SendShardArgs{}
        args.Storage = storage_toSend
        args.Config_num = kv.next_config_num
        args.Shard_index = shard_index       
        args.Applied = applied
        args.OpId = opId
        var reply SendShardReply
        ok := call(srv,"ShardKV.Receive_shard",args,&reply)
        fmt.Println("receive shard reply " ,reply.Err)
        if ok && reply.Err == OK {
						//deleteshard
            deleteArgs := DeleteShardArgs{shard_index,kv.next_config_num}
            op := Op{"DeleteShard",nrand(),deleteArgs}
            kv.propose(op)
            kv.evaluate()
            return  
				}
        if reply.Err == Waiting{
					opId=nrand()
				}
			}
     
}

func (kv *ShardKV) all_sent() bool{
  for shard_index,_:= range kv.shards{
			if kv.shards[shard_index] == true&& kv.current_config.Shards[shard_index] != kv.gid {
				return false
			}
	}  
	return true
}
func (kv *ShardKV) all_received() bool {
	for shard_index,_:= range kv.shards{
			if kv.shards[shard_index] == false&& kv.current_config.Shards[shard_index] == kv.gid {
				return false
			}
	} 
   return true
}
func (kv *ShardKV) is_initialized() bool{
	return kv.current_config.Num >0
}
func (kv *ShardKV) is_uptodate(config_num int ) bool{
  return kv.current_config.Num == config_num
}
func (kv *ShardKV) Receive_shard(args *SendShardArgs,reply *SendShardReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()
  if !kv.is_initialized() {
		return nil
	}  
  _,found := kv.cache[args.OpId]
  if found {
    reply.Err =kv.cache[args.OpId] 
		//return nil 
	}
  op := Op{"ReceiveShard",nrand(),*args}
  kv.propose(op)
  internalReply := kv.evaluate()
  reply.Err = internalReply.(SendShardReply).Err
 	kv.cache[args.OpId] = reply.Err
	return nil 
}
func (kv *ShardKV) assign_shards(){
		for shard_index,gid := range kv.current_config.Shards{
				if gid == kv.gid {
					kv.shards[shard_index] = true
				}else{
					kv.shards[shard_index] =false
				}
			}
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
func (kv *ShardKV) propose(op Op) {
	for {
		// propose the smallest value
    //kv.mu.Lock()
		if kv.seen[op.OpId] {
      //kv.mu.Unlock()
			return
		}
		opNo := kv.seenIdx
    //kv.mu.Unlock()

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
    //kv.mu.Lock()
		// only add op if it has not already been seen
		kv.ops[opNo] = curOp
		kv.seen[curOp.OpId] = true // mark op seen
		kv.seenIdx = max(kv.seenIdx, opNo+1)
    //kv.mu.Unlock()
	}
}
func (kv *ShardKV) evaluate() Result{
	var result Result 
	//kv.mu.Lock()
	//defer kv.mu.Unlock()

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
		case "PutAppend":
			var PutAppendArgs= op.Args.(PutAppendArgs)
      result = kv.put_append(&PutAppendArgs)
    case "Re_Config":
      kv.re_config()
    case "ReceiveShard":
			var SendShardArgs = op.Args.(SendShardArgs)
      result = kv.receive_shard(&SendShardArgs)
    case "Re_Config_Done":
      kv.re_config_done()
    case "DeleteShard":
      var DeleteShardArgs = op.Args.(DeleteShardArgs)
      kv.delete_shard(&DeleteShardArgs)
    case "NOP":
      
		}
	}
	return result 
}
func (kv *ShardKV) delete_shard(args *DeleteShardArgs) {
		if kv.is_uptodate(args.Config_num) {
	     for key,_ :=range kv.storage{
						if key2shard(key) == args.Shard_index {
							 delete(kv.storage,key)
						}
				}	

		}
    kv.shards[args.Shard_index] =false
}
func (kv *ShardKV) re_config_done(){
   kv.next_config_num = -1 
}
func (kv *ShardKV) re_config(){
    if kv.is_uptodate(kv.next_config_num){
			return 
		}
    fmt.Println("kv currect config num ",kv.current_config.Num)
		next_config := kv.sm.Query(kv.current_config.Num+1)
    if next_config.Num !=kv.current_config.Num+1{
			return 
		}
    kv.assign_shards()
    kv.previous_config = kv.current_config
    kv.current_config = next_config
    fmt.Println("next config number " , next_config.Num)
    kv.next_config_num = next_config.Num	
    
}

//We have the shard 
func (kv *ShardKV) currently_has_shard(key string) bool{
     shard_index := key2shard(key)
     return kv.shards[shard_index]
}
//The shard will be transfered to us or it is already transfered.
func (kv *ShardKV) has_shard(key string) bool{
     shard_index := key2shard(key)
     return kv.current_config.Shards[shard_index] == kv.gid
}
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
        reply.Err = ErrWrongGroup
		} 
   return reply
}
func(kv *ShardKV) put_append(args *PutAppendArgs) Result {
   reply :=PutAppendReply{}
   if kv.check_duplicates(args.OpId){
      fmt.Println("for key : ", args.Key)
			reply.Err =OK
			return reply
		}
   //Case 1: We have the shard  
   if kv.has_shard(args.Key) && kv.currently_has_shard(args.Key) {
         kv.applied[args.OpId] = true
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
        reply.Err = ErrWrongGroup
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
  gob.Register(GetReply{})
  gob.Register(PutAppendReply{})
  gob.Register(DummyArgs{})
  gob.Register(DeleteShardArgs{}) 
  gob.Register(Pair{})
  gob.Register(PutAppendArgs{})
  gob.Register(SendShardArgs{})
	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)
  kv.muSeq = make(map[string]*sync.Mutex)
	// Your initialization code here.
	// Don't call Join().
  kv.current_config =shardmaster.Config{}
	kv.previous_config = shardmaster.Config{}
  kv.storage =map[string]string{}
  kv.next_config_num= -1
  kv.shards =make ([]bool,shardmaster.NShards)
  kv.current_config.Groups = map[int64][]string{}
  kv.previous_config.Groups = map[int64][]string{}
  kv.ops= map[int] Op{}
  kv.seen = map[int64]bool{}
  kv.applied = map[int64] bool{}
  kv.cache = map[int64] Err{}
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


