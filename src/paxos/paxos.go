package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import (
	"fmt"
	"log"
	"math/rand"
	"time"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
)

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// Your data here.
  state map[int] *InstanceInfo
  done map[string] int 
  peer_amount int
}
type InstanceInfo struct{
  decided bool
  highest_promised int
  current_proposal int
  accepted_value interface{}
	accepted_num int
	decided_value interface{}	

}
type PrepareReply struct {
	Highest_promised int
	Highest_accepted int
  Highest_done int
	OK bool
	Value interface{}
}
type PrepareArgs struct {
	Seq int
	Current_proposed int
}
type AcceptArgs struct {
	Seq int
  Current_proposed int 
  Value interface{}
}
type AcceptReply struct{
  Highest_done int
  OK bool
}
type DecidedReply struct {
}
type DecidedArgs struct {
	Seq int 
	Value interface{}
}
//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
  px.mu.Lock()
  defer px.mu.Unlock()
  //Ignore if we already forgot the seq.
  if seq <  px.Min_without_lock() {
		return
	}
  px.first_time_init(seq) // Initialize if this is the first time we see this seq
 	go px.proposer(seq,v)
}
func ( px *Paxos) initialize() *InstanceInfo{
  return &InstanceInfo{ 
			highest_promised : -1,
			decided : false,
 		  current_proposal : px.me, // Since px.me is a unique number we can use it.
      accepted_num : -1	,
	}
} 
func (px *Paxos) choose_propose_num(seq int,highest_promised int) int {
   px.mu.Lock()
   defer px.mu.Unlock()

   candidate_propose_num := px.state[seq].current_proposal
   //Increase it until it is greater than the highest proposed number for this seq
   for candidate_propose_num <= highest_promised {
       candidate_propose_num += px.peer_amount
   }
   px.state[seq].current_proposal  =candidate_propose_num
	 return candidate_propose_num   
}
func (px *Paxos) local_accept(seq int , propose_num int ,value interface{} ) (bool){
  px.mu.Lock()
  defer px.mu.Unlock()
	status := false
  px.first_time_init(seq) //initialize if this is the first time we see this eeq
	if propose_num >= px.state[seq].highest_promised{
			px.state[seq].highest_promised = propose_num //update the highest promised number
			px.state[seq].accepted_value = value // update the accepted value 	
			px.state[seq].accepted_num= propose_num //update the accepted proposal number
			status= true
	}
  return status
}

func (px *Paxos) local_prepare(seq int , propose_num int ) (bool,int,int,interface {}){
  px.mu.Lock()
  defer px.mu.Unlock()
	status :=false
  px.first_time_init(seq)
	if propose_num > px.state[seq].highest_promised{
		px.state[seq].highest_promised =propose_num
	  status = true 	
	}
  return status,px.state[seq].highest_promised,px.state[seq].accepted_num,px.state[seq].accepted_value
	
}
func (px *Paxos) local_decided(seq int , value interface{} ) {
  px.mu.Lock()
  defer px.mu.Unlock()
  px.first_time_init(seq)
	if  !px.state[seq].decided {
    //fmt.Println(seq,value)
		px.state[seq].decided = true
		px.state[seq].decided_value= value
    
	}
}

func (px *Paxos) broadcast_prepare(seq int, propose_num int) (int,bool,interface{} ,int) {
   highest_accepted := -1
	 highest_promised := -1
	 var value interface{}
   total_ok := 0
   for _,peer := range px.peers {
     		if peer == px.peers[px.me] {
            ok,cur_promised,cur_accepted,val :=px.local_prepare(seq,propose_num) // call local prepare instead of rpc
            //Update the highest accepted proposal
						if cur_accepted > highest_accepted{
								value = val
								highest_accepted= cur_accepted
						}
						highest_promised = max (highest_promised,cur_promised) //In order to choose a higher number than the highest seen
            //In order to check the majority
            if ok {
							total_ok++
						}
        }else {
            args := PrepareArgs {Seq: seq, Current_proposed: propose_num, }
            reply := PrepareReply{}
						ok:= call(peer,"Paxos.Handle_prepare",&args,&reply) //RPC prepare call
            if ok {
              //Update the highest accepted proposal
							if reply.Highest_accepted > highest_accepted{
									highest_accepted= reply.Highest_accepted
									value = reply.Value
							}
							highest_promised = max ( highest_promised, reply.Highest_promised)//In order to choose a higher number than the highest seen
              //In order to check the majority
  						if reply.OK{
								total_ok++
							}
						}
				}
   }
	 return highest_promised,px.check_majority(total_ok), value, highest_accepted
   
}
func (px *Paxos) broadcast_decided(seq int,  v interface{})  {
   for _,peer := range px.peers {
     		if peer == px.peers[px.me] {
            px.local_decided(seq,v) // send a local decided instead of rpc
        }else {
            args := DecidedArgs {Seq: seq, Value: v }
            reply := DecidedReply{}
						call(peer,"Paxos.Handle_decided",&args,&reply) 
				}
   }
}
// Iterates over the states and removes the ones that are less than px.Min()
func (px *Paxos) free_state(num int) {
		for seq,_:= range  px.state{
		  if seq < num {
				delete( px.state, seq )
			}
		}	

}
//Update done entry for the given peer
func (px *Paxos) update(peer string,highest_done int) {
		px.mu.Lock()
    defer px.mu.Unlock()
    if highest_done >  px.done[peer] {
			px.done[peer] =  highest_done
      px.free_state(px.Min_without_lock())// Since we might have updated done entry for this peer it is possible that we need to free some states.
    }
}
func (px *Paxos) broadcast_accept(seq int, propose_num int, v interface{}) (bool) {
   total_ok := 0
   for _,peer := range px.peers {
        // send a local accept instead of rpc 
     		if peer == px.peers[px.me] {
            ok :=px.local_accept(seq,propose_num,v)
            if ok{
							total_ok++
						}
        }else {
            args := AcceptArgs{Seq: seq, Current_proposed: propose_num,Value : v }
            reply := AcceptReply{}
						ok:= call(peer,"Paxos.Handle_accept",&args,&reply) //call handle accept 
            if ok  {
              px.update(peer,reply.Highest_done)
  						if reply.OK{
								total_ok++
							}
						}
				}
   }
	 return px.check_majority(total_ok)
   
}

//Given total ok check if it is majority 
func (px *Paxos) check_majority(total_ok int) bool{
		return total_ok > (px.peer_amount)/2
}
func max(x int,y int) int {
		if x >y {
			return x
		}
		return y
}
func (px *Paxos) Handle_accept(args *AcceptArgs, reply * AcceptReply) error {
     px.mu.Lock()
		 defer px.mu.Unlock()
     px.first_time_init(args.Seq) // if this seq number is seen for the first time create an entry for it.
		 if args.Current_proposed >= px.state[args.Seq].highest_promised {
				reply.OK= true
        px.state[args.Seq].highest_promised = args.Current_proposed 
				px.state[args.Seq].accepted_num = args.Current_proposed // update the accepted num
        px.state[args.Seq].accepted_value = args.Value //update the accepted value.
		 }else {
				reply.OK =false
		 }
     reply.Highest_done= px.done[px.peers[px.me]] 
     return nil

}
func (px *Paxos) Handle_decided(args *DecidedArgs, reply * DecidedReply) error {
     px.mu.Lock()
		 defer px.mu.Unlock()
     px.first_time_init(args.Seq) //if this seq number is seen for the first time create an entry for it.
     //If it already decided a value then dont do anything.
     if !px.state[args.Seq].decided{ 
    		//fmt.Println(args.Seq,args.Value)
				px.state[args.Seq].decided=true
				px.state[args.Seq].decided_value= args.Value
			}
     return nil

}
//If this is the first time this peer sees this seq then initialize it
func (px *Paxos) first_time_init(seq int){
		_, found := px.state[seq]
    if !found{
			px.state[seq] = px.initialize()
		}		
}
func (px *Paxos) Handle_prepare(args *PrepareArgs, reply * PrepareReply) error {
     px.mu.Lock()
		 defer px.mu.Unlock()
     px.first_time_init(args.Seq) //if this seq number is seen for the first time create an entry for it.
		 if args.Current_proposed > px.state[args.Seq].highest_promised{
					reply.OK =true	
					px.state[args.Seq].highest_promised = args.Current_proposed // Update the highest seen value
		 }else {
					reply.OK =false
			}
     reply.Highest_promised = px.state[args.Seq].highest_promised // to update the highest seen number of the caller 
		 reply.Value = px.state[args.Seq].accepted_value  //To broadcast accept we need to find the value of the highest accepted num
		 reply.Highest_accepted = px.state[args.Seq].accepted_num // To find the highest accepted value among the peers.
     reply.Highest_done = px.done[px.peers[px.me]] // get the done value for this peer so that we can update the done map of the caller 
     return nil

}
func (px *Paxos) proposer(seq int,v interface{}) {
   highest_num := -1 // highest number seen so far
   //keep proposing until we decide on a single value, break if the peer is dead. 
   for px.is_deciding(seq) && !px.isdead(){
       proposal_num:= px.choose_propose_num(seq,highest_num) // Choose a number to propose that is greater than highest number seen so far.
       highest_promised, ok,val,highest_accepted := px.broadcast_prepare(seq,proposal_num) // broadcast prepare to the other nodes 
       highest_num = highest_promised // update highest num
			 value := v // Initialize the value to be accepted as this peers value.
       //If highest accepted number from some other peer is found then we need to broadcast accept its value.
       if highest_accepted != -1 {
					value = val //Update value to be broadcasted to be accepted.
			 }
       //If we got ok from majority of peers for prepare broadcast accept.
     	 if ok {
					ok =px.broadcast_accept(seq,proposal_num,value)	//Broadcast accept
          // If we got ok from majority of peers for accept then we can send decide.
          if ok {
            //If some other value has been decided then break.
       			if !px.is_deciding(seq) {
							break
			 			}
             px.broadcast_decided(seq,value) //broadcast the value.
						 break
					}else {
					  time.Sleep(time.Duration(rand.Intn(200)))
					}
			 }else{
					time.Sleep(time.Duration(rand.Intn(200)))
			 }
        


		}
}
func (px *Paxos) is_deciding(seq int ) bool{
    px.mu.Lock()
		defer px.mu.Unlock()
    // Check if we forgot this seq number
    if seq < px.Min_without_lock() {
			return false
		}
    //Assumtion: if we are here then this seq number must exist in our state
    _,found := px.state[seq]
    if !found {
			return false
    }
    return !px.state[seq].decided 

}
//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
  px.mu.Lock()
  defer px.mu.Unlock()
  px.done[px.peers[px.me]] = max(seq,px.done[px.peers[px.me]]) // update this peers highest done with the passed arg.
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
  px.mu.Lock()
  defer px.mu.Unlock()
  max_instance := -1 
  /// Iterate over all seqs known to this peer to find the maximum one.
  for seq,_:= range px.state{
		max_instance = max(max_instance,seq)

	}
	return max_instance
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.
  px.mu.Lock()
  defer px.mu.Unlock()
	min_value := px.done[px.peers[px.me]] 
  //Iterate over all done values for peers to find the minimum one
  for _,cur := range px.done{
      min_value = min(cur, min_value)
	}
  return min_value +1
}
func (px *Paxos) Min_without_lock() int {
	// You code here.
	min_value := px.done[px.peers[px.me]] 
  //Iterate over all done values for peers to find the minimum one
  for _,cur := range px.done{
      min_value = min(cur, min_value)
	}
  return min_value +1
}

func min(x int, y int) int {
	if x > y {
		return y
	}
	return x
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.
  px.mu.Lock()
  defer px.mu.Unlock()
  //We forgot the value 
  if seq < px.Min_without_lock() {
			return Forgotten,nil
	}
  _,found := px.state[seq]
  if found && px.state[seq].decided {
			return Decided,px.state[seq].decided_value
	}
	return Pending, nil
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.

  px.state = map[int] *InstanceInfo{}
  px.done = map[string] int{}
  // Initialize all of peer's nodes as -1 since when we iterate they need to be in the map even though we haven't received anything from them.
  for _,peer := range px.peers {
			px.done[peer] = -1
	}
  px.peer_amount = len(peers)
	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
