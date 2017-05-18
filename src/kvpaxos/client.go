package kvpaxos

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"net/rpc"
)

type Clerk struct {
	servers []string
	// You will have to modify this struct.

	cid   int64
	seqNo int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []string) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.cid = nrand()
	ck.seqNo = 0

	return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
	// update sequence number
	ck.seqNo += 1

	// create argument struct
	gArgs := new(GetArgs)
	gArgs.Key = key

	gArgs.Cid = ck.cid
	gArgs.SeqNo = ck.seqNo

	// create reply struct
	gReply := new(GetReply)

	// return the value if one exists, otherwise return the empty string
	ck.sendToServer("Get", &gArgs, &gReply)
	for gReply.Err != OK {
		ck.sendToServer("Get", &gArgs, &gReply)
	}

	return gReply.Value
}

//
// shared by Put and Append.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// update sequence number
	ck.seqNo += 1

	// create argument struct
	paArgs := new(PutAppendArgs)

	paArgs.Key = key
	paArgs.Value = value
	paArgs.Op = op

	paArgs.SeqNo = ck.seqNo
	paArgs.Cid = ck.cid

	// create reply struct
	paReply := new(PutAppendReply)

	ck.sendToServer("PutAppend", &paArgs, &paReply)
	for paReply.Err != OK {
		ck.sendToServer("PutAppend", &paArgs, &paReply)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) sendToServer(op string, args interface{}, reply interface{}) {
	// find a server
	for i := 0; !call(ck.servers[i%len(ck.servers)], "KVPaxos."+op, args, reply); i += 1 {
	}
}
