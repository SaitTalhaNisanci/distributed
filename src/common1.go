package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
	Waiting       = "Waiting"
)

type Err string
type Pair struct {
	Key   string
	Value string
}
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpId int64
}

type PutAppendReply struct {
	Err Err
}

type DummyArgs struct {
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	OpId int64
}

type GetReply struct {
	Err   Err
	Value string
}

type SendShardArgs struct {
	Storage     []Pair
	Applied     []int64
	Shard_index int
	Config_num  int
  OpId int64
}

type SendShardReply struct {
	Err Err
}

type DeleteShardArgs struct {
	Shard_index int
	Config_num  int
}
