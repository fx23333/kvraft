package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrCompleted   = "ErrCompleted"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key      string
	Value    string
	Op       string // "Put" or "Append"
	ClientId int64
	Sequence int
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err       Err
	NewLeader int
}

type GetArgs struct {
	Key      string
	ClientId int64
	Sequence int
	// You'll have to add definitions here.
}

type GetReply struct {
	Err       Err
	Value     string
	NewLeader int
}
