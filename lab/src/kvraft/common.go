package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
	True     = "True"
	False    = "False"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkId    int64
	OpId int64
}

type PutAppendReply struct {
	WrongLeader string
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClerkId  int64
	OpId int64
}

type GetReply struct {
	WrongLeader string
	Err         Err
	Value       string
}
