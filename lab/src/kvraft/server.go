package raftkv

import (
	"fmt"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const (
	Debug = 0
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Name string
	Key  string
	Value string
	// unique handle
	ClientId int64
	OpId int64
	//Index int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data map[string]string //store key value
	lastApply map[int64]int64 //clientid map to opid
	idxMsg map[int]chan raft.ApplyMsg //for every index create one channel to store msg

}

func (kv *KVServer) BackgroundProcess(){
	//对这个operation处理，看是否重复或者执行操作,
	for msg := range kv.applyCh{
		fmt.Println(msg)
		kv.mu.Lock()
		op := msg.Command.(Op)
		//lastOpid := kv.lastApply[op.ClientId]
		if lastOpid, ok := kv.lastApply[op.ClientId]; !(ok && lastOpid == op.OpId){
			if op.Name == "Put"{
				kv.data[op.Key] = op.Value
				//msg.CommandIndex//当前位置
			}else if op.Name == "Append"{
				kv.data[op.Key] += op.Value
			}
			kv.lastApply[op.ClientId] = op.OpId
			//kv.idxMsg[msg.CommandIndex] <- msg
		}else{
			fmt.Println("duplicate log ")
		}
		if v, ok := kv.idxMsg[msg.CommandIndex]; ok{
			v <- msg
		}
		kv.mu.Unlock()
	}
}

//func (kv *KVServer) testChan(){
//	for{
//		select {
//		case <-kv.applyCh:
//			fmt.Println("hahaIgot")
//		}
//	}
//}



func (kv *KVServer) WaitLog(idx int, op Op) bool{
	kv.mu.Lock()
	waitChan := make(chan raft.ApplyMsg, 1)
	kv.idxMsg[idx] = waitChan
	kv.mu.Unlock()
	for{
		select {
		case msg := <- waitChan:
			kv.mu.Lock()
			delete(kv.idxMsg, idx)
			kv.mu.Unlock()
			return msg.CommandIndex == idx && msg.Command == op
		case <- time.After(10*time.Millisecond):
			kv.mu.Lock()
			_, isLeader := kv.rf.GetState()
			if !isLeader{
				delete(kv.idxMsg, idx)
				kv.mu.Unlock()
				return false
			}
			kv.mu.Unlock()


		}
	}
}

//await 对每个index都加入一个channel，如果channel传过来消息代表成功，超时看成失败。对每个server都一直运行一个apply协程，记录每个client的最后一个request id，获取commit——log如果相同代表重复，直接return，不相同表示新的
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	command := Op{"Get", args.Key, "", args.ClerkId, args.OpId}
	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(command)
	kv.mu.Unlock()
	if !isLeader{
		reply.WrongLeader = True
		return
	}
	success := kv.WaitLog(index, command)
	if success{
		v, ok := kv.data[args.Key]
		if ok{
			reply.Err = OK
			reply.Value = v
		}else{
			reply.Err = ErrNoKey
			reply.Value = ""
		}

	}else{
		reply.WrongLeader = True
	}

	//for v := range kv.applyCh{
	//	// 判断handle和自己的比较，相等就代表此命令已经成功commit了，但是如果不是还要塞回去，该怎么做？
	//	c, ok := v.Command.(Op)
	//}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	command := Op{args.Op,args.Key, args.Value, args.ClerkId, args.OpId}
	kv.mu.Lock()
	idx, _, isLeader := kv.rf.Start(command)
	//fmt.Println(kv.rf.currentState)
	kv.mu.Unlock()
	if !isLeader{
		reply.WrongLeader = True
		//fmt.Println("instart")
		return
	}
	success := kv.WaitLog(idx, command)
	if success{
		reply.WrongLeader = False
		reply.Err = OK
	}else{
		//fmt.Println("inprocss")
		reply.WrongLeader = True
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	//data map[string]string //store key value
	//lastApply map[int64]int64 //clientid map to opid
	//idxMsg map[int]chan raft.ApplyMsg //for every index create one channel to store msg
	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.lastApply = make(map[int64]int64)
	kv.idxMsg = make(map[int]chan raft.ApplyMsg)
	go kv.BackgroundProcess()
	//go kv.testChan()


	return kv
}
