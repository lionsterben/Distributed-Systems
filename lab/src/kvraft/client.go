package raftkv

import (
	"fmt"
	"labrpc"
)
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader int //current leader index, need check
	handle int64 //send rpc index, for when resending, kvserver check
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	//ck.leader = -1
	ck.handle = nrand()
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	args := &GetArgs{key, ck.handle, nrand()}
	reply := &GetReply{}
	//ok := ck.servers[ck.leader].Call("KVServer.Get", args, reply)
	//if ok && reply.Err=="" && !reply.WrongLeader{
	//	return reply.Value
	//}
	//fmt.Println("get")
	for i:=ck.leader; ; i++{
		ok := ck.servers[i%len(ck.servers)].Call("KVServer.Get", args, reply)
		if ok && reply.WrongLeader==False{
			ck.leader = i%len(ck.servers)
			if reply.Err == ErrNoKey {
				return ""
			}else if reply.Err == OK{
				return reply.Value
			}
		}

	}
	// You will have to modify this function.
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	fmt.Println("putappend")
	args := PutAppendArgs{key, value, op, ck.handle, nrand()}
	reply := PutAppendReply{}
	for i:=ck.leader; ; i++{
		//fmt.Println(i%len(ck.servers))
		ok := ck.servers[i%len(ck.servers)].Call("KVServer.PutAppend", &args, &reply)
		//fmt.Println(reply.WrongLeader)
		if ok && reply.WrongLeader == False && reply.Err==OK{
			ck.leader = i%len(ck.servers)
			return
		}
	}
}

//func (ck *Clerk) PutAppend(key string, value string, op string) {
//	args, reply := PutAppendArgs{key, value, op, ck.handle, nrand()}, PutAppendReply{}
//
//	index := ck.leader
//	for reply.Err != OK { // Retry until success
//		ok := ck.servers[index%len(ck.servers)].Call("KVServer.PutAppend", &args, &reply)
//		if !ok || reply.WrongLeader {
//			index++
//		}
//	}
//	ck.leader = index % len(ck.servers) // Update latest known leader
//}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
