package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import (
	"labrpc"
	"time"
	"math/rand"
)

// import "bytes"
// import "labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor int
	log []*Log  //slice,index从1开始
	commitIndex int
	lastApplied int
	currentState string//0表示leader，1表示candidate，2表示follower
	nextIndex []int
	matchIndex []int
	isTimeout int//超时后发起election,0表示在这个周期里无vote或者append，1表示有，初始化为0
}

type Log struct {
	content string
	term int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.currentState == "leader"
	return term, isleader
}


//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm{
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}else if args.Term == rf.currentTerm {
		isUptodate := false
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			if rf.log[len(rf.log)-1].term == args.LastLogTerm {
				isUptodate = len(rf.log) <= args.LastLogIndex
			} else if rf.log[len(rf.log)-1].term < args.LastLogTerm {
				isUptodate = true
			} else {
				isUptodate = false
			}
		}
		reply.Term = rf.currentTerm
		reply.VoteGranted = isUptodate
		if isUptodate {
			rf.votedFor = args.CandidateId
		}
	}else{//这时候rf到达更新的term
		rf.currentTerm = args.Term
		rf.votedFor = -1
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.currentState = "follower"
		rf.votedFor = args.CandidateId
	}
	if reply.VoteGranted {
		rf.isTimeout = 1
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//define an AppendEntries RPC struct (though you may not need all the arguments yet)
//lab2A only finish heart beat mechanism
type AppendEntriesArgs struct{
	term int
	leaderId int
	prevLogIndex int
	prevLogTerm int
	enteries []*Log
	leaderCommit int
}

type AppendEntriesReply struct{
	term int
	success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
//todo
//心跳机制
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//if args.enteries == nil{//可以看成心跳机制
	//	reply.success = true
	//	reply.term = rf.currentTerm
	//}
	success := false
	if args.term < rf.currentTerm{
		success = false
	}else{
		if len(rf.log) < args.prevLogIndex || (len(rf.log) >= args.prevLogIndex && rf.log[args.prevLogIndex-1].term != args.prevLogTerm){
			success = false
			rf.isTimeout = 1
		}else{
			//todo append entry
			success = true
			rf.isTimeout = 1
			if args.leaderCommit < len(rf.log){
				rf.commitIndex = args.leaderCommit
			}else{
				rf.commitIndex = len(rf.log)
			}
		}
	}
	if args.term > rf.currentTerm{
		rf.currentTerm = args.term
		rf.currentState = "follower"
	}
	reply.success = success
	reply.term = rf.currentTerm
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
type vote_num struct {
	mu sync.Mutex
	num int
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = nil
	rf.isTimeout = 0
	rf.currentState = "follower"

	// initialize from state persisted before a crash
	//electElapse := rand.Int() % 1000
	rf.readPersist(persister.ReadRaftState())
	//todo
	//超时后转为candidate，发送request vote，转为leader
	go func(){//监控选举
		voteNum := &vote_num{}
		voteNum.num = 0
		time.Sleep(time.Duration(rand.Int()%1000) * time.Millisecond)
		for{
			rf.mu.Lock()
			if rf.isTimeout == 0 || rf.currentState == "candidate"{//超时,开始选举
				//voteNum := &vote_num{}
				//voteNum.num = 0
				rf.currentState = "candidate"
				rf.currentTerm += 1
				rf.votedFor = rf.me
				electTerm := rf.currentTerm
				electCandidateId := rf.me
				electLastLogIndex := len(rf.log)
				electLastLogTerm := rf.log[len(rf.log)-1].term
				for k, _ := range rf.peers {
					if k != rf.me {
						go func() {
							voteArgs := &RequestVoteArgs{electTerm, electCandidateId, electLastLogIndex, electLastLogTerm}
							voteReply := &RequestVoteReply{}
							ok := rf.sendRequestVote(k, voteArgs, voteReply)
							if ok {
								if voteReply.VoteGranted == true{
									rf.mu.Lock()
									if rf.currentTerm == voteArgs.Term{
									voteNum.mu.Lock()
									voteNum.num += 1
									voteNum.mu.Unlock()
								}
									if voteReply.Term > rf.currentTerm{
										rf.currentTerm = voteReply.Term
									}
									rf.mu.Unlock()
								}else{
									rf.mu.Lock()
									if voteReply.Term > rf.currentTerm{
										rf.currentTerm = voteReply.Term
									}
									rf.mu.Unlock()
								}
							}
						}()
						}
				}
				rf.mu.Unlock()
				time.Sleep(time.Duration(rand.Int()%1000) * time.Millisecond)
				rf.mu.Lock()
				if (voteNum.num + 1)> (len(rf.peers) / 2){
					rf.currentState = "leader"
					for k, _ := range rf.nextIndex {
						rf.nextIndex[k] = len(rf.log) + 1
					}
					for k, _ := range rf.matchIndex{
						rf.matchIndex[k] = 0
					}
				}
				rf.mu.Unlock()
			}else{//未超时
				rf.mu.Lock()
				rf.isTimeout = 0//重置
				rf.mu.Unlock()
				time.Sleep(time.Duration(rand.Int()%1000) * time.Millisecond)
			}

		}
	}()

	go func(){//检测是否为leader
		for{
			time.Sleep(time.Duration(rand.Int()%100) * time.Millisecond)//如果是leader，每隔100ms发送
			if rf.currentState == "leader"{
				rf.mu.Lock()
				hbTerm := rf.currentTerm
				rf.mu.Unlock()
				for k,_ := range rf.peers {
					if k != rf.me {
						go func() {
							heartBeatArgs := &AppendEntriesArgs{hbTerm, rf.me, rf.nextIndex[k] - 1, rf.log[rf.nextIndex[k]-1].term, nil, rf.commitIndex}
							heartBeatReply := &AppendEntriesReply{}
							rf.sendAppendEntries(k, heartBeatArgs, heartBeatReply)
							rf.mu.Lock()
							if heartBeatReply.term > rf.currentTerm{
								rf.currentTerm = heartBeatReply.term
							}
							rf.mu.Unlock()
						}()
					}
				}
			}
		}
	}()

	//todo apply



	return rf
}
