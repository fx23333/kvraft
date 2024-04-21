package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new Log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the Log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "sync/atomic"
import "../labrpc"
import "math/rand"
import "time"

//import "fmt"
import "bytes"
import "../labgob"

const FOLLOWER = 0
const CANDIDATE = 1
const LEADER = 2
const ELECTIONINTERVAL = 300 //300             //50
const ELECTIONBEGIN = 300    //300           //500
//这里我们使用两个变量来代表election timeout的一个范围[a, b]
//具体来说，ELECTIONBEGIN代表区间起点即a, 而ELECTIONINTERVAL用于表示区间长度

//允许的范围内，这两个值越小越好
const ELECTIONCHECKINTERVAL = 50 //50             //10
const HEATBEATINTERVAL = 50      //100

func Min(a int, b int) int {
	if a <= b {
		return a
	} else {
		return b
	}
}

func Max(a int, b int) int {
	if a >= b {
		return a
	} else {
		return b
	}
}

//
// as each Raft peer becomes aware that successive Log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed Log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	Snapshot     []byte
	Sequence     []int
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type     string
	Value    string
	Key      string
	ClientId int64
	Sequence int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	Mu                 sync.Mutex          // Lock to protect shared access to this peer's state
	peers              []*labrpc.ClientEnd // RPC end points of all peers
	persister          *Persister          // Object to hold this peer's persisted state
	me                 int                 // this peer's index into peers[]
	dead               int32               // set by Kill()
	electionnotTimeOut int

	lastVisitTime time.Time
	currentTerm   int
	VoteFor       int
	state         int
	Log           []interface{}
	logTerm       []int
	commitIndex   int
	nextIndex     []int //需要初始化
	matchIndex    []int //需要初始化
	logAccept     []int
	lastApplied   int

	snapshot          []byte
	lastIncludedIndex int
	lastIncludedTerm  int
	applyCh           chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

func getRandomElectionnotTimeOut() int {
	rand.Seed(time.Now().UnixNano())
	n := rand.Intn(ELECTIONINTERVAL) + ELECTIONBEGIN
	return n
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.Mu.Lock()
	isleader = (rf.state == LEADER)
	term = rf.currentTerm
	rf.Mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	//本函数不会上锁，需要在外部上锁

	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	var err error
	//rf.Mu.Lock()
	err = e.Encode(rf.currentTerm)
	if err != nil {
		panic("Persist error1" + err.Error())
	}
	err = e.Encode(rf.VoteFor)
	if err != nil {
		panic("Persist error2" + err.Error())
	}
	err = e.Encode(rf.lastIncludedIndex)
	if err != nil {
		panic("Persist error3" + err.Error())
	}
	err = e.Encode(rf.lastIncludedTerm)
	if err != nil {
		panic("Persist error6" + err.Error())
	}
	err = e.Encode(rf.Log)
	if err != nil {
		panic("Persist error4" + err.Error())

	}
	err = e.Encode(rf.logTerm)
	if err != nil {
		panic("Persist error5" + err.Error())
	}

	//DPrintf("Encoding! term = %v, VoteFor = %v, Log = %v\n", rf.currentTerm, rf.VoteFor, rf.Log)
	//for _, logEntry := range rf.Log {
	//	//DPrintf("I am encoding %v\n", logEntry)
	//	err = e.Encode(logEntry)
	//	if err != nil {
	//		panic("Persist error4" + err.Error())	//	}
	//	//switch tempLogEntry := logEntry.(type) {
	//	//case int:
	//	//	e.Encode(tempLogEntry)
	//	//case string:
	//	//	e.Encode(tempLogEntry)
	//	//}
	//}

	//for _, logTerm := range rf.logTerm {
	//	err = e.Encode(logTerm)
	//	if err != nil {
	//		panic("Persist error5" + err.Error())
	//	}
	//}

	//rf.Mu.Unlock()
	data := w.Bytes()
	//DPrintln(data)
	rf.persister.SaveStateAndSnapshot(data, rf.snapshot)

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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var tempCurrentTerm, tempVoteFor, tempLastIncludedIndex, tempLastIncludedTerm int
	var tempLog []interface{}
	var tempLogTerm []int
	var err error
	err = d.Decode(&tempCurrentTerm)
	if err != nil {
		panic("Decode Error 1" + err.Error())
	}
	err = d.Decode(&tempVoteFor)
	if err != nil {
		panic("Decode Error 2" + err.Error())
	}
	err = d.Decode(&tempLastIncludedIndex)
	if err != nil {
		panic("Decode Error 3" + err.Error())
	}
	err = d.Decode(&tempLastIncludedTerm)
	if err != nil {
		panic("Decode Error 6" + err.Error())
	}
	// var tempEntry interface{}
	// var tempEntryTerm int
	//DPrintf("term = %v, VoteFor = %v, length = %v\n", tempCurrentTerm, tempVoteFor, logLength)
	//DPrintf("real: %v, %v, %v\n\n", rf.currentTerm, rf.VoteFor, rf.Log)

	err = d.Decode(&tempLog)
	if err != nil {
		panic("Decode Error 4" + err.Error())
	}

	err = d.Decode(&tempLogTerm)
	if err != nil {
		panic("Decode Error 5" + err.Error())
	}
	//for i := 0; i < logLength; i++ {
	//	var tempEntry string
	//	err = d.Decode(&tempEntry)
	//	if err != nil {
	//		DPrintf("Decode Error 4! %v, index = %v\n", err.Error(), i)
	//		return
	//	} else {
	//		tempLog = append(tempLog, tempEntry)
	//	}
	//}
	//
	//for i := 0; i < logLength; i++ {
	//	var tempEntryTerm int
	//	err := d.Decode(&tempEntryTerm)
	//	if err != nil {
	//		DPrintf("Decode Error3! %v, index = %v\n", err.Error(), i)
	//		return
	//	} else {
	//		tempLogTerm = append(tempLogTerm, tempEntryTerm)
	//	}
	//}

	rf.Mu.Lock()
	rf.snapshot = rf.persister.ReadSnapshot()
	if rf.snapshot != nil {
		amsg := ApplyMsg{CommandValid: false, Snapshot: rf.snapshot}

		rf.applyCh <- amsg
	}
	rf.currentTerm = tempCurrentTerm
	rf.lastIncludedIndex = tempLastIncludedIndex
	rf.lastIncludedTerm = tempLastIncludedTerm
	rf.VoteFor = tempVoteFor
	rf.Log = make([]interface{}, len(tempLog))
	rf.logTerm = make([]int, len(tempLogTerm))
	copy(rf.Log, tempLog)
	copy(rf.logTerm, tempLogTerm)
	rf.lastApplied = rf.lastIncludedIndex
	rf.commitIndex = rf.lastIncludedIndex
	// rf.persist()
	rf.Mu.Unlock()
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	// Your data here (2A).
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []interface{}
	LogTerm      []int
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term            int
	Success         bool
	ConflictedIndex int
	ConflictedTerm  int
	Loglength       int
	Retry           bool //如果retry为true，不需要修改args，直接重发
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func updateCommitIndex(rf *Raft, commitIndex int) {

	// 	if (rf.commitIndex < commitIndex) {
	// //		DPrintln("Hiiiiiii")
	// 		rf.commitIndex = commitIndex
	// 	}
	rf.commitIndex = Max(rf.commitIndex, Min(commitIndex, len(rf.Log)-1+(rf.lastIncludedIndex+1)))
}

// 检查candidate是否更加up-to-date，也即返回值，同时会自动处理VoteFor
func ElectionRestrictionCheck(rf *Raft, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//本函数无锁，需要在外部上锁
	var myterm int
	// mylength := len(rf.Log) - 1
	mylength := rf.lastIncludedIndex + len(rf.Log)
	//if mylength == -1 {
	//	myterm = -200
	//} else {
	//	myterm = rf.logTerm[mylength]
	//}

	if mylength == -1 {
		myterm = -200
	} else if mylength == rf.lastIncludedIndex {
		myterm = rf.lastIncludedTerm
	} else {
		myterm = rf.logTerm[mylength-(rf.lastIncludedIndex+1)]
	}

	if myterm > args.LastLogTerm {
		// 我的最后一条log entry 的term更新，拒绝投票
		DPrintf("ER S3: me: %v, mylastlogterm = %v, candilastlogterm = %v, candidate = %v\n", rf.me, myterm, args.LastLogTerm, args.CandidateID)
		reply.VoteGranted = false
	} else if myterm < args.LastLogTerm {
		// 我的最后一条log entry 的term和candidate一样新，投票
		rf.lastVisitTime = time.Now()
		rf.VoteFor = args.CandidateID
		rf.persist()
		reply.VoteGranted = true
		DPrintf("ER S1: me: %v, mylastlogterm = %v, candilastlogterm = %v, candidate = %v\n", rf.me, myterm, args.LastLogTerm, args.CandidateID)
	} else {
		// term一样新，则比较长度
		if mylength > args.LastLogIndex {
			DPrintf("ER S4: me: %v, mylastlogterm = %v, candilastlogterm = %v, candidate = %v\n", rf.me, myterm, args.LastLogTerm, args.CandidateID)
			reply.VoteGranted = false
		} else {
			rf.lastVisitTime = time.Now()
			rf.VoteFor = args.CandidateID
			rf.persist()
			reply.VoteGranted = true
			DPrintf("ER S2: me: %v, mylastlogterm = %v, mylogterm = %v, candilastlogterm = %v, candidate = %v", rf.me, myterm, rf.logTerm, args.LastLogTerm, args.CandidateID)

		}
	}

	return reply.VoteGranted

}

//
// example RequestVote RPC handler.
//

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	DPrintf("%v(%v, %v) receive VoteRequest from %v(%v)\n", rf.me, rf.currentTerm, rf.state, args.CandidateID, args.Term)
	rf.Mu.Lock()
	if args == nil {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	DPrintf("%v(%v, %v) receive222 VoteRequest from %v(%v)\n", rf.me, rf.currentTerm, rf.state, args.CandidateID, args.Term)
	//DPrintf("%v receive %v\n", rf.me, args.CandidateID)
	if rf.state == LEADER {
		if rf.currentTerm >= args.Term {
			// 自己的term更加新，不承认candidate
			reply.VoteGranted = false
		} else {
			//承认新的leader
			//rf.currentTerm = args.Term
			rf.state = FOLLOWER
			// rf.lastVisitTime = time.Now()
			rf.currentTerm = Max(rf.currentTerm, args.Term)
			//election restriction

			if !ElectionRestrictionCheck(rf, args, reply) {
				rf.VoteFor = -1
				rf.persist()
			}
		}

	} else if rf.state == FOLLOWER {
		if rf.currentTerm > args.Term {
			//candidate状态太旧，不承认
			reply.VoteGranted = false

		} else if rf.currentTerm == args.Term {
			//已经投票给了这个candidate 或者还没投票

			if rf.VoteFor == -1 {
				// 未投票！
				ElectionRestrictionCheck(rf, args, reply)
				//若检测通过，则自动设置VoteFor，否则VoteFor不变

			} else {
				// 重要！！！！！！！！
				// 此时，follower已经投过票了!如果遇到的不是当前投票的candidate，则一定不会投票。如果遇到的是已经投过的candidate，
				// 不应该再去检查一致性，因为第二次可能会通不过。（或者在检查一致性，返回失败之后，不重设-1即可，否则可能会导致当前follower投出两票）
				// 若是原始candidate重发，则返回false也无影响。因为在candidate的代码中，受到投票才会计数
				reply.VoteGranted = false
			}
		} else if rf.currentTerm < args.Term {
			// 当前follower的term太旧了，同样要检查是否up-to-date然后再投票

			rf.currentTerm = Max(rf.currentTerm, args.Term)
			if !ElectionRestrictionCheck(rf, args, reply) {
				rf.VoteFor = -1
				rf.persist()
			}
		}

	} else { //candiedate的情况
		if rf.currentTerm >= args.Term {
			// 若term相等则公平竞争，若发送者term更小，则忽略。总之不投票

			reply.VoteGranted = false
		} else {
			//承认新的leader
			rf.currentTerm = Max(rf.currentTerm, args.Term)
			rf.state = FOLLOWER
			if !ElectionRestrictionCheck(rf, args, reply) {
				//DPrintf("I disagree\n")
				rf.VoteFor = -1
				rf.persist()
			}
		}
	}
	rf.currentTerm = Max(rf.currentTerm, args.Term)
	reply.Term = rf.currentTerm //这句话必须放在最后。如果某个server状态更新之后再去投票，它返回的应该是更新的term，而不是原始term，否则会导致candidate丢掉投票
	rf.Mu.Unlock()
	// Your code here (2A, 2B).
}

// 这个函数的主要目的是检查RPC参数中的两个Prev，同时在检查通过后也会完成log entries的插入
// 注意这里是在有锁的环境下执行的，因此不需要加锁
// 同时，这里在AppendCommand函数中被调用，该函数在最后进行了持久化，因此本函数不需要持久化
func ConsistencyCheck(rf *Raft, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {

	reply.Loglength = len(rf.Log) + (rf.lastIncludedIndex + 1)
	//这里是在检查follower长度是否小于leader！注意还需要考虑到leader的log大小为0的情况！

	//if args.PrevLogIndex != -1 && args.PrevLogIndex <= rf.lastIncludedIndex {
	//	if args.PrevLogIndex == rf.lastIncludedIndex && args.PrevLogTerm != rf.lastIncludedTerm {
	//		reply.Success = false
	//		return false
	//	}
	//}

	//if rf.lastIncludedIndex >= args.PrevLogIndex+1 && args.PrevLogIndex != -1 {
	//	// 此时欲写入位置与已快照位置重叠，理论上这种情况不会发生，我们需要要考虑到过时的RPC，我们直接忽略
	//
	//	DPrintln("Consistency Check: S1: follower shorter than leader; me: ", rf.me, "prevlogindex = ", args.PrevLogIndex, "logsize = ", len(rf.Log))
	//	// reply.Loglength = len(rf.Log) + (rf.lastIncludedIndex + 1)
	//
	//	reply.Success = false
	//} else if args.PrevLogIndex != -1 &&
	//	len(rf.Log)-1+(rf.lastIncludedIndex+1) < args.PrevLogIndex {
	//	// 这里代表当前log比leader的log小
	//
	//	DPrintln("S3", len(rf.Log)-1+(rf.lastIncludedIndex+1), args.PrevLogIndex+len(args.Entries))
	//	reply.Success = false
	//} else if args.PrevLogIndex != -1 &&
	//	args.PrevLogIndex == rf.lastIncludedIndex &&
	//	args.PrevLogTerm != rf.lastIncludedTerm {
	//	DPrintln("S4")
	//	// 这里需要考虑欲写入位置是lastIncludedIndex下一条的位置，必须拿出特判，否则与PrevLogIndex进行比较时，会数组越界
	//	reply.Success = false
	//} else if args.PrevLogIndex != -1 && args.PrevLogIndex > rf.lastIncludedIndex &&
	//	rf.logTerm[args.PrevLogIndex-(rf.lastIncludedIndex+1)] != args.PrevLogIndex {
	//	//这里log长度比leader不短，并且失配
	//	// 注意args.PrevLogIndex是为了避免出现刚刚启动。这种情况下则不需要比较PrevLogTerm等，直接匹配成功
	//	reply.ConflictedTerm = rf.logTerm[args.PrevLogIndex-(rf.lastIncludedIndex+1)]
	//	DPrintln("S2")
	//
	//	//二分查找和失配位置同term的第一条数据,即右区间左端点
	//	l := 0
	//	r := args.PrevLogIndex - (rf.lastIncludedIndex + 1)
	//	var mid int
	//	for l < r {
	//		mid = (l + r) >> 1
	//		if rf.logTerm[mid] >= rf.logTerm[args.PrevLogIndex-(rf.lastIncludedIndex+1)] {
	//			r = mid
	//		} else {
	//			l = mid + 1
	//		}
	//	}
	//	//DPrintf("S2: me:%v, leader:%v, myprevlogterm: %v argsPrevlogterm%v, l: %v, r: %v, logTerm[l]=%v, logTerm[r] = %v",
	//	//	rf.me, args.LeaderID, rf.logTerm[args.PrevLogIndex], args.PrevLogTerm, l, r, rf.logTerm[l], rf.logTerm[r])
	//
	//	//if l-1 >= 0 {
	//	//	DPrintf(", lastlogterm = %v\n", rf.logTerm[l-1])
	//	//}
	//	reply.ConflictedIndex = r + rf.lastIncludedIndex + 1
	//	//DPrintf("S2 l = %v\n", l)
	//	reply.Success = false

	reply.Loglength = len(rf.Log) + (rf.lastIncludedIndex + 1)
	//这里是在检查follower长度是否小于leader！注意还需要考虑到leader的log大小为0的情况！

	//if args.PrevLogIndex != -1 && args.PrevLogIndex <= rf.lastIncludedIndex {
	//	if args.PrevLogIndex == rf.lastIncludedIndex && args.PrevLogTerm != rf.lastIncludedTerm {
	//		reply.Success = false
	//		return false
	//	}
	//}

	if len(rf.Log)-1+(rf.lastIncludedIndex+1) < args.PrevLogIndex && args.PrevLogIndex != -1 {

		//DPrintln("Consistency Check: S1: follower shorter than leader; me: ", rf.me, "prevlogindex = ", args.PrevLogIndex, "logsize = ", len(rf.Log))
		// reply.Loglength = len(rf.Log) + (rf.lastIncludedIndex + 1)

		reply.Success = false
		DPrintf("S0(Log shorter), me = %v, leader = %d, lastincluedindex = %v, lastincludedterm = %v", rf.me, args.LeaderID, rf.lastIncludedIndex, rf.lastIncludedTerm)
	} else if args.PrevLogIndex != -1 && args.PrevLogIndex+1 <= rf.lastIncludedIndex {
		reply.Success = false
		DPrintf("S1")
	} else if args.PrevLogIndex != -1 &&
		args.PrevLogIndex == rf.lastIncludedIndex && args.PrevLogTerm != rf.lastIncludedTerm {
		reply.Success = false
		reply.ConflictedIndex = -114514
		DPrintf("SS, me = %v, leader = %v, args.PrevLogIndex = %v, args.PrevLogTerm = %v, rf.lastincludedTerm = %v", rf.me, args.LeaderID, args.PrevLogIndex, args.PrevLogTerm, rf.lastIncludedTerm)
	} else if args.PrevLogIndex != -1 &&
		args.PrevLogIndex > rf.lastIncludedIndex &&
		rf.logTerm[args.PrevLogIndex-(rf.lastIncludedIndex+1)] != args.PrevLogTerm {
		//这里log长度比leader不短，并且失配
		// 注意args.PrevLogIndex是为了避免出现刚刚启动。这种情况下则不需要比较PrevLogTerm等，直接匹配成功
		reply.ConflictedTerm = rf.logTerm[args.PrevLogIndex-(rf.lastIncludedIndex+1)]
		// DPrintln("S2")

		//二分查找和失配位置同term的第一条数据,即右区间左端点
		l := 0
		r := args.PrevLogIndex - (rf.lastIncludedIndex + 1)
		var mid int
		for l < r {
			mid = (l + r) >> 1
			if rf.logTerm[mid] >= rf.logTerm[args.PrevLogIndex-(rf.lastIncludedIndex+1)] {
				r = mid
			} else {
				l = mid + 1
			}
		}
		DPrintf("S2: me:%v, leader:%v, myprevlogterm: %v argsPrevlogterm%v, l: %v, r: %v, logTerm[l]=%v, logTerm[r] = %v",
			rf.me, args.LeaderID, 114514, args.PrevLogTerm, l, r, 114514, 114514)

		//if l-1 >= 0 {
		//	DPrintf(", lastlogterm = %v\n", rf.logTerm[l-1])
		//}
		reply.ConflictedIndex = r + rf.lastIncludedIndex + 1
		//DPrintf("S2 l = %v\n", l)
		reply.Success = false
	} else {
		//DPrintln("S3 ")
		//DPrintln("S3 ", args)
		// 匹配成功

		if len(args.Entries) != 0 {
			// DPrintln(rf.me, "My new log = ", rf.Log, "Term = ", rf.logTerm, "args = ", args)
			// leader发给follower的entries不为空时
			lastEntryIndex := args.PrevLogIndex + len(args.Entries)

			// lastEntryIndex 代表leader传给follower的最后一条log entry的下标
			for i := args.PrevLogIndex + 1; i <= lastEntryIndex; i++ {
				// 这里i的范围为leader传给follower的log entries的第一条到最后一条的下标
				// panic("")
				if i-(rf.lastIncludedIndex+1) > len(rf.Log)-1 {
					// 当 当前log entry超过了当前follower的log时，直接将leader后续log entry全部插入即可

					rf.Log = append(rf.Log[:i-(rf.lastIncludedIndex+1)], args.Entries[i-args.PrevLogIndex-1:]...)
					rf.logTerm = append(rf.logTerm[:i-(rf.lastIncludedIndex+1)], args.LogTerm[i-args.PrevLogIndex-1:]...)
					// DPrintln(rf.me, "My new log222 = ", rf.Log, "Term = ", rf.logTerm)
					break
				} else {
					// 当 当前log entry没有超过当前follower的log时，我们需要找到和leader的log不匹配的地方
					// 注意6.824指南中说明了，我们不可以直接将leader发给folloer 的log entry来代替PrevLogIndex
					// 后面的内容，因为可能当前RPC是过时的，反而把当前follower更加新的log替代了

					a := rf.logTerm[i-(rf.lastIncludedIndex+1)]
					// a代表当前follower对应的log entry
					b := args.LogTerm[i-args.PrevLogIndex-1]
					// b代表leader发过来的log entry
					if a != b {
						rf.Log = append(rf.Log[:i-(rf.lastIncludedIndex+1)], args.Entries[i-args.PrevLogIndex-1:]...)
						rf.logTerm = append(rf.logTerm[:i-(rf.lastIncludedIndex+1)], args.LogTerm[i-args.PrevLogIndex-1:]...)

						break
					}

				}

			}
			//DPrintln(rf.me, "My new log = ", rf.Log, "Term = ", rf.logTerm)
		}

		//updateCommitIndex(rf, args.LeaderCommit)
		reply.Success = true
	}
	if reply.Success {
		updateCommitIndex(rf, args.LeaderCommit)
	}
	//updateCommitIndex(rf, args.LeaderCommit)
	return reply.Success
}

//插入
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//panic("hi")
	//注意在最后进行了持久化！
	DPrintf("%v receive AppendEntries from leader %v", rf.me, args.LeaderID)
	if args == nil {
		reply.Success = false
		reply.ConflictedIndex = -1
		reply.ConflictedTerm = -1
		return
	}

	rf.Mu.Lock()
	DPrintf("%v receive AppendEntries from leader %v and can get lock!", rf.me, args.LeaderID)
	reply.Term = rf.currentTerm

	//follower收到appendEntries的情况
	if rf.state == FOLLOWER {
		if rf.currentTerm > args.Term || rf.VoteFor != args.LeaderID {
			//这里对应fig2中Receiver implementation中第1、2条
			reply.ConflictedIndex = -1
			reply.ConflictedTerm = -1
			reply.Success = false
		} else {
			//这是CurrentTerm < args.Term的情况，也是大部分情况下应该适用的情况
			//即leader给follower插入新的log entries，或者是heartbeat
			rf.currentTerm = args.Term
			rf.lastVisitTime = time.Now()
			//rf.persist()
			ConsistencyCheck(rf, args, reply)
		}

	} else if rf.state == CANDIDATE {
		if rf.currentTerm > args.Term {
			//这时发送消息时的leader过期已经过期
			reply.ConflictedIndex = -1
			reply.ConflictedTerm = -1
			reply.Success = false
			//DPrintln("S4")
		} else if rf.currentTerm < args.Term {
			//发现了比自己term更大的term，接受新的leader
			rf.VoteFor = args.LeaderID
			rf.lastVisitTime = time.Now()
			rf.currentTerm = args.Term
			rf.state = FOLLOWER
			//rf.persist()
			ConsistencyCheck(rf, args, reply)

			//DPrintln("S5")
		} else { //rf.currentTerm == args.Term
			//接受新的leader，这里也应该是常规情况
			//当前candidate和当前leader同时竞选leader，当前leader获胜，因此candidate变回follower
			rf.lastVisitTime = time.Now()
			rf.VoteFor = args.LeaderID
			rf.state = FOLLOWER
			//DPrintln("S6")
			//rf.persist()
			ConsistencyCheck(rf, args, reply)
		}

	} else { //leader
		if rf.currentTerm > args.Term {
			//do nothing except for set reply.Success as false
			//调用RPC的leader过期了，自己才应该（有可能）是真正的leader
			//DPrintln("S7")
			reply.ConflictedIndex = -1
			reply.ConflictedTerm = -1
			reply.Success = false
		} else if rf.currentTerm < args.Term {
			//说明当前的leader过期了，可能发生在一个network partition的情况下
			rf.VoteFor = args.LeaderID
			rf.lastVisitTime = time.Now()
			rf.currentTerm = args.Term
			rf.state = FOLLOWER
			//rf.persist()
			ConsistencyCheck(rf, args, reply)
			//DPrintln("S8")
		} else {
			//这是永远不可能发生的情况，一个term内不可能会有两个合法leader！出现Bug
			reply.Success = false
			DPrintf("impossible situation!!!!!\n i am %v, %v, from leader %v\n", rf.me, rf.state, args.LeaderID)
		}

	}
	//DPrintf("%v(%v) receive append from %v(%v), entry = %v, sucess = %v\n", rf.me, rf.currentTerm, args.LeaderID, args.Term, args.Entries, reply.Success)

	//更改了VoteFor，log等信息，所以需要持久化
	rf.persist()
	rf.Mu.Unlock()
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
// within a notTimeOut interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own notTimeOuts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//

// func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
// 	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
// 	return ok
// }

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's Log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft Log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//

// type AppendEntriesArgs struct {
//     Term int
// 	LeaderID int
// 	PrevLogIndex int
// 	PrevLogTerm int
// 	Entries     []interface{}
// 	LeaderCommit int
// }

// type AppendEntriesReply struct {
// 	Term int
// 	Success bool
//  ConflictedIndex int
//  ConflictedTerm  int
// }

//DEBUG_WARNING maybe可以检查锁的使用
func AppendCommand(rf *Raft, port *labrpc.ClientEnd, peersidx int, args *AppendEntriesArgs) (bool, bool, int) {
	reply := &AppendEntriesReply{}
	rpcSuccess := port.Call("Raft.AppendEntries", args, reply)

	rf.Mu.Lock()
	//逻辑疑似存在问题？第二个条件也许不应该使用？改成//if (rpcSuccess && reply.Term > rf.currentTerm) {？
	if rpcSuccess && (reply.Term > rf.currentTerm || rf.currentTerm > args.Term) {
		DPrintf("%v reply = %v", rf.me, reply)
		rf.currentTerm = Max(reply.Term, rf.currentTerm)

		//满足fig2中rules for servers第二条
		rf.state = FOLLOWER
		rf.lastVisitTime = time.Now()
		DPrintf("%v: I am no more leader!,  newterm = %v\n", rf.me, rf.currentTerm)
		rf.Mu.Unlock()
		return true, false, reply.Term
	}
	rf.Mu.Unlock()

	if rpcSuccess && !reply.Success && reply.ConflictedIndex != -1 && reply.ConflictedTerm != -1 {
		//返回值中的success失败（代表插入失败），且产生了冲突，  conflictindex =-1 conflictterm = -1代表非冲突
		rf.Mu.Lock()
		DPrintf("%v reply = %v", rf.me, reply)
		if true {
			l := 0
			r := len(rf.logTerm) - 1

			var mid int
			for l < r {
				mid = (l + r + 1) >> 1
				if rf.logTerm[mid] <= reply.ConflictedTerm {
					l = mid
				} else {
					r = mid - 1
				}
			}
			//DPrintf("l = %v, r = %v\n", l, r)
			//DPrintf("peer = %v, index = %v, term = %v, length = %v, entries = %v, confidx = %v, confterm = %v\n",
			//peersidx, reply.ConflictedIndex, reply.ConflictedTerm, reply.Loglength, args.Entries, reply.ConflictedIndex, reply.ConflictedTerm)
			if reply.Loglength-1 < args.PrevLogIndex {
				//对应笔记情况三，也就是follower的log完全比 leader的短(这里考虑的是发出RPC时的leader，当前leader的log或许也可以？)
				//DPrintln("AppendCommand S1!")
				rf.nextIndex[peersidx] = Min(reply.Loglength, rf.nextIndex[peersidx])
				DPrintf("Append failure situation 3 leader: %v, peer: %v, nextindex = %v\n", rf.me, peersidx, rf.nextIndex[peersidx])
			} else if r <= -1 {
				//二分法会保证lr都在区间范围内，这里是为了预防 在设置区间时，len = 0导致r == -1的情况，也就是此时leader没有log
				DPrintf("Append failure situation 5 leader: %v, peer: %v, nextindex = %v\n", rf.me, peersidx, rf.nextIndex[peersidx])
				rf.nextIndex[peersidx] = 0
			} else if rf.logTerm[r] < reply.ConflictedTerm {
				//对应笔记情况一，follower失配位置同term的第一条log entry，与leader对应的log entry的term不匹配。
				//此时，要在leader中找到小于等于ConflictedTerm的最大Term的最后一条log entry，这是在二分法中完成的。
				rf.nextIndex[peersidx] = Min(reply.ConflictedIndex, rf.nextIndex[peersidx])
				DPrintf("Append failure situation 1 leader: %v, peer: %v, nextindex = %v\n", rf.me, peersidx, rf.nextIndex[peersidx])
			} else if rf.logTerm[r] > reply.ConflictedTerm { //新情况
				DPrintf("Append failure situation 4 leader: %v, peer: %v, nextindex = %v\n", rf.me, peersidx, rf.nextIndex[peersidx])
				rf.nextIndex[peersidx] = 0
			} else { //rf.logTerm[l] == reply.ConflictedTerm             对应情况二
				DPrintf("Append failure situation 2 leader: %v, peer: %v, nextindex = %v\n", rf.me, peersidx, rf.nextIndex[peersidx])
				rf.nextIndex[peersidx] = Min(l+1+(rf.lastIncludedIndex+1), rf.nextIndex[peersidx])
			}

			// 新写的代码，还没有测试！

			// if (reply.Loglength - 1 < args.PrevLogIndex) {
			// 	//对应笔记情况三，也就是follower的log完全比 leader的短(这里考虑的是发出RPC时的leader，当前leader的log或许也可以？)
			// 	//DPrintln("AppendCommand S1!")
			// 	rf.nextIndex[peersidx] = Min(reply.Loglength, rf.nextIndex[peersidx])
			// 	DPrintf("Append failure situation 3 leader: %v, peer: %v, nextindex = %v\n", rf.me, peersidx, rf.nextIndex[peersidx])
			// } else if (r == -1) {
			// 	//二分法会保证lr都在区间范围内，这里是为了预防 在设置区间时，len = 0导致r == -1的情况，也就是此时leader没有log
			// 	DPrintf("Append failure situation 5 leader: %v, peer: %v, nextindex = %v\n", rf.me, peersidx, rf.nextIndex[peersidx])
			// 	rf.nextIndex[peersidx] = 0
			// } else if (rf.logTerm[reply.ConflictedIndex] < reply.ConflictedTerm) {
			// 	//对应笔记情况一，follower失配位置同term的第一条log entry，与leader对应的log entry的term不匹配。
			// 	//此时，要在leader中找到小于等于ConflictedTerm的最大Term的最后一条log entry，这是在二分法中完成的。
			// 	rf.nextIndex[peersidx] = Min(reply.ConflictedIndex, rf.nextIndex[peersidx])
			// 	DPrintf("Append failure situation 1 leader: %v, peer: %v, nextindex = %v\n", rf.me, peersidx, rf.nextIndex[peersidx])
			// } else if (rf.logTerm[reply.ConflictedIndex] > reply.ConflictedTerm) {         //新情况
			// 	DPrintf("Append failure situation 4 leader: %v, peer: %v, nextindex = %v\n", rf.me, peersidx, rf.nextIndex[peersidx])
			// 	rf.nextIndex[peersidx] = Min(l + 1, rf.nextIndex[peersidx])
			// } else {//rf.logTerm[l] == reply.ConflictedTerm             对应情况二
			// 	DPrintf("Append failure situation 2 leader: %v, peer: %v, nextindex = %v\n", rf.me, peersidx, rf.nextIndex[peersidx])
			// 	rf.nextIndex[peersidx] = Min(l + 1, rf.nextIndex[peersidx])
			// }

			rf.Mu.Unlock()
		}

		//rf.Mu.Unlock()

	} else if rpcSuccess && reply.Success {
		rf.Mu.Lock()
		var idx int
		for idx = 0; idx < len(rf.peers); idx++ {
			if rf.peers[idx] == port {
				break
			}
		}
		DPrintf("leader %v to %v AppendEntries OK!\n", rf.me, idx)
		//注意这里必须要用args中engtries的大小，不能用leader当前log的大小！
		rf.matchIndex[peersidx] = Max(rf.matchIndex[peersidx], args.PrevLogIndex+len(args.Entries))

		//OPTIMIZATION_WARNING  这里或许没有用？
		rf.nextIndex[peersidx] = Max(len(rf.Log)+(rf.lastIncludedIndex+1), rf.nextIndex[peersidx])
		for i := args.PrevLogIndex + 1; i <= args.PrevLogIndex+len(args.Entries); i++ {
			if len(rf.logAccept)-1 <= i {
				rf.logAccept = append(rf.logAccept, 1)
			} else {
				rf.logAccept[i]++
			}
		}
		rf.Mu.Unlock()
	}
	return rpcSuccess, reply.Success, reply.Term
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	var isLeader bool
	//var cmdIndex int
	rf.Mu.Lock()

	if rf.state == LEADER {
		isLeader = true
	} else {
		isLeader = false
	}
	term = rf.currentTerm

	if isLeader {
		//DPrintf("%v command = %v\n", rf.me, command)

		//cmdIndex = len(rf.Log)
		rf.Log = append(rf.Log, command)
		rf.logTerm = append(rf.logTerm, rf.currentTerm)
		//DPrintf("index = %v\n", index)

		if len(rf.logAccept)-1 <= len(rf.Log) {
			rf.logAccept = append(rf.logAccept, 1)
		} else {
			rf.logAccept[len(rf.Log)]++
		}

		DPrintf("Leader %vget command: %v, term = %v, Log = %v, logterm = %v\n", rf.me, command, rf.currentTerm, rf.Log, rf.logTerm)
	}

	//DPrintf("%v, %v, %v\n", rf.me, command, isLeader)
	// Your code here (2B).
	index = len(rf.Log) + rf.lastIncludedIndex + 1

	rf.persist()
	rf.Mu.Unlock()

	//DPrintf("get start!\n")
	if isLeader {
		//DPrintf("i am leader %v, i am sending index = %v, term = %v, commited = %v\n", rf.me, index, term, rf.commitIndex)
		//go StartSendingAppend(rf, command, index)
	}
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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

func (rf *Raft) follower() {
	//这是一个常驻协程（只要rf的状态没有发生变化），主要功能为进行超时检查和应用已提交log

	rf.Mu.Unlock()
	//注意在peer函数中没有释放锁，在这里释放

	for {
		var applyQueue []ApplyMsg
		time.Sleep(time.Duration(ELECTIONCHECKINTERVAL) * time.Millisecond)
		//这里的sleep的作用是避免本常驻协程总是在死循环拖慢整个系统速度
		DPrintf("%v is follower, term = %v, votefor = %v", rf.me, rf.currentTerm, rf.VoteFor)
		rf.Mu.Lock()
		if rf.state != FOLLOWER {
			//注意我们在一开始释放了锁，我们需要再次获得锁时检查我们的状态是否发生了变化
			DPrintf("%v is no more follower", rf.me)
			rf.Mu.Unlock()
			return
		} else {
			//检查是否超时
			DPrintf("hi, lastvisitedTime = %v", rf.lastVisitTime)
			if int(time.Now().Sub(rf.lastVisitTime)/time.Millisecond) > rf.electionnotTimeOut {
				rf.state = CANDIDATE
				DPrintf("%v is time-out", rf.me)
				rf.Mu.Unlock()
				return
			}
		}

		//定期检查commitidx
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			amsg := ApplyMsg{CommandValid: true, Command: rf.Log[i-(rf.lastIncludedIndex+1)], CommandIndex: i + 1}

			if rf.Log[i-(rf.lastIncludedIndex+1)] != nil {
				//DPrintln("Leader is going to apply")
				DPrintln("follower commit: ", "me: ", rf.me, " ", amsg, "Log= ", rf.Log, "logterm = ", rf.logTerm, "commitindex = ", rf.commitIndex, "lastapplied", rf.lastApplied)
				rf.lastApplied++
				//rf.Mu.Unlock()
				//rf.applyCh <- amsg
				applyQueue = append(applyQueue, amsg)
				//rf.Mu.Lock()
				DPrintln("follower commit over: ", "me: ", rf.me, " ", amsg)
			} else {
				panic("")
			}

		}
		rf.Mu.Unlock()
		for _, amsg := range applyQueue {
			rf.applyCh <- amsg
		}

	}
}

// type AppendEntriesArgs struct {
//     Term int
// 	LeaderID int
// 	PrevLogIndex int
// 	PrevLogTerm int
// 	// entries[]
// 	// leaderCommit int
// }

// type ApplyMsg struct {
// 	CommandValid bool
// 	Command      interface{}
// 	CommandIndex int
// }

func (rf *Raft) leader() {
	//需要初始化leader的一些数据，包括nextIndex数组和matchIndex数组，在论文中指出。

	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.Log) + rf.lastIncludedIndex + 1
		rf.matchIndex[i] = -1
	}

	rf.Mu.Unlock()
	//初始化leader必须的数据结构，然后释放锁。注意在peer处上了锁，此处必须解锁

	for {
		var applyQueue []ApplyMsg
		rf.Mu.Lock()
		//由于中途释放了锁，rf状态可能发生改变，必须每一轮都需要判断
		if rf.state != LEADER {
			rf.Mu.Unlock()
			DPrintf("%v: I am no more leader!\n", rf.me)
			return
		}

		//给每个peer发送heartbeat，我们并未区分heartbeat或是AppendEntry，将AppendEntry融合在heartbeat中
		for idx, port := range rf.peers {
			if idx == rf.me {
				continue
			}
			//DPrintf("I am going to append\n")
			if false {

			} else {

				if rf.nextIndex[idx] <= rf.lastIncludedIndex {
					args := InstallSnapshotArgs{Term: rf.currentTerm, LeaderID: rf.me,
						LastIncludedIndex: rf.lastIncludedIndex, LastIncludedTerm: rf.lastIncludedTerm, Data: rf.snapshot}
					// panic("")
					DPrintf("leader %v am going to call InstallSnapShot RPC to %v", rf.me, idx)
					go SendingInstallSnapshot(rf, &args, port, idx)
					//DPrintf("new data, rf.lastincludedindex = %v, rf.nextindex = %v", rf.lastIncludedIndex, rf.nextIndex[idx])
					//continue
				} else {
					//设置tempPrevLogIndex和tempPrevLogTerm
					var tempPrevLogIndex, tempPrevLogTerm int
					tempPrevLogIndex = rf.nextIndex[idx] - 1
					if tempPrevLogIndex == -1 {
						//即此时我们刚刚启动，log中为空的情况
						//我们令AppendEntries RPC中的PrevLogIndex和PrevLogTerm全部为-1
						tempPrevLogTerm = -1
					} else if tempPrevLogIndex == rf.lastIncludedIndex {
						tempPrevLogTerm = rf.lastIncludedTerm
					} else {

						tempPrevLogTerm = rf.logTerm[tempPrevLogIndex-(rf.lastIncludedIndex+1)]
					}
					DPrintf("%v's tempPrevLogIndex = %v, tempPrevLogTerm = %v, leader(me:%v).lastIncludedIndex = %v, lastIncludedTerm = %v", idx, tempPrevLogIndex, tempPrevLogTerm, rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm)
					args := &AppendEntriesArgs{Term: rf.currentTerm, LeaderID: rf.me,
						PrevLogIndex: tempPrevLogIndex, PrevLogTerm: tempPrevLogTerm, LeaderCommit: rf.commitIndex}

					//如果没有Entry需要插入则是普通的heartbeat
					if len(rf.Log)-1-(tempPrevLogIndex-rf.lastIncludedIndex-1) != 0 {
						args.Entries = make([]interface{}, len(rf.Log)-1-(tempPrevLogIndex-(rf.lastIncludedIndex+1)))
						args.LogTerm = make([]int, len(rf.Log)-1-(tempPrevLogIndex-(rf.lastIncludedIndex+1)))
						copy(args.Entries, rf.Log[rf.nextIndex[idx]-(rf.lastIncludedIndex+1):])
						copy(args.LogTerm, rf.logTerm[rf.nextIndex[idx]-(rf.lastIncludedIndex+1):])
					}

					//注意AppendCommand函数执行时未必在无锁环境！
					go AppendCommand(rf, port, idx, args)
					DPrintf("leader %v send append to %d over", rf.me, idx)
				}
				//StartSendingAppend(rf, command, index)
			}
		}
		//BUG_WARNING
		//这里，我们维护了一个logAccept数组，该数组保存每条log被提交的次数。
		//后续，我们需要利用logAccept数组来更新commitIndex，
		//具体来说，根据每个peer的matchIndex来更新leader的commitIndex，
		//即论文fig2中rules for leaders最后一条
		//注意，这里遇到leader的时候直接跳过，但leader的票数
		logAccept := make([]int, len(rf.Log)-1+(rf.lastIncludedIndex+1)-rf.commitIndex)
		//for i := 0; i < len(rf.peers); i++ {
		//	if i == rf.me {
		//		continue
		//	}
		//	for j := 0; j <= rf.matchIndex[i]; j++ {
		//		logAccept[j]++
		//	}
		//}
		//
		////OPTIMIZATION_WARNING  可以优化以下查找，遇到不符合的term结束
		//for i := len(rf.Log) - 1 + (rf.lastIncludedIndex + 1); i >= 0; i-- {
		//	//从后往前找
		//	if logAccept[i]+1 >= (len(rf.peers)+2)/2 && rf.logTerm[i-(rf.lastIncludedIndex+1)] == rf.currentTerm {
		//		//+2是因为要满足majority， +1是因为leader自己也算1票，前面直接跳过了
		//		//这里我们必须要判断当前遍历到的log entry的term是不是当前leader的term，
		//		//符合论文fig2中rules fo r leaders最后一条
		//		rf.commitIndex = i
		//		break
		//	}
		//}

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			for j := rf.commitIndex + 1; j <= rf.matchIndex[i]; j++ {
				logAccept[j-rf.commitIndex-1]++
			}
		}

		for i := len(logAccept) + rf.commitIndex; i > rf.commitIndex; i-- {
			//从后往前找
			DPrintf("i = %v, lastinludedindex = %v", i, rf.lastIncludedIndex)
			if logAccept[i-rf.commitIndex-1]+1 >= (len(rf.peers)+2)/2 && rf.logTerm[i-(rf.lastIncludedIndex+1)] == rf.currentTerm {
				//+2是因为要满足majority， +1是因为leader自己也算1票，前面直接跳过了
				//这里我们必须要判断当前遍历到的log entry的term是不是当前leader的term，
				//符合论文fig2中rules fo r leaders最后一条
				rf.commitIndex = Max(rf.commitIndex, i)
				break
			}
		}
		DPrintf("CommitIndex = %v, leader(%v)", rf.commitIndex, rf.me)
		//此处用于apply
		//OPTIMIZATION_WARNING  可以优化提交，单起一个go routine
		//DPrintf("current commit:%v\n", rf.commitIndex)
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			amsg := ApplyMsg{CommandValid: true, Command: rf.Log[i-(rf.lastIncludedIndex+1)], CommandIndex: i + 1}
			if rf.Log[i-(rf.lastIncludedIndex+1)] != nil {

				DPrintln("Leader commit: ", "me: ", rf.me, " ", amsg, "commitIndex = ", rf.commitIndex, "lastapplied = ", rf.lastApplied) // ， "Log= ", rf.Log, "logterm = ", rf.logTerm)
				rf.lastApplied++
				// rf.Mu.Unlock()
				//rf.applyCh <- amsg
				applyQueue = append(applyQueue, amsg)
				// rf.Mu.Lock()
			} else {
				panic("")
			}

		}
		rf.Mu.Unlock()
		for _, amsg := range applyQueue {
			rf.applyCh <- amsg
		}

		time.Sleep(time.Duration(HEATBEATINTERVAL) * time.Millisecond)
	}
}

// 这个结构体主要是为了统计票数决定的，由于多个协程可以操纵票数，因此需要mu保护count和vote
// 而条件变量cond，主要是为了阻塞candidate主协程，等待投票完成
type CandidateGetVoting struct {
	Mu         sync.Mutex
	count      int
	vote       int
	cond       *sync.Cond
	notTimeOut bool
}

func SendingRequest(args *RequestVoteArgs, port *labrpc.ClientEnd, candiargs *CandidateGetVoting, rf *Raft, peersidx int) {
	reply := &RequestVoteReply{}
	ok := port.Call("Raft.RequestVote", args, reply)
	if ok {
		candiargs.Mu.Lock()
		rf.Mu.Lock()
		if rf.currentTerm < reply.Term || reply.Term > args.Term {
			//rpc成功，但是candidate的term太旧了。转follower
			rf.currentTerm = Max(reply.Term, rf.currentTerm)
			rf.state = FOLLOWER
			rf.lastVisitTime = time.Now()
			//rf.Mu.Unlock()
			//candiargs.Mu.Lock()
			candiargs.notTimeOut = false
			//candiargs.Mu.Unlock()
		} else if reply.VoteGranted && rf.currentTerm == reply.Term && rf.currentTerm == args.Term {
			//必须返回成功，并且给你投票的server的term必须要与你当前的term一致
			DPrintf("%v Get vote from %v, myterm = %v, voteterm=%v\n", rf.me, peersidx, rf.currentTerm, reply.Term)

			candiargs.vote++
			candiargs.count++
			candiargs.cond.Broadcast()
		} else {
			//panic("It seems that some logic went wrong, please debug function: SendingRequest!!!!fx")
		}
		rf.Mu.Unlock()
		candiargs.Mu.Unlock()
	} else {
		//panic("")
	}

}

// type CandidateReturn struct {
// 	Mu sync.Mutex
// 	notTimeOut bool
// }

func (rf *Raft) candidate() {
	rf.lastVisitTime = time.Now()
	rf.currentTerm++
	DPrintf("%v is candidate now, term = %v", rf.me, rf.currentTerm)
	//这里可能有大问题（事实证明问题不大），先发出了requestote请求，而后再开始计时
	rf.electionnotTimeOut = getRandomElectionnotTimeOut()
	// DPrintln("notTimeOut:", rf.electionnotTimeOut)
	candiargs := &CandidateGetVoting{count: 1, vote: 1, notTimeOut: true}
	candiargs.cond = sync.NewCond(&candiargs.Mu)
	termSendingVoteRequest := rf.currentTerm
	rf.VoteFor = rf.me

	// tempLastLogIndex和tempLastLogTerm是为了进行投票时update检查
	tempLastLogIndex := len(rf.Log) - 1 + (rf.lastIncludedIndex + 1)
	var tempLastLogTerm int
	if tempLastLogIndex < 0 {
		tempLastLogTerm = -200
	} else if tempLastLogIndex == rf.lastIncludedIndex {
		tempLastLogTerm = rf.lastIncludedTerm
	} else {
		tempLastLogTerm = rf.logTerm[tempLastLogIndex-(rf.lastIncludedIndex+1)]
	}

	rf.persist()
	rf.Mu.Unlock() //注意在peer函数中没有释放锁，在这里释放

	// 给每个peer发送RequestVote RPC
	for idx, port := range rf.peers {
		if idx == rf.me {
			continue
		}
		args := &RequestVoteArgs{}
		args.Term = termSendingVoteRequest
		args.CandidateID = rf.me
		args.LastLogIndex = tempLastLogIndex
		args.LastLogTerm = tempLastLogTerm
		go SendingRequest(args, port, candiargs, rf, idx)
	}

	// candiReturn := &CandidateReturn{notTimeOut: true}

	go func() {
		// 我们需要让candidate主协程等待peers的投票而阻塞，因此我们需要单开一个协程
		// 它用于判断election time是否到期，以及状态是否发生了变化
		// 我们不用关心Term的变化，因为会在收到RPC的时候处理，状态同样如此。
		for {
			time.Sleep(time.Duration(ELECTIONCHECKINTERVAL) * time.Millisecond)
			candiargs.Mu.Lock()
			rf.Mu.Lock()
			if rf.state != CANDIDATE {
				candiargs.cond.Broadcast()
				// 唤醒所有阻塞的SendingRequest协程，通知退出。

				rf.Mu.Unlock()
				candiargs.Mu.Unlock()
				return

			} else if int(time.Now().Sub(rf.lastVisitTime)/time.Millisecond) > rf.electionnotTimeOut {
				//election timeout到期了
				candiargs.notTimeOut = false
				candiargs.cond.Broadcast()
				rf.Mu.Unlock()
				candiargs.Mu.Unlock()
				return
			}
			rf.Mu.Unlock()
			candiargs.Mu.Unlock()
		}
	}()
	totalPeer := len(rf.peers)

	// 这里是6.824 TA给出的推荐写法，也是go语言条件变量的标准用法
	// 具体来说，我们要将锁和条件变量绑定，然后在循环外上锁，而后写循环，循环内容为条件变量的条件
	// 在循环内我们需要使用wait()方法。每一次外部激活一下条件变量，循环就会被执行一次。
	// 固定写法：
	// 		c.L.Lock()
	// 		for !condition() {
	//        	c.Wait()
	// 		}
	// 		... make use of condition ...
	// 		c.L.Unlock()
	candiargs.Mu.Lock()
	for candiargs.vote < (totalPeer+2)/2 && candiargs.count != totalPeer && candiargs.notTimeOut {
		// 当票数没满  并且   没超时    并且   没有得到所有回复时

		// 注意，这里我们设计了需要等待得到所有回复了才有可能退出
		rf.Mu.Lock()
		if rf.state != CANDIDATE {
			//变成了follower，解锁，candidate主协程返回
			rf.Mu.Unlock()
			candiargs.Mu.Unlock()
			return
		}
		rf.Mu.Unlock()
		candiargs.cond.Wait()

	}

	// 注意此时candiargs的锁还没有被放掉！
	if candiargs.count >= (totalPeer+2)/2 {
		// 当选
		DPrintf("%v:I became Leader!term = %v, mylog = %v, logterm = %v\n", rf.me, rf.currentTerm, rf.Log, rf.logTerm)
		rf.state = LEADER
		candiargs.Mu.Unlock()
		return
		//需要发送heartbeat，在leader部分处理
	} else if candiargs.count == totalPeer {
		// 未当选，但是得到了所有的回复
		println("lost balloting, me:", rf.me, "term: ", rf.currentTerm)
		// rf.state = FOLLOWER

	} else {
		//超时
	}

	candiargs.Mu.Unlock()

}

func peer(rf *Raft) {
	for {
		rf.Mu.Lock()
		if rf.killed() {
			rf.Mu.Unlock()
			return
		} else {
			//DPrintf("peer %v, state: %v, termL %v\n", rf.me, rf.state, rf.currentTerm)
			if rf.state == FOLLOWER {
				rf.follower()
			} else if rf.state == LEADER {
				rf.leader()
			} else {
				rf.candidate()
			}
		}
	}
}

func monitor(rf *Raft) {
	for {
		rf.Mu.Lock()
		DPrintf("%v state == %v\n", rf.me, rf.state)
		rf.Mu.Unlock()
		time.Sleep(time.Duration(100) * time.Millisecond)
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.snapshot = nil
	rf.lastVisitTime = time.Now()
	rf.electionnotTimeOut = getRandomElectionnotTimeOut()
	rf.state = FOLLOWER
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.commitIndex = -1
	rf.applyCh = applyCh
	rf.lastApplied = -1
	rf.VoteFor = -1
	rf.currentTerm = 0
	rf.lastIncludedIndex = -1
	rf.lastIncludedTerm = -1

	rf.readPersist(persister.ReadRaftState())
	if rf.snapshot != nil {
		asmg := ApplyMsg{
			CommandValid: false,
			Command:      nil,
			CommandIndex: 0,
			Snapshot:     rf.snapshot}
		rf.applyCh <- asmg
	}
	DPrintf("peer %v is created! currentTerm = %v, VoteFor = %v, Log = %v, lastincludedindex = %v, lastincludedTerm = %v\n", me, rf.currentTerm, rf.VoteFor, rf.Log, rf.lastIncludedIndex, rf.lastIncludedTerm)
	go peer(rf)
	//go monitor(rf)

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash

	return rf
}

//func (rf *Raft) DiscardLogEntry(index int) {
//	rf.Mu.Lock()
//	rf.Log = rf.Log[index:]
//	rf.logTerm = rf.logTerm[index]
//	rf.persist()
//	rf.Mu.Unlock()
//}

func (rf *Raft) ApplySnapshot(snapshot []byte, index int) {
	//panic("")
	DPrintf("Snapshot from server in raft!, me = %v", rf.me)
	rf.Mu.Lock()
	if index <= rf.lastIncludedIndex {
		DPrintf("index = %v, rf.lastIncludedIndex = %v", index, rf.lastIncludedIndex)
		rf.Mu.Lock()
		return
	}
	DPrintf("Snapshot from server2222 in raft!, me = %v", rf.me)
	//var db map[string]string
	//db = make(map[string]string)
	rf.snapshot = snapshot
	//r := bytes.NewBuffer(snapshot)
	//d := labgob.NewDecoder(r)
	//
	//err := d.Decode(&db)
	//if err != nil {
	//	panic(err)
	//}

	//DPrintf("dababase:%v", db)
	//panic("")
	DPrintf("Snapshot 3333 from server in raft lastIncludedTerm = %d, index = %d", rf.lastIncludedIndex, index)
	DPrintf("leader's log before snapshot: %v, %v", rf.Log, rf.logTerm)
	rf.lastIncludedTerm = rf.logTerm[index-(rf.lastIncludedIndex+1)]
	tempNewLog := make([]interface{}, len(rf.Log[index+1-(rf.lastIncludedIndex+1):]))
	tempNewTerm := make([]int, len(rf.logTerm[index+1-(rf.lastIncludedIndex+1):]))
	copy(tempNewLog, rf.Log[index+1-(rf.lastIncludedIndex+1):])
	copy(tempNewTerm, rf.logTerm[index+1-(rf.lastIncludedIndex+1):])
	rf.Log = make([]interface{}, len(tempNewLog))
	rf.logTerm = make([]int, len(tempNewTerm))
	DPrintf("leader's log after snapshot: %v, %v", rf.Log, rf.logTerm)
	copy(rf.Log, tempNewLog)
	copy(rf.logTerm, tempNewTerm)
	rf.lastIncludedIndex = index
	rf.lastApplied = Max(rf.lastApplied, index)
	//for i := 0; i < len(rf.peers); i++ {
	//	rf.nextIndex[i] = Max(rf.nextIndex[i], rf.lastIncludedIndex+1)
	//}

	rf.persist()

	rf.Mu.Unlock()
	DPrintf("Snapshot from server over in raft!, me = %v, state = %v", rf.me, rf.state)
}

func SendingInstallSnapshot(rf *Raft, args *InstallSnapshotArgs, port *labrpc.ClientEnd, peeridx int) {
	// panic("")
	reply := new(InstallSnapshotReply)
	ok := port.Call("Raft.InstallSnapshot", args, reply)
	//rf.Mu.Lock()
	if ok {
		DPrintf("InstallSnapshot reply = %v, %v", reply, rf.currentTerm)
		//panic("")
		if rf.currentTerm < reply.Term || reply.Term > args.Term {
			//rpc成功，但是leader的term太旧了。转follower
			rf.currentTerm = Max(reply.Term, rf.currentTerm)
			rf.state = FOLLOWER
			rf.lastVisitTime = time.Now()

			rf.persist()
			//rf.Mu.Unlock()
			return
		}

		//rf.nextIndex[peeridx] = Min(args.LastIncludedIndex+1, rf.nextIndex[peeridx])
		rf.matchIndex[peeridx] = Max(rf.matchIndex[peeridx], args.LastIncludedIndex)
		rf.nextIndex[peeridx] = Max(args.LastIncludedIndex+1, rf.nextIndex[peeridx])

	}
	rf.persist()
	//rf.Mu.Unlock()
}

// ApplySnapShotAmongPeers 应该在外部上锁
func ApplySnapShotAmongPeers(rf *Raft, args *InstallSnapshotArgs) {

	//panic("")
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	}
	//var db map[string]string
	//db = make(map[string]string)
	//rf.snapshot = snapshot
	//r := bytes.NewBuffer(snapshot)
	//d := labgob.NewDecoder(r)
	//
	//err := d.Decode(&db)
	//if err != nil {
	//	panic(err)
	//}

	//DPrintf("dababase:%v", db)

	//for i := 0; i < len(rf.peers); i++ {
	//	rf.nextIndex[i] = Max(rf.nextIndex[i], rf.lastIncludedIndex+1)
	//}
	// panic("")
	if args.LastIncludedIndex > rf.lastIncludedIndex+len(rf.Log) {
		rf.Log = nil
		rf.logTerm = nil
		// panic("")
	} else {
		tempNewLog := make([]interface{}, len(rf.Log[args.LastIncludedIndex+1-(rf.lastIncludedIndex+1):]))
		tempNewTerm := make([]int, len(rf.logTerm[args.LastIncludedIndex+1-(rf.lastIncludedIndex+1):]))
		copy(tempNewLog, rf.Log[args.LastIncludedIndex+1-(rf.lastIncludedIndex+1):])
		copy(tempNewTerm, rf.logTerm[args.LastIncludedIndex+1-(rf.lastIncludedIndex+1):])
		rf.Log = make([]interface{}, len(tempNewLog))
		rf.logTerm = make([]int, len(tempNewTerm))
		copy(rf.Log, tempNewLog)
		copy(rf.logTerm, tempNewTerm)

		//rf.Log = rf.Log[args.LastIncludedIndex+1-(rf.lastIncludedIndex+1):]
		//rf.logTerm = rf.logTerm[args.LastIncludedIndex+1-(rf.lastIncludedIndex+1):]
		// panic("")
	}
	rf.snapshot = args.Data

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.lastApplied = Max(rf.lastApplied, args.LastIncludedIndex)
	rf.commitIndex = Max(rf.commitIndex, args.LastIncludedIndex)
	amsg := ApplyMsg{CommandValid: false, Snapshot: args.Data}
	rf.Mu.Unlock()
	rf.applyCh <- amsg
	rf.Mu.Lock()
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// 这里是从AppendEntries借鉴而来
	//panic("")
	if args == nil {
		return
	}
	rf.Mu.Lock()
	reply.Term = rf.currentTerm
	// panic("")
	//follower收到InstallSnapshot的情况
	if rf.state == FOLLOWER {
		if rf.currentTerm <= args.Term && rf.VoteFor == args.LeaderID {
			//这是CurrentTerm < args.Term的情况，也是大部分情况下应该适用的情况
			//即leader给follower插入新的log entries，或者是heartbeat
			rf.currentTerm = args.Term
			rf.lastVisitTime = time.Now()
			ApplySnapShotAmongPeers(rf, args)
		}

	} else if rf.state == CANDIDATE {
		if rf.currentTerm < args.Term {
			//发现了比自己term更大的term，接受新的leader
			rf.VoteFor = args.LeaderID
			rf.lastVisitTime = time.Now()
			rf.currentTerm = args.Term
			rf.state = FOLLOWER
			ApplySnapShotAmongPeers(rf, args)

			//rf.persist()

			//DPrintln("S5")
		} else { //rf.currentTerm == args.Term
			//接受新的leader，这里也应该是常规情况
			//当前candidate和当前leader同时竞选leader，当前leader获胜，因此candidate变回follower
			rf.lastVisitTime = time.Now()
			rf.VoteFor = args.LeaderID
			rf.state = FOLLOWER
			ApplySnapShotAmongPeers(rf, args)
		}

	} else { //leader
		if rf.currentTerm < args.Term {
			//说明当前的leader过期了，可能发生在一个network partition的情况下
			rf.VoteFor = args.LeaderID
			rf.lastVisitTime = time.Now()
			rf.currentTerm = args.Term
			rf.state = FOLLOWER
			ApplySnapShotAmongPeers(rf, args)
			//rf.persist()
			//DPrintln("S8")
		} else if rf.currentTerm == args.Term {
			//这是永远不可能发生的情况，一个term内不可能会有两个合法leader！出现Bug
			panic("leader error")
			//DPrintf("impossible situation!!!!!\n i am %v, %v, from leader %v\n", rf.me, rf.state, args.LeaderID)
		}

	}
	//DPrintf("%v(%v) receive append from %v(%v), entry = %v, sucess = %v\n", rf.me, rf.currentTerm, args.LeaderID, args.Term, args.Entries, reply.Success)

	//更改了VoteFor，log等信息，所以需要持久化
	rf.persist()
	rf.Mu.Unlock()
}
