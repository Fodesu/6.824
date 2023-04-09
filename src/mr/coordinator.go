package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type CoordinatorPharse int

const (
	Maping CoordinatorPharse = iota
	Reducing
	AllDone
)

type Coordinator struct {
	// Your definitions here.
	fileNames         []string
	nReduce           int
	nMap              int
	pharse            CoordinatorPharse
	MapTasks          []Task
	ReduceTasks       []Task
	nextGiveWorkId    int
	Mutex             sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) Choose(args *Task, reply *Task) error {
	c.Mutex.Lock()
	//! 放在开头保证没有workerid为-1的 worker
	if args.Workerid == -1 {
		reply.Workerid = c.nextGiveWorkId
		args.Workerid = c.nextGiveWorkId
		c.nextGiveWorkId++
	}

	if c.pharse == Maping {
		allocate := -1
		for i := 0; i < c.nMap; i++ {
			if c.MapTasks[i].Status == Waiting {
				allocate = i
				break
			}
		}
		if allocate == -1 {
			reply.Status = Waiting
			c.Mutex.Unlock()
		} else {
			c.MapTasks[allocate].Workerid = args.Workerid
			c.MapTasks[allocate].LastTime = time.Now()
			c.MapTasks[allocate].Status = Working
			*reply = c.MapTasks[allocate]
			c.Mutex.Unlock()
			go func() {
				time.Sleep(10 * time.Second)
				c.Mutex.Lock()
				if c.MapTasks[allocate].Status == Working {
					c.MapTasks[allocate].Status = Waiting
				}
				c.Mutex.Unlock()
			}()
		}
	} else if c.pharse == Reducing {
		// fmt.Println("arrow the Reduce Aera!!!")
		// fmt.Println("c.nReduce is : ", c.nReduce)
		allocate := -1
		for i := 0; i < c.nReduce; i++ {
			if c.ReduceTasks[i].Status == Waiting {
				allocate = i
				break
			}
		}
		if allocate == -1 {
			reply.Status = Waiting
			c.Mutex.Unlock()
		} else {
			fmt.Printf("RPC Choose Get ReduceTask of %d\n", allocate)
			c.ReduceTasks[allocate].Workerid = args.Workerid
			c.ReduceTasks[allocate].LastTime = time.Now()
			c.ReduceTasks[allocate].Status = Working
			*reply = c.ReduceTasks[allocate]
			c.Mutex.Unlock()
			go func() {
				time.Sleep(10 * time.Second)
				c.Mutex.Lock()
				if c.ReduceTasks[allocate].Status == Working {
					c.ReduceTasks[allocate].Status = Waiting
				}
				c.Mutex.Unlock()
			}()
		}
	} else if c.pharse == AllDone {
		reply.Status = Done
		c.Mutex.Unlock()
	}
	return nil
}

func (c *Coordinator) ReportMapTask(args *MapTaskJoinArgs, reply *MapTaskJoinReply) error {
	c.Mutex.Lock()
	//! 如果接收到了超过10s 的被认为已经crash的任务报告，就提前返回不接受，注意解锁
	for i := 0; i < c.nMap; i++ {
		if c.MapTasks[i].Id == args.FileId {
			if c.MapTasks[i].Workerid == args.WorkerId {
				c.MapTasks[i].Status = Done
				reply.Accept = true
			} else {
				fmt.Printf("task is not that worker : %v did ", args.WorkerId)
				reply.Accept = false
			}
		}
	}
	c.Mutex.Unlock()
	return nil
}

func (c *Coordinator) ReportReduceTask(args *ReduceTaskJoinArgs, reply *ReduceTaskJoinReply) error {
	c.Mutex.Lock()
	for i := 0; i < c.nReduce; i++ {
		if c.ReduceTasks[i].Id == args.FileId {
			if c.ReduceTasks[i].Workerid == args.WorkerId {
				c.ReduceTasks[i].Status = Done
				reply.Accept = true
			} else {
				fmt.Printf("task is not that worker : %v did ", args.WorkerId)
				reply.Accept = false
			}
		}
	}
	c.Mutex.Unlock()
	return nil
}

func (c *Coordinator) checkTaskHeart() {
	for {
		time.Sleep(2 * time.Second)
		fmt.Printf("checking Task Heart Now Coordinator.Pharse : %v\n", CoordinatorPharse(c.pharse))
		c.Mutex.Lock()
		DownNum := 0
		if c.pharse == Maping {
			for i := 0; i < c.nMap; i++ {
				if c.MapTasks[i].Status == Done {
					DownNum++
				}
			}
			if DownNum == c.nMap {
				c.pharse = Reducing
			}
		} else {
			for i := 0; i < c.nReduce; i++ {
				if c.ReduceTasks[i].Status == Done {
					DownNum++
				}
			}
			if DownNum == c.nReduce {
				c.pharse = AllDone
			}
		}

		c.Mutex.Unlock()
	}
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {

	// Your code here.
	c.Mutex.Lock()
	ret := c.pharse == AllDone
	c.Mutex.Unlock()
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	fmt.Println(files)
	fmt.Println(len(files))
	c := Coordinator{}

	// Your code here.
	c.nextGiveWorkId = 0
	c.fileNames = files
	c.nMap = len(files) //!8
	c.nReduce = nReduce
	c.pharse = Maping
	c.MapTasks = make([]Task, c.nMap+10)
	c.ReduceTasks = make([]Task, c.nReduce+10)
	for i := 0; i < c.nMap; i++ {
		c.MapTasks[i] = Task{
			FileName:   files[i],
			Id:         i,
			Status:     Waiting,
			TypefoTask: MapTask,
			NReduce:    nReduce,
			Workerid:   -1,
		}
	}
	for i := 0; i < c.nReduce; i++ {
		c.ReduceTasks[i] = Task{
			Status:     Waiting,
			FileNum:    c.nMap,
			Id:         i,
			TypefoTask: ReduceTask,
			NReduce:    nReduce,
			Workerid:   -1,
		}
	}
	c.server()
	go c.checkTaskHeart()
	return &c
}
