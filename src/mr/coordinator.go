package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type TaskStatus int

const (
	Idle TaskStatus = iota + 1
	Inprogress
	Completed
)

type Task struct {
	id           int
	file         string
	status       TaskStatus
	taskType     TaskType
	reduceBucket *int
}

type Coordinator struct {
	// Your definitions here.
	mu          sync.Mutex
	mapTasks    map[int]*Task
	reduceTasks map[int]*Task
	nReduce     int
}

type TaskType int

const (
	Map = iota + 1
	Reduce
	Wait
	Done
)

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AssignTask(args *TaskRequest, reply *TaskReply) error {

	// lock critical section
	c.mu.Lock()
	defer c.mu.Unlock()

	completedMapTasks := 0
	// Assign only idle tasks
	for taskID, task := range c.mapTasks {
		if task.status == Idle {
			// Generate RPC response
			reply.TaskID = &taskID
			reply.TaskType = task.taskType
			reply.File = &task.file
			reply.NReduce = &c.nReduce
		}
		if task.status == Completed {
			completedMapTasks += 1
		}
	}

	// If maps are assigned but not all are done -> wait
	if completedMapTasks != len(c.mapTasks) {
		reply.TaskType = Wait
	}

	completedReduceTasks := 0
	// If all maps are done, then we can assign reduce tasks
	for taskID, task := range c.reduceTasks {
		if task.status == Idle {
			// Generate RPC response
			reply.TaskID = &taskID
			reply.TaskType = task.taskType
			reply.File = &task.file
			reply.NReduce = &c.nReduce
		}

		if task.status == Completed {
			completedReduceTasks += 1
		}

	}

	// If all maps are done and reduce tasks are still on going workers have to wait
	// If maps are assigned but not all are done -> wait
	if completedReduceTasks != len(c.mapTasks) {
		reply.TaskType = Wait
	}

	// TODO: implement the returning of Done

	return nil
}

func (c *Coordinator) ReportTaskStatus(req *ReportTaskRequest, reply *ReportTaskResponse) error {
	var hashmap map[int]*Task

	switch req.TaskType {
	case Map:
		hashmap = c.mapTasks
	case Reduce:
		hashmap = c.reduceTasks
	}

	hashmap[req.TaskID].status = req.Status

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
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	// Create a list of Map tasks using the files to register as Idle tasks
	for i, file := range files {
		c.mapTasks[i] = &Task{
			id:       i,
			file:     file,
			status:   Idle,
			taskType: Map,
		}

	}
	c.server()
	return &c
}
