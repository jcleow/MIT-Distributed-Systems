package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskStatus int

const (
	Idle TaskStatus = iota + 1
	InProgress
	Completed
)

type TaskType int

const (
	Map TaskType = iota
	Reduce
	Wait
	Done
)

type Task struct {
	id           int
	file         string
	status       TaskStatus
	taskType     TaskType
	timeAssigned time.Time
}

type Coordinator struct {
	// Your definitions here.
	mu                   sync.Mutex
	mapTasks             map[int]*Task
	reduceTasks          map[int]*Task
	nReduce              int
	completedMapTasks    int
	completedReduceTasks int
	totalFiles           int
}

const WORKER_TIMEOUT_IN_SEC = 10

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

	// Assign idle or timed out tasks
	for taskID, task := range c.mapTasks {
		isIdleTask := task.status == Idle
		isTimedOutTask := time.Since(task.timeAssigned) > WORKER_TIMEOUT_IN_SEC*time.Second && task.status == InProgress

		if isTimedOutTask {
			DPrintf("Task timed out!! Reassigning...\n")
		}

		if isIdleTask || isTimedOutTask {
			// Denote the map task to be in progress to prevent race condition
			c.mapTasks[taskID].status = InProgress
			c.mapTasks[taskID].timeAssigned = time.Now()

			// Generate RPC response
			reply.TaskID = taskID
			reply.TaskType = task.taskType
			reply.File = task.file
			reply.NReduce = c.nReduce
			DPrintf("Sending out map task - ID: %d\n", taskID)

			return nil

		}

	}

	// If maps are assigned but not all are done -> wait
	if c.completedMapTasks != len(c.mapTasks) {
		reply.TaskType = Wait
		DPrintf(
			"Maps are assigned but not all are done - completed: %d, total:%d\n",
			c.completedMapTasks,
			len(c.mapTasks),
		)
		return nil
	}

	// If all maps are done, then we can assign reduce tasks
	for taskID, task := range c.reduceTasks {
		isIdleTask := task.status == Idle
		isTimedOutTask := time.Since(task.timeAssigned) > WORKER_TIMEOUT_IN_SEC*time.Second && task.status == InProgress

		if isTimedOutTask {
			DPrintf("Task timed out!! Reassigning...\n")
		}

		if isIdleTask || isTimedOutTask {
			// Denote the map task to be in progress to prevent race condition
			c.reduceTasks[taskID].status = InProgress
			c.reduceTasks[taskID].timeAssigned = time.Now()

			// Generate RPC response
			reply.TaskID = taskID
			reply.TaskType = task.taskType
			reply.File = task.file
			reply.NReduce = c.nReduce

			DPrintf("Sending out reduce tasks \n")
			return nil
		}

	}

	// If all maps are done and reduce tasks are still on going workers have to wait
	// If maps are assigned but not all are done -> wait
	if c.completedReduceTasks != len(c.reduceTasks) {
		reply.TaskType = Wait
		DPrintf("Not all reduce tasks are completed yet\n Total Tasks %d: Tasks Completed %d\n",
			len(c.mapTasks), c.completedReduceTasks)
		return nil
	}

	DPrintf("Sending out done!\n")
	reply.TaskType = Done

	return nil
}

/*
RPC for workers to report the status of their tasks
*/
func (c *Coordinator) ReportTaskStatus(req *ReportTaskRequest, reply *ReportTaskResponse) error {
	// lock up critical section
	c.mu.Lock()
	defer c.mu.Unlock()

	var hashmap map[int]*Task

	switch req.TaskType {
	case Map:
		hashmap = c.mapTasks
	case Reduce:
		hashmap = c.reduceTasks
	case Wait:
	case Done:
	default:
		break
	}

	DPrintf(
		"Report Task Status: %d, Task ID: %d , Task Type:%d \n",
		req.Status, req.TaskID, req.TaskType,
	)

	// Only update the status if it is a Map / Reduce task
	if hashmap != nil {
		// Only count towards completion if transitioning from in progress to completion
		// This prevents slow workers that are still working on an already completed task to issue an increase in completion count
		if req.Status == Completed && hashmap[req.TaskID].status == InProgress {
			if req.TaskType == Map {
				c.completedMapTasks += 1
				DPrintf("Map task %d completed. Total: %d\n",
					req.TaskID, c.completedMapTasks)
			}
			if req.TaskType == Reduce {
				c.completedReduceTasks += 1
				DPrintf("reduce task %d completed. Total: %d\n",
					req.TaskID, c.completedReduceTasks)
			}
			hashmap[req.TaskID].status = Completed
		}

	}

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
	isDone := c.completedMapTasks == c.totalFiles && c.completedReduceTasks == c.nReduce
	// DPrintf("Completed map tasks %d ; total files %d\n", c.completedMapTasks, c.totalFiles)
	// DPrintf("Completed reduce tasks %d ; total reduce %d\n", c.completedReduceTasks, c.nReduce)
	DPrintf("Is Program done? %t...\n", isDone)
	return isDone

}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	// Create a list of Map tasks using the files to register as Idle tasks
	c.mapTasks = make(map[int]*Task)
	c.totalFiles = len(files)

	// Create all the map tasks
	for i, file := range files {
		c.mapTasks[i] = &Task{
			id:       i,
			file:     file,
			status:   Idle,
			taskType: Map,
		}
	}

	// Create all the reduce tasks
	c.nReduce = nReduce
	c.reduceTasks = make(map[int]*Task)
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = &Task{
			id:       i, // this will be the reduce bucket that worker works on
			status:   Idle,
			taskType: Reduce,
		}

	}

	c.server()
	return &c
}
