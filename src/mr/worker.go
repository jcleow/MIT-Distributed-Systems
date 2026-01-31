package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	CallExample()

	for {
		reply := RequestTask()
		switch reply.TaskType {
		case Map:
			// implement map
			MapFile(reply.File, reply.NReduce, reply.TaskID, mapf)
			reportTaskReq := ReportTaskRequest{reply.TaskID, reply.TaskType, Completed}
			ReportTaskStatus(&reportTaskReq)
		case Reduce:
			ReduceFiles(reply.TaskID, reducef)
			reportTaskReq := ReportTaskRequest{reply.TaskID, reply.TaskType, Completed}
			ReportTaskStatus(&reportTaskReq)
		case Wait:
			time.Sleep(time.Second)
			fmt.Printf("Worker is waiting...\n")
		case Done:
			fmt.Printf("Worker is exiting...\n")
			return
		}

	}

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func RequestTask() TaskReply {
	// Empty struct to capture worker's response
	req := TaskRequest{}

	// Empty struct to capture coordinator's response
	reply := TaskReply{}

	ok := call("Coordinator.AssignTask", &req, &reply)
	if !ok {
		fmt.Printf("Error")
	}

	fmt.Printf("Reply : %+v\n", reply)

	return reply
}

func ReportTaskStatus(req *ReportTaskRequest) error {
	reply := ReportTaskResponse{}

	ok := call("Coordinator.ReportTaskStatus", req, &reply)
	if !ok {
		log.Fatalf("Failed to call ReportTaskStatus")
	}

	return nil

}

func reduce(
	intermediate []KeyValue,
	taskID int,
	reducef func(string, []string) string) string {
	tmpFile, _ := os.CreateTemp("", fmt.Sprintf("temp-mr-out-%d", taskID))
	i := 0
	for i < len(intermediate) {
		j := i + 1

		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	reducedFileName := fmt.Sprintf("mr-out-%d", taskID)
	os.Rename(tmpFile.Name(), reducedFileName)

	return reducedFileName
}

func ReduceFiles(taskID int, reducef func(string, []string) string) error {

	intermediate := []KeyValue{}
	// Createing the intermediate files for the range of filenames provided
	intermediateFiles := fmt.Sprintf("mr-*-%d", taskID)
	filenames, err := filepath.Glob(intermediateFiles)
	if err != nil {
		log.Fatalf("Unable to get all global files\n")
	}

	for _, filename := range filenames {
		fmt.Printf("filename %s to reduce\n", filename)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("Cannot open file %s", filename)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))

	// Perform reduce on each distinct key in intermediate
	reduce(intermediate, taskID, reducef)

	return nil

}

// Map a single file
func MapFile(filename string, nReduce int, taskID int, mapf func(string, string) []KeyValue) error {
	/*
		The worker's map task code will need a way to
		store intermediate key/value pairs in files in a way that
		can be correctly read back during reduce tasks.

		One possibility is to use Go's encoding/json package.
		To write key/value pairs in JSON format to an open file:

		  enc := json.NewEncoder(file)
		  for _, kv := ... {
		    err := enc.Encode(&kv)

		and to read such a file back:

		  dec := json.NewDecoder(file)
		  for {
		    var kv KeyValue
		    if err := dec.Decode(&kv); err != nil {
		      break
		    }
		    kva = append(kva, kv)
		  }
	*/
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open file: %s", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", file)
	}

	// use mapf to map out each word to key value pair
	kva := mapf(filename, string(content))

	// partition into nReduce buckets where each bucket is identified by its key
	buckets := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		bucket := ihash(kv.Key) % nReduce
		buckets[bucket] = append(buckets[bucket], kv)
	}

	var mrFiles []string
	// To store it into an intermediate file
	for y := 0; y < nReduce; y += 1 {
		// using the paper's hint to create a temp file and atomically renaming it once it is completely written
		// 1. Create temp file
		tmpFile, _ := os.CreateTemp("", fmt.Sprintf("mr-tmp-%d", taskID))
		enc := json.NewEncoder(tmpFile)
		// 2. Encode to temp file
		for _, kv := range buckets[y] {
			enc.Encode(&kv)
		}
		// Close after writing 100%
		tmpFile.Close()

		// Atomically rename to final name
		// if mapreduce crashes,  the reduce operations won't find half written files
		mrFileName := fmt.Sprintf("mr-%d-%d", taskID, y)
		os.Rename(tmpFile.Name(), mrFileName)
		mrFiles = append(mrFiles, mrFileName)
	}

	return nil

}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
