package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key
type ByKey []KeyValue

// for sorting by key.
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
	//workerID := fmt.Sprintf("worker-%d-%d", os.Getpid(), time.Now().UnixNano())
	//rpc.args.workerID = workerID
	// Your worker implementation here.

	// lien tuc getTask
	for {
		args := Args{}
		reply := Reply{}
		if !call("Coordinator.GetTask", &args, &reply) {
			return
		}

		if reply.TaskType == "map" {
			filename := reply.FileName
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()

			kva := mapf(filename, string(content))
			// chia cac kv trong kva vao NReduce buckets
			buckets := make([][]KeyValue, reply.NReduce)
			for _, kv := range kva {
				r := ihash(kv.Key) % reply.NReduce
				buckets[r] = append(buckets[r], kv)
			}

			//write kva to files mr-<mapTaskIndex>-<reduceIndex>
			for r := 0; r < reply.NReduce; r++ {
				//make tmp file first then rename to the right format mr-mapindex-reduceindex
				// Temp file + atomic rename ⇒ avoid partially written files being read.
				tmp, err := ioutil.TempFile(".", "mr-map-*")
				if err != nil {
					log.Fatalf("cannot create temp file for map task %v reduce bucket %v: %v", reply.TaskIndex, r, err)
				}
				enc := json.NewEncoder(tmp)
				for _, kv := range buckets[r] {
					if err := enc.Encode(&kv); err != nil {
						log.Fatalf("cannot encode intermediate kv for map task %v reduce bucket %v: %v", reply.TaskIndex, r, err)
					}
				}
				if err := tmp.Sync(); err != nil {
					log.Fatalf("cannot sync intermediate file for map task %v reduce bucket %v: %v", reply.TaskIndex, r, err)
				}
				if err := tmp.Close(); err != nil {
					log.Fatalf("cannot close intermediate file for map task %v reduce bucket %v: %v", reply.TaskIndex, r, err)
				}

				oname := fmt.Sprintf("mr-%d-%d", reply.TaskIndex, r)
				if err := os.Rename(tmp.Name(), oname); err != nil {
					log.Fatalf("cannot rename %v to %v: %v", tmp.Name(), oname, err)
				}
			}
			// call FinishTask
			finishArgs := Args{TaskIndex: reply.TaskIndex, TaskType: reply.TaskType}
			finishReply := Reply{}
			call("Coordinator.FinishTask", &finishArgs, &finishReply)

			// sau khi hoan thanh phase "map" thi ta chuyen sang phase "reduce". Tai day
			//ta da co  N*M files mr-<mapTaskIndex>-<reduceIndex> , N la so luong file dau vao
			// M la so luong bucket (NReduce)
		} else if reply.TaskType == "reduce" {
			// reduce task index: la bucket r gom cac key rieng biet cho bucket do
			r := reply.TaskIndex
			intermediate := []KeyValue{}
			for i := 0; i < reply.NumberMapTasks; i++ {
				filename := fmt.Sprintf("mr-%d-%d", i, r)
				file, err := os.Open(filename)
				if err != nil {
					continue
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
				file.Close()
			}

			//sorting intermediate
			sort.Sort(ByKey(intermediate))

			oname := fmt.Sprintf("mr-out-%d", r)

			// write output to a temp file first, then atomically rename
			tmpfile, err := ioutil.TempFile(".", "mr-out-*")
			if err != nil {
				log.Fatalf("cannot create temp file for %v: %v", oname, err)
			}

			// call Reduce on each distinct key in intermediate[],
			// and write the result to the temp file
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

				if _, err := fmt.Fprintf(tmpfile, "%v %v\n", intermediate[i].Key, output); err != nil {
					log.Fatalf("cannot write reduce output for %v: %v", oname, err)
				}

				i = j
			}
			// flush and close, then atomically rename into place
			if err := tmpfile.Sync(); err != nil {
				log.Fatalf("cannot sync reduce output for %v: %v", oname, err)
			}
			if err := tmpfile.Close(); err != nil {
				log.Fatalf("cannot close reduce output for %v: %v", oname, err)
			}
			if err := os.Rename(tmpfile.Name(), oname); err != nil {
				log.Fatalf("cannot rename %v to %v: %v", tmpfile.Name(), oname, err)
			}

			// report completion to coordinator
			finishArgs := Args{TaskIndex: reply.TaskIndex, TaskType: reply.TaskType}
			finishReply := Reply{}
			call("Coordinator.FinishTask", &finishArgs, &finishReply)

		} else if reply.TaskType == "wait" {
			time.Sleep(500 * time.Millisecond)
		} else {
			return
		}
	}
}

// uncomment to send the Example RPC to the coordinator.
// CallExample()

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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
