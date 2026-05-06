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

type MapTask struct {
	index    int
	fileName string
	//workerId  string
	startTime time.Time // ho tro check neu qua 10s hay gi do se move task do tu Inprogess -> Pending , bat ke sau Worker thuc hien task do call FinishTask()
}
type ReduceTask struct {
	index int
	//workerId  string
	startTime time.Time
}
type Coordinator struct {
	// Your definitions here.
	files       []string
	nReduce     int
	mapTasks    [3]map[int]*MapTask //3 item, each item is a map , map 1 for Pending, map 2 for InProgess, map3 for Done
	reduceTasks [3]map[int]*ReduceTask
	isDone      bool
	//tuong tuong khi 1 task duoc initialized, no se co 1 object MapTask/ReduceTask rieng biet
	// mapTasks co pointer tro den taskObject nay
	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *Args, reply *Reply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.isDone {
		reply.TaskType = "done"
		return nil
	}
	if len(c.mapTasks[0]) != 0 {
		var task *MapTask
		var TaskIndex int
		for idx, t := range c.mapTasks[0] {
			task = t
			TaskIndex = idx
			break
			//chi lay 1 cai
		}
		delete(c.mapTasks[0], TaskIndex) //O(1)
		task.startTime = time.Now()
		c.mapTasks[1][TaskIndex] = task //O(1)

		// make reply to worker
		reply.FileName = task.fileName
		reply.TaskType = "map"
		reply.NReduce = c.nReduce
		reply.TaskIndex = task.index
		fmt.Printf("map task %d\n", task.index)
	} else if len(c.mapTasks[1]) != 0 {
		// Workers have to wait in case of some InProgress tasks are failed
		reply.TaskType = "wait"
		// sau khi hoan thanh phase "map" thi ta chuyen sang phase "reduce". Tai day
		//ta da co  N*M files mr-<mapTaskIndex>-<reduceIndex> , N la so luong file dau vao
		// M la so luong bucket (nReduce)
	} else if len(c.reduceTasks[0]) != 0 {
		var task *ReduceTask
		var TaskIndex int
		for idx, t := range c.reduceTasks[0] {
			task = t
			TaskIndex = idx
			break
		}
		delete(c.reduceTasks[0], TaskIndex) //map in golang -> delete by index take O(1)
		task.startTime = time.Now()
		c.reduceTasks[1][TaskIndex] = task

		reply.NumberMapTasks = len(c.files)
		reply.TaskType = "reduce"
		reply.NReduce = c.nReduce
		reply.TaskIndex = task.index
		fmt.Printf("reduce task %d\n", task.index)
	} else if len(c.reduceTasks[1]) != 0 {
		// Workers have to wait in case of some InProgress tasks are failed
		reply.TaskType = "wait"
	} else {
		c.isDone = true
		reply.TaskType = "done"
	}
	return nil
}

func (c *Coordinator) FinishTask(args *Args, reply *Reply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch args.TaskType {
	case "map":
		index := args.TaskIndex
		if task, exists := c.mapTasks[1][index]; exists {
			delete(c.mapTasks[1], index)
			c.mapTasks[2][index] = task
		} //else if task, exists := c.mapTasks[0][index]; exists {
		// A slow worker may finish after the monitor requeued the task.
		//delete(c.mapTasks[0], index)
		//c.mapTasks[2][index] = task
		//}

	case "reduce":
		index := args.TaskIndex
		if task, exists := c.reduceTasks[1][index]; exists {
			delete(c.reduceTasks[1], index)
			c.reduceTasks[2][index] = task
		} //else if task, exists := c.reduceTasks[0][index]; exists {
		// A slow worker may finish after the monitor requeued the task.
		//delete(c.reduceTasks[0], index)
		//c.reduceTasks[2][index] = task
		//}
		if len(c.reduceTasks[0]) == 0 && len(c.reduceTasks[1]) == 0 {
			c.isDone = true
		}
	}
	return nil
}

// Timeout monitor - periodically checks for timed-out tasks and reassigns them
func (c *Coordinator) monitor() {
	checkInterval := 1 * time.Second
	taskTimeout := 10 * time.Second

	for {
		time.Sleep(checkInterval)
		c.mu.Lock()

		// Check map tasks for timeouts
		for taskIdx, task := range c.mapTasks[1] { // InProgress bucket
			if time.Since(task.startTime) > taskTimeout {
				// Task timed out, move back to Pending
				delete(c.mapTasks[1], taskIdx)
				c.mapTasks[0][taskIdx] = task
			}
		}

		// Check reduce tasks for timeouts
		for taskIdx, task := range c.reduceTasks[1] { // InProgress bucket
			if time.Since(task.startTime) > taskTimeout {
				// Task timed out, move back to Pending
				delete(c.reduceTasks[1], taskIdx)
				c.reduceTasks[0][taskIdx] = task
			}
		}

		c.mu.Unlock()
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
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := false
	if c.isDone {
		return true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.files = files
	c.nReduce = nReduce
	c.isDone = false

	// Initialize map task buckets
	c.mapTasks[0] = make(map[int]*MapTask)
	c.mapTasks[1] = make(map[int]*MapTask)
	c.mapTasks[2] = make(map[int]*MapTask)

	// Initialize reduce task buckets
	c.reduceTasks[0] = make(map[int]*ReduceTask)
	c.reduceTasks[1] = make(map[int]*ReduceTask)
	c.reduceTasks[2] = make(map[int]*ReduceTask)

	// Initialize map tasks in Pending bucket
	for i := 0; i < len(files); i++ {
		mapTask := &MapTask{
			index:    i,
			fileName: files[i],
		}
		c.mapTasks[0][i] = mapTask
	}

	// Initialize reduce tasks  O(M)  M: so luong bucket
	for j := 0; j < nReduce; j++ {
		reduceTask := &ReduceTask{
			index: j,
		}
		c.reduceTasks[0][j] = reduceTask
	}

	c.server()
	go c.monitor()
	return &c
}
