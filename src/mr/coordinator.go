package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	MapIdPool    []int
	NMCompleted  int
	MPoolMutex   sync.Mutex
	MDMutex      *sync.Cond
	NMap         int
	FileNamePool []string
	MTimeCost    []time.Time

	ReduceIdPool []int
	RPollMutex   sync.Mutex
	NReduce      int
	NRCompleted  int
	RTimeCost    []time.Time

	WorkId int
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetMapInfo(args *int, reply *MapReplay) error {
	c.MPoolMutex.Lock()
	defer c.MPoolMutex.Unlock()
	for k, v := range c.MapIdPool {
		if v == -1 {
			continue
		} else if v == 0 || (time.Now().Second()-c.MTimeCost[k].Second() >= 10) {
			c.WorkId++
			c.MapIdPool[k] = c.WorkId
			c.MTimeCost[k] = time.Now()
			reply.MapId = k
			reply.Filename = c.FileNamePool[k]
			reply.NReduce = c.NReduce
			reply.WorkId = c.WorkId
			log.Printf("get mapid :%v, workid: %v", k, c.WorkId)
			return nil
		}
	}
	if c.NMCompleted == c.NMap {
		reply.MapId = -2 // all completed
	} else {
		reply.MapId = -1
	}
	return nil
}

func (c *Coordinator) GetReduceInfo(args *int, reply *ReduceReplay) error {
	//c.MDMutex.L.Lock()
	//if c.NMCompleted != c.NMap {
	//	c.MDMutex.Wait()
	//}
	//c.MDMutex.L.Unlock()
	c.RPollMutex.Lock()
	defer c.RPollMutex.Unlock()
	for k, v := range c.ReduceIdPool {
		if v == -1 {
			continue
		} else if v == 0 || (time.Now().Second()-c.RTimeCost[k].Second() >= 10) {
			c.WorkId++
			c.ReduceIdPool[k] = c.WorkId
			c.RTimeCost[k] = time.Now()
			reply.ReduceId = k
			reply.NMap = c.NMap
			reply.WorkId = c.WorkId
			log.Printf("get reduceid :%v, workid: %v", k, c.WorkId)
			return nil
		}
	}
	if c.NRCompleted == c.NReduce {
		reply.ReduceId = -2
	} else {
		reply.ReduceId = -1
	}
	return nil
}

func (c *Coordinator) UpdateMapResult(args *MapMessage, reply *int) error {
	result, mapId, workId := args.Result, args.MapId, args.WorkId
	c.MPoolMutex.Lock()
	defer c.MPoolMutex.Unlock()
	if c.MapIdPool[mapId] == workId {
		if result {
			c.MapIdPool[mapId] = -1 // map successfully
			c.NMCompleted++
			log.Printf("map %v finished, workid is %v", mapId, workId)
		} else {
			c.MapIdPool[mapId] = 0
		}
		//if c.NMCompleted == c.NMap {
		//	c.MDMutex.Broadcast()
		//}
	}
	return nil
}

func (c *Coordinator) UpdateReduceResult(args *ReduceMessage, reply *int) error {
	result, reduceId, workId := args.Result, args.ReduceId, args.WorkId
	c.RPollMutex.Lock()
	defer c.RPollMutex.Unlock()
	if c.ReduceIdPool[reduceId] == workId {
		if result {
			c.ReduceIdPool[reduceId] = -1
			c.NRCompleted++
			log.Printf("reduce %v finished, workid is %v", reduceId, workId)
		} else {
			c.ReduceIdPool[reduceId] = 0
		}
	}
	return nil

}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false
	// Your code here.
	if c.NRCompleted == c.NReduce {
		ret = true
	}
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.NReduce = nReduce
	c.FileNamePool = files
	c.NMap = len(files)

	c.MapIdPool = make([]int, c.NMap)
	c.ReduceIdPool = make([]int, c.NReduce)
	c.MTimeCost = make([]time.Time, c.NMap)
	c.RTimeCost = make([]time.Time, c.NReduce)
	c.MDMutex = sync.NewCond(&sync.Mutex{})
	c.server()
	return &c
}
