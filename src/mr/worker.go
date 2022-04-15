package mr

import (
	"bufio"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	tryTime := 10
	for {
		mapReplay, err := GetMapInfo()
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			tryTime--
			if tryTime == 0 {
				return
			}
			continue
		}
		timeCount := time.
		if mapReplay.MapId == -2 {
			break
		} else if mapReplay.MapId == -1 {
			time.Sleep(2 * time.Second)
			continue
		}
		go func() {
			ok := MapProcess(mapf, &mapReplay, &timeCount)
			log.Printf("woker's mapid %v workid %v result %v", mapReplay.MapId, mapReplay.WorkId, ok)
			UpdateMapResult(&MapMessage{Result: ok, MapId: mapReplay.MapId, WorkId: mapReplay.WorkId})
		}()
	}
	tryTime = 10
	for {
		reduceReply, err := GetReduceInfo()
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			tryTime--
			if tryTime == 0 {
				return
			}
			continue
		}
		timeCount := time.Time{}
		if reduceReply.ReduceId == -2 {
			break
		} else if reduceReply.ReduceId == -1 {
			time.Sleep(2 * time.Second)
			continue
		}
		go func() {
			ok := ReduceProcess(reducef, &reduceReply, &timeCount)
			log.Printf("woker's reduceid %v workid %v result %v", reduceReply.ReduceId, reduceReply.WorkId, ok)
			UpdateReduceResult(&ReduceMessage{Result: ok, ReduceId: reduceReply.ReduceId, WorkId: reduceReply.WorkId})
		}()
	}

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()

}

func MapProcess(mapf func(string, string) []KeyValue, mapReplay *MapReplay, timeCount *time.Time) bool {
	var intermediate []KeyValue
	filename := mapReplay.Filename
	file, err := os.Open(filename)
	defer file.Close()
	if err != nil {
		log.Printf("cannot open %v", filename)
		return false
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Printf("cannot read %v", filename)
		return false
	}
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	sort.Sort(ByKey(intermediate))

	oname := "mr-out-" + strconv.Itoa(mapReplay.MapId) + "-"

	fileNameMap := make(map[int]int)
	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		reduceId := ihash(intermediate[i].Key) % mapReplay.NReduce
		openname := oname + strconv.Itoa(reduceId) + ".txt"
		mode := os.O_CREATE
		if _, ok := fileNameMap[reduceId]; ok {
			mode |= os.O_APPEND
			fileNameMap[reduceId] = 1
		} else {
			mode |= os.O_WRONLY
		}
		ofile, err := os.OpenFile(openname, mode, 0666)
		if err != nil {
			log.Println(err)
			ofile.Close()
			return false
		}
		write := bufio.NewWriter(ofile)
		for k := i; k <= j; k++ {
			write.WriteString(fmt.Sprintf("%v %v\n", intermediate[i].Key, 1))
		}
		if time.Now().Second()-timeCount.Second() >= 9 {
			ofile.Close()
			return false
		}
		write.Flush()
		ofile.Close()
		i = j
	}
	return true
}

func ReduceProcess(reducef func(string, []string) string, reduceReplay *ReduceReplay, timeCount *time.Time) bool {
	reduceId := reduceReplay.ReduceId
	var contents string
	for i := 0; i < reduceReplay.NMap; i++ {
		filename := "mr-out-" + strconv.Itoa(i) + "-" + strconv.Itoa(reduceId) + ".txt"
		if _, err := os.Stat(filename); errors.Is(err, os.ErrNotExist) {
			continue
		}
		file, err := os.Open(filename)
		if err != nil {
			log.Printf("cannot open %v", filename)
			file.Close()
			return false
		}
		content, err := ioutil.ReadAll(file)
		file.Close()
		if err != nil {
			log.Printf("cannot read %v", filename)
			return false
		}
		contents += string(content)
	}
	oname := "mr-out-" + strconv.Itoa(reduceId) + ".txt"
	ofile, _ := os.Create(oname)
	defer ofile.Close()
	ff := func(r rune) bool { return !unicode.IsLetter(r) }
	// split contents into an array of words.
	words := strings.FieldsFunc(contents, ff)
	var intermediate []KeyValue
	for _, w := range words {
		kv := KeyValue{w, "1"}
		intermediate = append(intermediate, kv)
	}
	sort.Sort(ByKey(intermediate))
	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		if time.Now().Second()-timeCount.Second() >= 9 {
			return false
		}
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	for i := 0; i < reduceReplay.NMap; i++ {
		filename := "mr-out-" + strconv.Itoa(i) + "-" + strconv.Itoa(reduceId) + ".txt"
		if _, err := os.Stat(filename); errors.Is(err, os.ErrNotExist) {
			continue
		}
		if time.Now().Second()-timeCount.Second() >= 9 {
			return false
		}
		os.Remove(filename)
	}
	return true
}

func GetMapInfo() (MapReplay, error) {
	var reply MapReplay
	var args int

	ok := call("Coordinator.GetMapInfo", &args, &reply)
	if ok {
		return reply, nil
	} else {
		fmt.Printf("call failed!\n")
		return MapReplay{}, errors.New("get map info error")
	}
}

func GetReduceInfo() (ReduceReplay, error) {
	var reply ReduceReplay
	var args int

	ok := call("Coordinator.GetReduceInfo", &args, &reply)
	if ok {
		return reply, nil
	} else {
		fmt.Printf("call failed!\n")
		return ReduceReplay{}, errors.New("get reduce info error")
	}

}

func UpdateMapResult(message *MapMessage) {
	var reply int
	call("Coordinator.UpdateMapResult", message, &reply)
}

func UpdateReduceResult(message *ReduceMessage) {
	var reply int
	call("Coordinator.UpdateReduceResult", message, &reply)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Printf("dialing:", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
