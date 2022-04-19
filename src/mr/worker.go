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
	tryTime := 5
	timeDuringCall := 1
	//log.Printf("trh %v", runtime.GOMAXPROCS(0))
	for {
		mapReplay, err := GetMapInfo()
		if err != nil {
			time.Sleep(time.Duration(timeDuringCall) * time.Second)
			tryTime--
			if tryTime == 0 {
				log.Println("coordinator GG")
				return
			}
			continue
		}
		//timeCount := time.Now()
		if mapReplay.MapId == -2 {
			break
		} else if mapReplay.MapId == -1 {
			time.Sleep(2 * time.Second)
			//log.Printf("waiting for other map work")
			continue
		}
		func() {
			ok := MapProcess(mapf, &mapReplay)
			log.Printf("woker's mapid %v workid %v result %v", mapReplay.MapId, mapReplay.WorkId, ok)
			UpdateMapResult(&MapMessage{Result: ok, MapId: mapReplay.MapId, WorkId: mapReplay.WorkId})
		}()
		//time.Sleep(5 * time.Second)
	}
	tryTime = 10
	for {
		reduceReply, err := GetReduceInfo()
		if err != nil {
			time.Sleep(time.Duration(timeDuringCall) * time.Second)
			tryTime--
			if tryTime == 0 {
				log.Println("coordinator GG")
				return
			}
			continue
		}
		if reduceReply.ReduceId == -2 {
			break
		} else if reduceReply.ReduceId == -1 {
			time.Sleep(2 * time.Second)
			//log.Printf("waiting for other reduce work")
			continue
		}
		func() {
			ok := ReduceProcess(reducef, &reduceReply)
			log.Printf("woker's reduceid %v workid %v result %v", reduceReply.ReduceId, reduceReply.WorkId, ok)
			UpdateReduceResult(&ReduceMessage{Result: ok, ReduceId: reduceReply.ReduceId, WorkId: reduceReply.WorkId})
		}()
	}

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()

}

func MapProcess(mapf func(string, string) []KeyValue, mapReplay *MapReplay) bool {
	timeCount := time.Now()
	log.Printf("get mapid :%v, workid: %v, filename: %v", mapReplay.MapId, mapReplay.WorkId, mapReplay.Filename)
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

	oname := "mr-" + strconv.Itoa(mapReplay.MapId) + "-"

	fileNameMap := make(map[int]int)
	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		reduceId := ihash(intermediate[i].Key) % mapReplay.NReduce
		openname := oname + strconv.Itoa(reduceId) + ".txt"
		var (
			ofile *os.File
			err   error
		)
		//ofile, err = os.OpenFile(openname, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)

		if _, ok := fileNameMap[reduceId]; ok {
			//log.Printf("old file %v %v %v", reduceId, i, intermediate[i].Key)
			ofile, err = os.OpenFile(openname, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
		} else {
			//log.Printf("new file %v %v %v", reduceId, i, intermediate[i].Key)
			ofile, err = os.Create(openname)
			fileNameMap[reduceId] = 1
		}
		if err != nil {
			log.Println("open file err", err)
			ofile.Close()
			return false
		}
		write := bufio.NewWriter(ofile)
		for k := i; k < j; k++ {
			outString := fmt.Sprintf("%v,%v\n", intermediate[i].Key, intermediate[i].Value)
			//log.Printf("out_string: %v", outString)
			//ofile.WriteString(outString)
			write.WriteString(outString)
		}
		if time.Now().Sub(timeCount) >= time.Second*time.Duration(mapReplay.TimeOutLimit-1) {
			ofile.Close()
			log.Printf("map process timeout")
			return false
		}
		write.Flush()
		ofile.Close()
		i = j
	}
	return true
}

func ReduceProcess(reducef func(string, []string) string, reduceReplay *ReduceReplay) bool {
	timeCount := time.Now()
	reduceId := reduceReplay.ReduceId
	log.Printf("get reduceid :%v, workid: %v", reduceId, reduceReplay.WorkId)
	var contents string
	for i := 0; i < reduceReplay.NMap; i++ {
		filename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reduceId) + ".txt"
		if _, err := os.Stat(filename); errors.Is(err, os.ErrNotExist) {
			continue
		}
		log.Printf("reduce i : %v", i)
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
			file.Close()
			return false
		}
		contents += string(content)
		//log.Printf("content %v ", contents)
	}
	oname := "mr-out-" + strconv.Itoa(reduceId) + ".txt"
	ofile, _ := os.Create(oname)
	defer ofile.Close()
	ff := func(r rune) bool { return unicode.IsSpace(r) }
	// split contents into an array of words.
	words := strings.FieldsFunc(contents, ff)
	var intermediate []KeyValue
	fkv := func(r rune) bool { return r == ',' }
	for _, w := range words {
		tkv := strings.FieldsFunc(w, fkv)
		kv := KeyValue{tkv[0], tkv[1]}
		intermediate = append(intermediate, kv)
	}
	sort.Sort(ByKey(intermediate))
	log.Printf("len : %v", len(intermediate))
	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		//f2, _ := os.Open(".")
		//files2, _ := f2.Readdir(-1)
		//for _, file := range files2 {
		//	fmt.Println(file.Name())
		//}
		//root := "."
		//var files []string
		//err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		//	files = append(files, path)
		//	return nil
		//})
		//if err != nil {
		//	panic(err)
		//}
		//for _, file := range files {
		//	fmt.Println(file)
		//}

		//files, err := ioutil.ReadDir(".")
		//if err != nil {
		//	panic(err)
		//}
		//invocations := 0
		//for _, f := range files {
		//	if strings.HasPrefix(f.Name(), "mr-worker-jobcount") {
		//		invocations++
		//		log.Printf("fname: %v %v %v", f.Name(), reduceReplay.ReduceId, reduceReplay.WorkId)
		//	}
		//}
		//log.Printf("out: %v", invocations)
		output := reducef(intermediate[i].Key, values)
		//log.Printf("out2: %v", output)
		if time.Now().Sub(timeCount) >= time.Second*time.Duration(reduceReplay.TimeOutLimit-1) {
			log.Printf("reduce process timeout")
			return false
		}
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	//for i := 0; i < reduceReplay.NMap; i++ {
	//	filename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reduceId) + ".txt"
	//	if _, err := os.Stat(filename); errors.Is(err, os.ErrNotExist) {
	//		continue
	//	}
	//	if time.Now().Sub(timeCount) >= time.Second*time.Duration(reduceReplay.TimeOutLimit-1) {
	//		log.Printf("reduce process timeout")
	//		return false
	//	}
	//	os.Remove(filename)
	//}
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
		log.Printf("dialing: %v %v", rpcname, err)
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
