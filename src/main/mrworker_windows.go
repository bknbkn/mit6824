package main

//
// start a worker process, which is implemented
// in ../mr/worker.go. typically there will be
// multiple worker processes, talking to one coordinator.
//
// go run mrworker.go wc.so
//
// Please do not change this file.
//

import (
	"6.824/mr"
	"strconv"
	"strings"
	"unicode"
)

func main() {
	//if len(os.Args) != 2 {
	//	fmt.Fprintf(os.Stderr, "Usage: mrworker xxx.so\n")
	//	os.Exit(1)
	//}
	////runtime.GOMAXPROCS(1)
	//log.Printf("cpu: %v", runtime.NumCPU())
	//mapf, reducef := loadPlugin(os.Args[1])
	mapf := func(filename string, contents string) []mr.KeyValue {
		// function to detect word separators.
		ff := func(r rune) bool { return !unicode.IsLetter(r) }

		// split contents into an array of words.
		words := strings.FieldsFunc(contents, ff)

		kva := []mr.KeyValue{}
		for _, w := range words {
			kv := mr.KeyValue{w, "1"}
			kva = append(kva, kv)
		}
		return kva
	}

	reducef := func(key string, values []string) string {
		// return the number of occurrences of this word.
		return strconv.Itoa(len(values))
	}
	mr.Worker(mapf, reducef)
}

//
// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
//
//func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
//	p, err := plugin.Open(filename)
//	if err != nil {
//		log.Fatalf("cannot load plugin %v", filename)
//	}
//	xmapf, err := p.Lookup("Map")
//	if err != nil {
//		log.Fatalf("cannot find Map in %v", filename)
//	}
//	mapf := xmapf.(func(string, string) []mr.KeyValue)
//	xreducef, err := p.Lookup("Reduce")
//	if err != nil {
//		log.Fatalf("cannot find Reduce in %v", filename)
//	}
//	reducef := xreducef.(func(string, []string) string)
//
//	return mapf, reducef
//}
