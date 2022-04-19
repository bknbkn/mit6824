package main

//
// a MapReduce pseudo-application that counts the number of times map/reduce
// tasks are run, to test whether jobs are assigned multiple times even when
// there is no failure.
//
// go build -buildmode=plugin crash.go
//

import (
	"6.824/mr"
	"path/filepath"
)
import "math/rand"
import "strings"
import "strconv"
import "time"
import "fmt"
import "os"
import "io/ioutil"

var count int

func Map(filename string, contents string) []mr.KeyValue {
	me := os.Getpid()
	f := fmt.Sprintf("mr-worker-jobcount-%d-%d", me, count)
	count++
	err := ioutil.WriteFile(f, []byte("x"), 0666)
	if err != nil {
		panic(err)
	}
	time.Sleep(time.Duration(2000+rand.Intn(3000)) * time.Millisecond)
	return []mr.KeyValue{mr.KeyValue{"a", "x"}}
}

func Reduce(key string, values []string) string {
	root := "."
	var files []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		files = append(files, path)
		return nil
	})
	if err != nil {
		panic(err)
	}
	//for _, file := range files {
	//	fmt.Println(file)
	//}
	//files, err := ioutil.ReadDir(".")
	//if err != nil {
	//	panic(err)
	//}
	invocations := 0
	for _, f := range files {
		if strings.HasPrefix(f, "mr-worker-jobcount") {
			invocations++
		}
	}
	return strconv.Itoa(invocations)
}
