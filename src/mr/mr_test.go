//package main
package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"testing"
)

func TestDir(t *testing.T) {
	files, err := ioutil.ReadDir("../main")
	if err != nil {
		panic(err)
	}
	invocations := 0
	for _, f := range files {
		if strings.HasPrefix(f.Name(), "mr-worker-jobcount") {
			invocations++
		}
		log.Printf("fname: %v %v %v", f.Name(), 1, 1)
	}
	log.Printf("out: %v", invocations)

	f2, err := os.Open(".")
	files2, _ := f2.Readdir(-1)
	for _, file := range files2 {
		fmt.Println(file.Name())
	}
}
