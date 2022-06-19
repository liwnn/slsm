package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"runtime/pprof"
	"strconv"
	"strings"
	"time"

	_ "net/http/pprof"

	"github.com/liwnn/slsm"
)

func main() {
	//insertLoopupTest()
	lsm := slsm.NewLSM(800, 20, 1.0, 0.00100, 1024, 20)
	defer lsm.Close()

	fmt.Println("LSM Tree DSL Interactive Mode")
	r := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("> ")
		line, err := r.ReadString('\n')
		if err != nil {
			panic(err)
		}
		line = line[:len(line)-1]
		queryLine(lsm, line)
	}
}

func queryLine(lsm *slsm.LSM, line string) {
	cmds := strings.FieldsFunc(line, func(r rune) bool {
		return r == ' '
	})
	if len(cmds) == 0 {
		return
	}
	switch cmds[0][0] {
	case 'p':
		if len(cmds) != 3 {
			return
		}
		pk, _ := strconv.Atoi(cmds[1])
		v, _ := strconv.Atoi(cmds[2])
		lsm.InsertKey(pk, v)
	case 'g':
		lk, _ := strconv.Atoi(cmds[1])
		v, found := lsm.Lookup(lk)
		if found {
			fmt.Print(v)
		}
		fmt.Println()
	case 'r':
		lk1, _ := strconv.Atoi(cmds[1])
		lk2, _ := strconv.Atoi(cmds[2])
		res := lsm.Range(lk1, lk2)
		for i := 0; i < len(res); i++ {
			fmt.Printf("%v:%v ", res[i].Key, res[i].Value)
		}
		fmt.Println()
	case 'd':
		dk, _ := strconv.Atoi(cmds[1])
		lsm.DeleteKey(dk)
	case 's':
		lsm.PrintStats()
	}
}

func insertLoopupTest() {
	r := rand.NewSource(time.Now().Unix())
	var num_inserts = 1000000
	var num_runs = 20
	var buffer_capacity uint64 = 800
	var bf_fp = .001
	var pageSize uint32 = 512
	var disk_runs_per_level = 20
	var merge_fraction = 1.0
	lsmTree := slsm.NewLSM(buffer_capacity, num_runs, merge_fraction, bf_fp, pageSize, disk_runs_per_level)

	to_insert := make([]int, 0, 10)
	for i := 0; i < num_inserts; i++ {
		to_insert = append(to_insert, int(r.Int63()))
	}

	start := time.Now()
	for i := 0; i < num_inserts; i++ {
		if i%100000 == 0 {
			fmt.Printf("insert %v\n", i)
		}
		lsmTree.InsertKey(to_insert[i], i)
	}
	finish := time.Now()
	total_insert := float64(finish.Unix() - start.Unix())
	total_insert += float64(finish.UnixNano()-start.UnixNano()) / 1000000000.0

	fmt.Printf("Time: %v s\n", total_insert)
	fmt.Printf("Inserts per second: %v s\n", float64(num_inserts)/total_insert)

	println("Starting lookups")
	f, _ := os.Create("cpuprofile")
	pprof.StartCPUProfile(f)
	start = time.Now()
	for i := 0; i < num_inserts; i++ {
		if i%100000 == 0 {
			fmt.Printf("lookup %v\n", i)
		}

		lsmTree.Lookup(to_insert[i])
	}
	finish = time.Now()
	pprof.StopCPUProfile()
	total_lookup := float64(finish.Second() - start.Second())
	total_lookup += float64(finish.Nanosecond()-start.Nanosecond()) / 1000000000.0
	fmt.Printf("Time: %v s\n", total_lookup)
	fmt.Printf("Loopups per second: %v s\n", float64(num_inserts)/total_lookup)
}
