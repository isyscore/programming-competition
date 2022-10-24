package main

import (
	"bufio"
	"fmt"
	f0 "github.com/isyscore/isc-gobase/file"
	. "github.com/isyscore/isc-gobase/isc"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const filename = "./small_int_set"
const indexfile = "./index"

var routineCount int64 = 10

type avgNums struct {
	avg float64
	n   int64
}

func main() {
	if len(os.Args) == 3 {
		if os.Args[1] == "index" {
			rc, _ := strconv.ParseInt(os.Args[2], 10, 64)
			buildIndex(rc)
		}
		return
	}

	if len(os.Args) == 2 {
		routineCount, _ = strconv.ParseInt(os.Args[1], 10, 64)
	}

	useIndex := false
	var indexes []int64

	if f0.FileExists(indexfile) {
		_list := ISCList[string](f0.ReadFileLines(indexfile)).Filter(func(it string) bool { return strings.TrimSpace(it) != "" })
		indexes = ListToMapFrom[string, int64](_list).Map(func(it string) int64 {
			i, _ := strconv.ParseInt(it, 10, 64)
			return i
		})
		useIndex = true
		routineCount = int64(len(indexes)) - 1
	}

	fi, _ := os.Stat(filename)
	totalSize := fi.Size()
	blockSize := int64(math.Ceil(float64(totalSize) / float64(routineCount)))

	_start := time.Now().UnixMilli()

	wg := sync.WaitGroup{}
	ch := make(chan avgNums)
	done := make(chan bool, 1)

	var av = 0.0
	var n = 1.0

	go func(ac *float64, nc *float64) {
		for item := range ch {
			*ac = (n-1)/(n+float64(item.n)-1)*(*ac) + item.avg*(float64(item.n)/(n+float64(item.n)-1))
			*nc += float64(item.n)
		}
		done <- true
	}(&av, &n)

	var current int64 = 0
	var limitSize int64 = 0
	for i := 0; i < int(routineCount); i++ {

		if useIndex {
			limitSize = indexes[i+1] - indexes[i]
		} else {
			limitSize = blockSize
		}
		if limitSize <= 0 {
			break
		}

		wg.Add(1)
		go func(idx int, c int64, l int64) {
			read(idx, c, l, ch)
			wg.Done()
		}(i, current, limitSize)

		// 507229825 / -11135.900658

		if useIndex {
			current = indexes[i+1]
		} else {
			current += blockSize + 1
		}
	}
	wg.Wait()
	close(ch)

	<-done
	close(done)

	_end := time.Now().UnixMilli()
	fmt.Printf("numbers = %d, avg = %f, time = %dms\n", int64(n-1), av/1000000.0, _end-_start)
}

func read(idx int, offset int64, limit int64, ch chan avgNums) {
	av := 0.0
	n := 1.0
	file, _ := os.OpenFile(filename, os.O_RDONLY, os.ModePerm)
	defer func(file *os.File) {
		_ = file.Close()
	}(file)
	_, _ = file.Seek(offset, 0)
	reader := bufio.NewReader(file)
	var cummulativeSize int64 = 0
	for {
		if cummulativeSize >= limit {
			break
		}
		b, _, err := reader.ReadLine()
		if err != nil {
			break
		}
		if len(b) > 0 {
			cummulativeSize += int64(len(b) + 1) // 计算时要加上最后的回车符
			f, err := strconv.ParseFloat(strings.TrimSpace(string(b)), 64)
			if err == nil {
				fmt.Printf("idx = %d, num = %f, cz = %d\n", idx, f, cummulativeSize)
				av = (n-1)/n*av + (f*1000000.0)/n
				n += 1.0
			}
		}
	}
	fmt.Printf("read = %d, avg = %f, n = %d\n", idx, av, int64(n-1))
	ch <- avgNums{avg: av, n: int64(n - 1)}
}

func buildIndex(rc int64) {
	fi, _ := os.Stat(filename)
	totalSize := fi.Size()
	blockSize := int64(math.Ceil(float64(totalSize) / float64(rc)))

	file, _ := os.OpenFile(filename, os.O_RDONLY, os.ModePerm)
	defer func(file *os.File) {
		_ = file.Close()
	}(file)

	var slicePointer ISCList[Pair[int, int64]]
	slicePointer = append(slicePointer, Pair[int, int64]{0, 0})

	current := blockSize

	for i := 1; i < int(rc); i++ {
		_, _ = file.Seek(current, 0)
		reader := bufio.NewReader(file)
		// 读一个字节
		var offset int64 = 0
		hasReturn := false
		for {
			b, err := reader.ReadByte()
			offset++
			if err == nil {
				// 如果读到了
				if b == '\n' {
					// 如果读到一个回车，直接记录上一个的结束位置
					slicePointer = append(slicePointer, Pair[int, int64]{First: i, Second: current + offset})
					hasReturn = true
					break
				}
			} else {
				break
			}
		}
		if !hasReturn {
			// 一行到最后都没读到回车，也算是一行
			slicePointer = append(slicePointer, Pair[int, int64]{First: i, Second: current + offset + 1})
		}
		current += offset + blockSize + 1
		if current >= totalSize {
			break
		}
	}

	slicePointer = append(slicePointer, Pair[int, int64]{First: int(rc), Second: totalSize})

	idxStr := slicePointer.JoinToStringFull("\n", "", "", func(it Pair[int, int64]) string {
		return fmt.Sprintf("%d", it.Second)
	})
	f0.WriteFile(indexfile, idxStr)
}
