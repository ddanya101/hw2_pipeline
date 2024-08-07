package main

import (
	"sort"
	"strconv"
	"strings"
	"sync"
)

func ExecutePipeline(jobs ...job) {
	wg := &sync.WaitGroup{}

	in := make(chan interface{}, 100)
	for _, jb := range jobs {
		out := make(chan interface{}, 100)
		wg.Add(1)
		go func(jb job, in, out chan interface{}, wg *sync.WaitGroup) {
			defer wg.Done()
			defer close(out)
			jb(in, out)
		}(jb, in, out, wg)
		in = out
	}

	wg.Wait()
}

type Worker struct {
	Number int
	Result string 
}

func startWorkerCrc32(data string, workerNum int, ch chan <-Worker) {
	ch <- Worker{Number: workerNum, Result: DataSignerCrc32(data)}
}

func startSh(data string, out chan interface{}, quotaLimitMd5 chan struct{}, wg *sync.WaitGroup) {
	chanCrc32 := make(chan Worker, 2)
	chanMd5 := make(chan string)

	go func(data string, ch chan <- string) {
		quotaLimitMd5 <- struct{}{}
		ch <- DataSignerMd5(data)
		 <- quotaLimitMd5 
	}(data, chanMd5)

	go startWorkerCrc32(<-chanMd5, 1, chanCrc32)
	go startWorkerCrc32(data, 0, chanCrc32)

	result := "NO_SET"
	if res := <- chanCrc32; res.Number == 0 {
		result = res.Result + "~" + (<- chanCrc32).Result
	} else {
		result = (<- chanCrc32).Result + "~" + res.Result
	}
	
	out <- result
	wg.Done()
}

func SingleHash(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	quotaLimitMd5 := make(chan struct{}, 1)
	for input := range in {
		data := strconv.Itoa(input.(int))
		wg.Add(1)
		go startSh(data, out, quotaLimitMd5, wg)
	}
	wg.Wait()
}

func startMh(step1 string, out chan interface{}, firstWg *sync.WaitGroup) {
	wg := &sync.WaitGroup{}
	chanCrc32 := make(chan Worker, 6)

	for th := 0; th <= 5; th++ {
		wg.Add(1)
		go func(th int, data string, ch chan <- Worker, wg *sync.WaitGroup){
			defer wg.Done()
			res := DataSignerCrc32(strconv.Itoa(th) + step1)
			chanCrc32 <- Worker{Number: th, Result: res}
		}(th, step1, chanCrc32, wg)
	}

	wg.Wait()
	close(chanCrc32)
	result := make([]string, 6)
	for wrkMh := range chanCrc32 {
		result[wrkMh.Number] = wrkMh.Result
	}
	
	out <- strings.Join(result, "")
	firstWg.Done()
}

func MultiHash(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	for input := range in {
		step1 := input.(string)
		wg.Add(1)
		go startMh(step1, out, wg)
	}
	wg.Wait()
}

func CombineResults(in, out chan interface{}) {
	result := []string{}
	for input := range in {
		result = append(result, input.(string))
	}
	sort.Strings(result)
	out <- strings.Join(result, "_")
}