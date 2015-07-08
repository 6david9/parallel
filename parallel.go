package parallel

import (
	"sync"
	"runtime"
)

type Worker func(input interface{}) interface{}
type ResultHandler func(index int, result interface{})

type ParallelChannel struct {
	taskChan      chan interface{}
	outputChan    chan interface{}
	quitChan      chan bool
	parallelCount int
	running       bool
	worker        Worker
	resultHandler ResultHandler
	waitGroup     sync.WaitGroup
}

func NewParallelChannel(count int, worker Worker, handler ResultHandler) ParallelChannel {
	if count == 0 {
		count = 1
	} else if count < 0 {
		count = runtime.NumCPU()
	}
	return ParallelChannel{parallelCount: count, worker: worker, resultHandler: handler, running: false}
}

func (pc *ParallelChannel) AddTask(task interface{}) {
	pc.taskChan <- task
}

func (pc *ParallelChannel) TaskChan() chan interface{} {
	return pc.taskChan
}

func (pc *ParallelChannel) OutputChan() chan interface{} {
	return pc.outputChan
}

func (pc *ParallelChannel) Stop() {
	close(pc.taskChan)
	close(pc.quitChan)
	pc.waitGroup.Wait()
}

func (pc *ParallelChannel) ForceStop() {
	close(pc.taskChan)

	for i := 0; i < pc.parallelCount; i++ {
		pc.quitChan <- true
	}
	close(pc.quitChan)
	pc.waitGroup.Wait()
}

func (pc *ParallelChannel) Run() {
	if pc.running {
		return
	}
	pc.running = true

	pc.taskChan = make(chan interface{}, pc.parallelCount)
	pc.outputChan = make(chan interface{}, pc.parallelCount)
	pc.quitChan = make(chan bool, pc.parallelCount)

	pc.waitGroup.Add(1)
	pc.startHandleResults()

	go func() {
		var wg sync.WaitGroup

		for i := 0; i < pc.parallelCount; i++ {
			wg.Add(1)

			go func() {
			LOOP:
				for {
					select {
					case <-pc.quitChan: // 用户显式的退出
						wg.Done()
						break LOOP

					case task, ok := <-pc.taskChan:
						if !ok { // 没有任务后退出
							wg.Done()
							break LOOP
						}

						result := pc.worker(task)
						pc.outputChan <- result

					default:
						wg.Done()
						break LOOP
					}
				}
			}()
		}
		wg.Wait()

		close(pc.outputChan)

		pc.running = false
		pc.waitGroup.Done()
	}()
}

func (pc *ParallelChannel) startHandleResults() {
	go func() {
		index := 0
		for {
			result, ok := <-pc.outputChan
			if !ok {
				break
			}
			pc.resultHandler(index, result)
			index += 1
		}
	}()
}
