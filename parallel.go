package parallel

import (
	"fmt"
	"sync"
)

type Worker func(input interface{}) (interface{}, error)

type TaskError struct {
	Input interface{}
	Error error
}

func (t *TaskError) String() string {
	return fmt.Sprintf("%s %v", t.Input, t.Error)
}

func PrintTaskErrors(errs []TaskError) {
	for _, err := range errs {
		fmt.Println(err.String())
	}
}

type TaskChannel struct {
	taskChan   chan interface{}
	resultChan chan interface{}
	errorChan  chan TaskError
}

func NewTaskChannel(tc chan interface{}, rc chan interface{}, ec chan TaskError) TaskChannel {
	return TaskChannel{taskChan: tc, resultChan: rc, errorChan: ec}
}

func (tc *TaskChannel) runInWorker(w Worker) {
	for {
		input, ok := <-tc.taskChan
		if !ok {
			break // quit when finished tasks
		}

		result, err := w(input)

		if err != nil {
			if tc.errorChan != nil {
				taskErr := TaskError{Input: input, Error: err}
				tc.errorChan <- taskErr
			}
		} else {
			tc.resultChan <- result
		}
	}
}

func (tc *TaskChannel) asyncCollectResults(results *[]interface{}, errs *[]TaskError) {
	// collect results
	go func() {
		for {
			result, ok := <-tc.resultChan
			if !ok {
				break
			}
			*results = append(*results, result)
		}
	}()

	// collect errors
	go func() {
		for {
			retErr, ok := <-tc.errorChan
			if !ok {
				break
			}
			*errs = append(*errs, retErr)
		}
	}()
}

func ParallelRun(limit int, inputs []interface{}, w Worker) ([]interface{}, []TaskError) {
	count := len(inputs)

	if limit < 1 || limit > count {
		limit = count
	}

	taskChan := make(chan interface{}, count)
	resultChan := make(chan interface{}, count)
	errorChan := make(chan TaskError, count)

	// dispatch tasks
	go func() {
		for _, input := range inputs {
			taskChan <- input
		}
		close(taskChan)
	}()

	tc := NewTaskChannel(taskChan, resultChan, errorChan)

	// schedule goroutines to collect results and errors
	results := make([]interface{}, 0, count)
	errs := make([]TaskError, 0)
	tc.asyncCollectResults(&results, &errs)

	// start #limit workers
	var taskWG sync.WaitGroup
	for i := 0; i < limit; i++ {
		// id := i
		taskWG.Add(1)
		go func() {
			tc.runInWorker(w)
			// fmt.Printf("(%d) Done!\n", id)
			taskWG.Done()
		}()
	}
	taskWG.Wait()

	// after below lines resultChan and errorChan will be closed.
	// when other goroutines write to these channel it would be crashed
	// and it should be crashed. because till to this line, sub-goroutines should
	// have finished running tasks.
	close(resultChan)
	close(errorChan)

	return results, errs
}
