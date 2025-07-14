package jobqueue

import (
	"context"
	"sync"
)

// DoFunc is a function type that defines the job processing function.
type DoFunc[J, R any] func(ctx context.Context, job *J) (*R, error)

// JobResult represents the result of a job processed by a worker.
// It contains the job, the result of the job, and any error that occurred during processing.
type JobResult[J, R any] struct {
	Job    *J
	Result *R
	Err    error
}

// Launch launches arbitrary number of worker goroutines and which
// receive jobs from the job queue and sends the result to results channel.
func Launch[J, R any](
	ctx context.Context,
	workersCount int,
	queueSize int,
	f DoFunc[J, R],
) (
	jobQueue chan *J,
	resultsChan chan JobResult[J, R],
	cleanup func(),
) {
	jobQueue = make(chan *J, queueSize)
	resultsChan = make(chan JobResult[J, R], queueSize)
	var wg sync.WaitGroup
	wg.Add(workersCount)

	for i := 0; i < workersCount; i++ {
		go func() {
			defer wg.Done()

			for {
				select {
				case job, ok := <-jobQueue:
					if !ok {
						return // Channel closed, exit the goroutine
					}
					result, err := f(ctx, job)
					resultsChan <- JobResult[J, R]{Job: job, Result: result, Err: err}
				case <-ctx.Done():
					return // Context cancelled, exit the goroutine
				}
			}
		}()
	}

	return jobQueue, resultsChan, func() {
		close(jobQueue)
		wg.Wait()
		close(resultsChan)
	}
}
