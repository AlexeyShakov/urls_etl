package domain

import (
	"context"
	"sync"
	"testing"
	"time"
)

type fakePipelineRepo struct {
	mu sync.Mutex

	pipelines    []Pipeline
	tasks        []PipelineTask
	stageResults []StageResult

	nextPipelineID int64
	nextTaskID     int64

	savePipelineCalls     int
	savePipelineTaskCalls int
	saveStageResultCalls  int

	savePipelineErrors     []error
	savePipelineTaskErrors []error
	saveStageResultErrors  []error
}

func newFakePipelineRepo() *fakePipelineRepo {
	return &fakePipelineRepo{
		nextPipelineID: 1,
		nextTaskID:     1,
	}
}

func (r *fakePipelineRepo) SavePipeline(ctx context.Context, pipeline Pipeline) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.savePipelineCalls++

	if err := popError(&r.savePipelineErrors); err != nil {
		return 0, err
	}

	id := r.nextPipelineID
	r.nextPipelineID++

	pipeline.ID = id
	r.pipelines = append(r.pipelines, pipeline)

	return id, nil
}

func (r *fakePipelineRepo) SavePipelineTask(ctx context.Context, task PipelineTask) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.savePipelineTaskCalls++

	if err := popError(&r.savePipelineTaskErrors); err != nil {
		return 0, err
	}

	id := r.nextTaskID
	r.nextTaskID++

	task.ID = id
	r.tasks = append(r.tasks, task)

	return id, nil
}

func (r *fakePipelineRepo) SaveStageResult(ctx context.Context, result StageResult) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.saveStageResultCalls++

	if err := popError(&r.saveStageResultErrors); err != nil {
		return err
	}

	r.stageResults = append(r.stageResults, result)

	return nil
}

type fakeRequester struct {
	mu    sync.Mutex
	calls []PipelineData

	responses []ResponseData
}

func (r *fakeRequester) Do(ctx context.Context, pipelineData PipelineData) ResponseData {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.calls = append(r.calls, pipelineData)

	if len(r.responses) > 0 {
		resp := r.responses[0]
		r.responses = r.responses[1:]
		return resp
	}

	return ResponseData{
		URL:        pipelineData.Request.URL,
		StatusCode: 200,
		Body:       `{"item_ids":[1,2,3]}`,
		Err:        nil,
	}
}

func popError(errorsList *[]error) error {
	if len(*errorsList) == 0 {
		return nil
	}

	err := (*errorsList)[0]
	*errorsList = (*errorsList)[1:]

	return err
}

func waitWithTimeout(t *testing.T, wg *sync.WaitGroup, timeout time.Duration) {
	t.Helper()

	done := make(chan struct{})

	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return
	case <-time.After(timeout):
		t.Fatal("workers did not stop before timeout")
	}
}

func testURLs() []RequestData {
	return []RequestData{
		{
			URL: "http://localhost:8080/getItems",
			Headers: map[string]string{
				"Content-Type": "application/json",
			},
			Payload: `{"user_id":1}`,
		},
		{
			URL: "http://localhost:8080/getItems",
			Headers: map[string]string{
				"Content-Type": "application/json",
			},
			Payload: `{"user_id":2}`,
		},
	}
}
