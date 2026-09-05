package domain

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"
)

// TestWholePipeline тестируем весь пайплайн, но с fake-сущностями: fake-DB и fake-http
func TestWholePipeline(t *testing.T) {
	//todo 1. создать fake-repository
	// скорее всего тут нужно хранить мапу, куда будем класть данные, чтобы в конце проверить, что данные есть
	//todo 2. создать http ответы

	// ТЕСТ КЕЙСЫ
	// RunPipeline
	// +++1. не смогли сохранить пайплайн в БД, вышли из RunPipeline, должен быть лог
	// +++2. Создали пайплайн не с первого раза дальше успешный пайп
	// +++4. Не смогли сохранить таску в БД
	// +++5. Смогли сохранить таску в БД, но не с первого раза дальше все ок
	// +++7. Запрос во внешний сервис пришел с ошибкой, смогли сохранить в БД
	// +++7. Запрос во внешний сервис пришел с ошибкой, смогли сохранить в БД не с первого раза
	// 8. Запрос во внешний сервис пришел с ошибкой, не смогли сохранить в БД
	// +++10. Запустить два URL успешно
}

// //////////////////////////////////////////
// TestWholePipelineSuccess тестирует успешное прохождение первого этапа pipeline.
//
// Проверяем, что:
//   - pipeline сохраняется в БД;
//   - для каждого URL создается PipelineTask;
//   - каждый URL обрабатывается worker-ом;
//   - для каждой задачи сохраняется StageResult со статусом success.
func TestWholePipelineSuccess(t *testing.T) {
	ctx := context.Background()

	repo := newFakePipelineRepo()
	requester := &fakeRequester{}

	urls := testURLs()

	requestChannel := make(chan PipelineData, 10)
	firstWorkerCh := make(chan PipelineData, 10)
	secondWorkerCh := make(chan PipelineData, 10)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		DispatchRequests(requestChannel, firstWorkerCh, secondWorkerCh)
	}()

	wg.Add(2)
	go func() {
		defer wg.Done()
		RequestWorker(ctx, 1, firstWorkerCh, requester, repo)
	}()

	go func() {
		defer wg.Done()
		RequestWorker(ctx, 2, secondWorkerCh, requester, repo)
	}()

	RunPipeline(ctx, urls, requestChannel, repo)

	wg.Wait()

	if len(repo.pipelines) != 1 {
		t.Fatalf("expected 1 pipeline, got %d", len(repo.pipelines))
	}

	if len(repo.tasks) != len(urls) {
		t.Fatalf("expected %d tasks, got %d", len(urls), len(repo.tasks))
	}

	if len(requester.calls) != len(urls) {
		t.Fatalf("expected %d requester calls, got %d", len(urls), len(requester.calls))
	}

	if len(repo.stageResults) != len(urls) {
		t.Fatalf("expected %d stage results, got %d", len(urls), len(repo.stageResults))
	}

	for _, result := range repo.stageResults {
		if result.Stage != StageGetItems {
			t.Fatalf("expected stage %q, got %q", StageGetItems, result.Stage)
		}

		if result.Status != StatusSuccess {
			t.Fatalf("expected status %q, got %q", StatusSuccess, result.Status)
		}

		if result.Attempt != 1 {
			t.Fatalf("expected attempt 1, got %d", result.Attempt)
		}

		if len(result.Details) == 0 {
			t.Fatal("expected non-empty stage result details")
		}
	}
}

// TestWholePipeline_SavePipelineFailed проверяет сценарий,
// когда pipeline не удалось сохранить в БД.
//
// В этом случае RunPipeline должен завершиться раньше,
// чем задачи попадут в worker-ы.
//
// Проверяем, что:
//   - попытка сохранить pipeline была выполнена;
//   - PipelineTask не создавались;
//   - Requester не вызывался;
//   - StageResult не сохранялись;
//   - worker-ы корректно завершились после закрытия каналов.
func TestWholePipeline_SavePipelineFailed(t *testing.T) {
	ctx := context.Background()

	repo := newFakePipelineRepo()
	repo.savePipelineErrors = []error{ErrStorageUnavailableNonRetryable}

	requester := &fakeRequester{}

	urls := testURLs()
	requestChannel := make(chan PipelineData, 10)
	firstWorkerCh := make(chan PipelineData, 10)
	secondWorkerCh := make(chan PipelineData, 10)

	var wg sync.WaitGroup

	// Запускаем dispatcher так же, как в реальном pipeline.
	// Он будет читать из requestChannel и распределять задачи между worker-ами.
	wg.Add(1)
	go func() {
		defer wg.Done()
		DispatchRequests(requestChannel, firstWorkerCh, secondWorkerCh)
	}()

	// Запускаем оба worker-а, чтобы тест проверял полный flow,
	// а не только изолированную функцию RunPipeline.
	wg.Add(2)
	go func() {
		defer wg.Done()
		RequestWorker(ctx, 1, firstWorkerCh, requester, repo)
	}()

	go func() {
		defer wg.Done()
		RequestWorker(ctx, 2, secondWorkerCh, requester, repo)
	}()

	RunPipeline(ctx, urls, requestChannel, repo)

	waitWithTimeout(t, &wg, time.Second)

	if repo.savePipelineCalls != 1 {
		t.Fatalf("expected SavePipeline to be called once, got %d", repo.savePipelineCalls)
	}

	if len(repo.pipelines) != 0 {
		t.Fatalf("expected no saved pipelines, got %d", len(repo.pipelines))
	}

	if len(repo.tasks) != 0 {
		t.Fatalf("expected no saved tasks, got %d", len(repo.tasks))
	}

	if len(repo.stageResults) != 0 {
		t.Fatalf("expected no saved stage results, got %d", len(repo.stageResults))
	}

	if len(requester.calls) != 0 {
		t.Fatalf("expected requester not to be called, got %d calls", len(requester.calls))
	}
}

// TestWholePipeline_SavePipelineRetrySuccess проверяет сценарий,
// когда первая попытка сохранить pipeline завершается retryable-ошибкой,
// а повторная попытка проходит успешно.
//
// После успешного retry pipeline должен продолжить работу:
//   - создать PipelineTask для каждого URL;
//   - отправить задачи в worker-ы;
//   - выполнить запросы через requester;
//   - сохранить StageResult со статусом success.
func TestWholePipeline_SavePipelineRetrySuccess(t *testing.T) {
	ctx := context.Background()

	repo := newFakePipelineRepo()
	repo.savePipelineErrors = []error{
		ErrStorageUnavailableRetryable,
		nil,
	}

	requester := &fakeRequester{}

	urls := testURLs()
	requestChannel := make(chan PipelineData, 10)
	firstWorkerCh := make(chan PipelineData, 10)
	secondWorkerCh := make(chan PipelineData, 10)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		DispatchRequests(requestChannel, firstWorkerCh, secondWorkerCh)
	}()

	wg.Add(2)
	go func() {
		defer wg.Done()
		RequestWorker(ctx, 1, firstWorkerCh, requester, repo)
	}()

	go func() {
		defer wg.Done()
		RequestWorker(ctx, 2, secondWorkerCh, requester, repo)
	}()

	RunPipeline(ctx, urls, requestChannel, repo)

	waitWithTimeout(t, &wg, time.Second)

	if repo.savePipelineCalls != 2 {
		t.Fatalf("expected SavePipeline to be called twice, got %d", repo.savePipelineCalls)
	}

	if len(repo.pipelines) != 1 {
		t.Fatalf("expected 1 saved pipeline, got %d", len(repo.pipelines))
	}

	if len(repo.tasks) != len(urls) {
		t.Fatalf("expected %d saved tasks, got %d", len(urls), len(repo.tasks))
	}

	if len(requester.calls) != len(urls) {
		t.Fatalf("expected %d requester calls, got %d", len(urls), len(requester.calls))
	}

	if len(repo.stageResults) != len(urls) {
		t.Fatalf("expected %d stage results, got %d", len(urls), len(repo.stageResults))
	}

	for _, result := range repo.stageResults {
		if result.Stage != StageGetItems {
			t.Fatalf("expected stage %q, got %q", StageGetItems, result.Stage)
		}

		if result.Status != StatusSuccess {
			t.Fatalf("expected status %q, got %q", StatusSuccess, result.Status)
		}
	}
}

// TestWholePipeline_SaveTaskFailed проверяет сценарий,
// когда pipeline был успешно создан, но задачи сохранить не удалось.
//
// В этом случае RunPipeline должен пропустить задачи,
// которые не удалось сохранить в БД.
//
// Проверяем, что:
//   - pipeline сохранен;
//   - попытки сохранить PipelineTask были выполнены;
//   - задачи не попали в worker-ы;
//   - Requester не вызывался;
//   - StageResult не сохранялись;
//   - worker-ы корректно завершились после закрытия каналов.
func TestWholePipeline_SaveTaskFailed(t *testing.T) {
	ctx := context.Background()

	repo := newFakePipelineRepo()
	repo.savePipelineTaskErrors = []error{
		ErrStorageUnavailableNonRetryable, // для первого URL
		ErrStorageUnavailableNonRetryable, // для второго URL
	}

	requester := &fakeRequester{}

	urls := testURLs()

	requestChannel := make(chan PipelineData, 10)
	firstWorkerCh := make(chan PipelineData, 10)
	secondWorkerCh := make(chan PipelineData, 10)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		DispatchRequests(requestChannel, firstWorkerCh, secondWorkerCh)
	}()

	wg.Add(2)
	go func() {
		defer wg.Done()
		RequestWorker(ctx, 1, firstWorkerCh, requester, repo)
	}()

	go func() {
		defer wg.Done()
		RequestWorker(ctx, 2, secondWorkerCh, requester, repo)
	}()

	RunPipeline(ctx, urls, requestChannel, repo)

	waitWithTimeout(t, &wg, time.Second)

	if repo.savePipelineCalls != 1 {
		t.Fatalf("expected SavePipeline to be called once, got %d", repo.savePipelineCalls)
	}

	if len(repo.pipelines) != 1 {
		t.Fatalf("expected 1 saved pipeline, got %d", len(repo.pipelines))
	}

	if repo.savePipelineTaskCalls != len(urls) {
		t.Fatalf(
			"expected SavePipelineTask to be called %d times, got %d",
			len(urls),
			repo.savePipelineTaskCalls,
		)
	}

	if len(repo.tasks) != 0 {
		t.Fatalf("expected no saved tasks, got %d", len(repo.tasks))
	}

	if len(requester.calls) != 0 {
		t.Fatalf("expected requester not to be called, got %d calls", len(requester.calls))
	}

	if len(repo.stageResults) != 0 {
		t.Fatalf("expected no saved stage results, got %d", len(repo.stageResults))
	}
}

// TestWholePipeline_SaveTaskRetrySuccess проверяет сценарий,
// когда PipelineTask удалось сохранить не с первой попытки.
//
// Первая попытка SavePipelineTask завершается retryable-ошибкой,
// повторная попытка проходит успешно.
// После этого задача должна попасть в worker,
// запрос должен выполниться, а StageResult должен сохраниться со статусом success.
func TestWholePipeline_SaveTaskRetrySuccess(t *testing.T) {
	ctx := context.Background()

	repo := newFakePipelineRepo()
	repo.savePipelineTaskErrors = []error{
		ErrStorageUnavailableRetryable, // для первого URL
		nil,
		ErrStorageUnavailableRetryable, // для второго URL
		nil,
	}

	requester := &fakeRequester{}

	urls := testURLs()

	requestChannel := make(chan PipelineData, 10)
	firstWorkerCh := make(chan PipelineData, 10)
	secondWorkerCh := make(chan PipelineData, 10)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		DispatchRequests(requestChannel, firstWorkerCh, secondWorkerCh)
	}()

	wg.Add(2)
	go func() {
		defer wg.Done()
		RequestWorker(ctx, 1, firstWorkerCh, requester, repo)
	}()

	go func() {
		defer wg.Done()
		RequestWorker(ctx, 2, secondWorkerCh, requester, repo)
	}()

	RunPipeline(ctx, urls, requestChannel, repo)

	waitWithTimeout(t, &wg, time.Second)

	if repo.savePipelineCalls != 1 {
		t.Fatalf("expected SavePipeline to be called once, got %d", repo.savePipelineCalls)
	}

	if len(repo.pipelines) != 1 {
		t.Fatalf("expected 1 saved pipeline, got %d", len(repo.pipelines))
	}

	if repo.savePipelineTaskCalls != len(urls)*2 {
		t.Fatalf(
			"expected SavePipelineTask to be called %d times, got %d",
			len(urls)*2,
			repo.savePipelineTaskCalls,
		)
	}

	if len(repo.tasks) != len(urls) {
		t.Fatalf("expected %d saved tasks, got %d", len(urls), len(repo.tasks))
	}

	if len(requester.calls) != len(urls) {
		t.Fatalf("expected %d requester calls, got %d", len(urls), len(requester.calls))
	}

	if len(repo.stageResults) != len(urls) {
		t.Fatalf("expected %d stage results, got %d", len(urls), len(repo.stageResults))
	}

	for _, result := range repo.stageResults {
		if result.Stage != StageGetItems {
			t.Fatalf("expected stage %q, got %q", StageGetItems, result.Stage)
		}

		if result.Status != StatusSuccess {
			t.Fatalf("expected status %q, got %q", StatusSuccess, result.Status)
		}

		if result.Attempt != 1 {
			t.Fatalf("expected attempt 1, got %d", result.Attempt)
		}
	}
}

// TestWholePipeline_RequestFailedStageResultSaved проверяет сценарий,
// когда внешний сервис вернул ошибку, но результат этапа удалось сохранить в БД.
//
// Проверяем, что:
//   - pipeline был создан;
//   - задачи были созданы;
//   - requester был вызван для каждой задачи;
//   - для каждой задачи был сохранен StageResult;
//   - StageResult сохранен со статусом failed;
//   - в details сохранена информация об ошибке.
func TestWholePipeline_RequestFailedStageResultSaved(t *testing.T) {
	ctx := context.Background()

	repo := newFakePipelineRepo()

	requester := &fakeRequester{
		responses: []ResponseData{
			{
				URL:        "http://localhost:8080/getItems",
				StatusCode: 404,
				Body:       `{"error":"not found"}`,
				Err:        ErrNotFound,
			},
			{
				URL:        "http://localhost:8080/getItems",
				StatusCode: 500,
				Body:       `{"error":"internal server error"}`,
				Err:        ErrStorageUnavailableRetryable,
			},
		},
	}

	urls := testURLs()

	requestChannel := make(chan PipelineData, 10)
	firstWorkerCh := make(chan PipelineData, 10)
	secondWorkerCh := make(chan PipelineData, 10)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		DispatchRequests(requestChannel, firstWorkerCh, secondWorkerCh)
	}()

	wg.Add(2)
	go func() {
		defer wg.Done()
		RequestWorker(ctx, 1, firstWorkerCh, requester, repo)
	}()

	go func() {
		defer wg.Done()
		RequestWorker(ctx, 2, secondWorkerCh, requester, repo)
	}()

	RunPipeline(ctx, urls, requestChannel, repo)

	waitWithTimeout(t, &wg, time.Second)

	if repo.savePipelineCalls != 1 {
		t.Fatalf("expected SavePipeline to be called once, got %d", repo.savePipelineCalls)
	}

	if len(repo.pipelines) != 1 {
		t.Fatalf("expected 1 saved pipeline, got %d", len(repo.pipelines))
	}

	if len(repo.tasks) != len(urls) {
		t.Fatalf("expected %d saved tasks, got %d", len(urls), len(repo.tasks))
	}

	if len(requester.calls) != len(urls) {
		t.Fatalf("expected %d requester calls, got %d", len(urls), len(requester.calls))
	}

	if len(repo.stageResults) != len(urls) {
		t.Fatalf("expected %d stage results, got %d", len(urls), len(repo.stageResults))
	}

	for _, result := range repo.stageResults {
		if result.Stage != StageGetItems {
			t.Fatalf("expected stage %q, got %q", StageGetItems, result.Stage)
		}

		if result.Status != StatusFailed {
			t.Fatalf("expected status %q, got %q", StatusFailed, result.Status)
		}

		if result.Attempt != 1 {
			t.Fatalf("expected attempt 1, got %d", result.Attempt)
		}

		if len(result.Details) == 0 {
			t.Fatal("expected non-empty stage result details")
		}

		var details map[string]any
		if err := json.Unmarshal(result.Details, &details); err != nil {
			t.Fatalf("failed to unmarshal stage result details: %v", err)
		}

		if _, ok := details["error"]; !ok {
			t.Fatal("expected error field in stage result details")
		}
	}
}

// TestWholePipeline_RequestFailedStageResultRetrySuccess проверяет сценарий,
// когда внешний сервис вернул ошибку, а результат этапа get_items
// удалось сохранить в БД только со второй попытки.
//
// Для детерминированности в тесте используется один URL:
//   - первый вызов SaveStageResult возвращает retryable-ошибку;
//   - второй вызов SaveStageResult успешно сохраняет результат.
//
// Проверяем, что:
//   - pipeline был создан;
//   - PipelineTask был создан;
//   - requester был вызван один раз;
//   - SaveStageResult был вызван два раза;
//   - StageResult в итоге сохранен со статусом failed;
//   - в details сохранена информация об ошибке.
func TestWholePipeline_RequestFailedStageResultRetrySuccess(t *testing.T) {
	ctx := context.Background()

	repo := newFakePipelineRepo()
	repo.saveStageResultErrors = []error{
		ErrStorageUnavailableRetryable,
		nil,
	}

	requester := &fakeRequester{
		responses: []ResponseData{
			{
				URL:        "http://localhost:8080/getItems",
				StatusCode: 503,
				Body:       `{"error":"service unavailable"}`,
				Err:        ErrStorageUnavailableRetryable,
			},
		},
	}
	expectedError := repo.saveStageResultErrors[0].Error()
	urls := []RequestData{
		testURLs()[0],
	}

	requestChannel := make(chan PipelineData, 10)
	firstWorkerCh := make(chan PipelineData, 10)
	secondWorkerCh := make(chan PipelineData, 10)

	var wg sync.WaitGroup

	// Запускаем dispatcher, который перенаправляет задачи
	// из общего канала в worker channels.
	wg.Add(1)
	go func() {
		defer wg.Done()
		DispatchRequests(requestChannel, firstWorkerCh, secondWorkerCh)
	}()

	// Запускаем оба worker-а как в реальном pipeline.
	// Несмотря на то, что URL один, это проверяет полный flow:
	// RunPipeline -> DispatchRequests -> RequestWorker.
	wg.Add(2)
	go func() {
		defer wg.Done()
		RequestWorker(ctx, 1, firstWorkerCh, requester, repo)
	}()

	go func() {
		defer wg.Done()
		RequestWorker(ctx, 2, secondWorkerCh, requester, repo)
	}()
	RunPipeline(ctx, urls, requestChannel, repo)

	waitWithTimeout(t, &wg, time.Second)

	if repo.savePipelineCalls != 1 {
		t.Fatalf("expected SavePipeline to be called once, got %d", repo.savePipelineCalls)
	}

	if len(repo.pipelines) != 1 {
		t.Fatalf("expected 1 saved pipeline, got %d", len(repo.pipelines))
	}

	if repo.savePipelineTaskCalls != 1 {
		t.Fatalf("expected SavePipelineTask to be called once, got %d", repo.savePipelineTaskCalls)
	}

	if len(repo.tasks) != 1 {
		t.Fatalf("expected 1 saved task, got %d", len(repo.tasks))
	}

	if len(requester.calls) != 1 {
		t.Fatalf("expected requester to be called once, got %d", len(requester.calls))
	}

	if repo.saveStageResultCalls != 2 {
		t.Fatalf("expected SaveStageResult to be called twice, got %d", repo.saveStageResultCalls)
	}

	if len(repo.stageResults) != 1 {
		t.Fatalf("expected 1 saved stage result, got %d", len(repo.stageResults))
	}

	result := repo.stageResults[0]

	if result.Stage != StageGetItems {
		t.Fatalf("expected stage %q, got %q", StageGetItems, result.Stage)
	}

	if result.Status != StatusFailed {
		t.Fatalf("expected status %q, got %q", StatusFailed, result.Status)
	}

	if result.Attempt != 1 {
		t.Fatalf("expected attempt 1, got %d", result.Attempt)
	}

	if len(result.Details) == 0 {
		t.Fatal("expected non-empty stage result details")
	}

	var details map[string]any
	if err := json.Unmarshal(result.Details, &details); err != nil {
		t.Fatalf("failed to unmarshal stage result details: %v", err)
	}

	actualError, ok := details["error"].(string)
	if !ok {
		t.Fatal("expected error field to be string")
	}

	if actualError != expectedError {
		t.Fatalf("expected error %q, got %q", expectedError, actualError)
	}
}

// TestWholePipeline_RequestFailedStageResultSaveFailed проверяет сценарий,
// когда внешний сервис вернул ошибку, и результат этапа get_items
// не удалось сохранить в БД.
//
// Для детерминированности в тесте используется один URL.
//
// Проверяем, что:
//   - pipeline был создан;
//   - PipelineTask был создан;
//   - requester был вызван один раз;
//   - SaveStageResult был вызван один раз;
//   - StageResult не был сохранен, потому что БД вернула ошибку.
func TestWholePipeline_RequestFailedStageResultSaveFailed(t *testing.T) {
	ctx := context.Background()

	repo := newFakePipelineRepo()
	repo.saveStageResultErrors = []error{
		ErrStorageUnavailableNonRetryable,
	}

	responseErr := ErrNotFound

	requester := &fakeRequester{
		responses: []ResponseData{
			{
				URL:        "http://localhost:8080/getItems",
				StatusCode: 404,
				Body:       `{"error":"not found"}`,
				Err:        responseErr,
			},
		},
	}

	urls := []RequestData{
		testURLs()[0],
	}

	requestChannel := make(chan PipelineData, 10)
	firstWorkerCh := make(chan PipelineData, 10)
	secondWorkerCh := make(chan PipelineData, 10)

	var wg sync.WaitGroup

	// Запускаем dispatcher, чтобы тест проходил через тот же flow,
	// что и реальный pipeline.
	wg.Add(1)
	go func() {
		defer wg.Done()
		DispatchRequests(requestChannel, firstWorkerCh, secondWorkerCh)
	}()

	// Запускаем два worker-а как в реальном приложении.
	wg.Add(2)
	go func() {
		defer wg.Done()
		RequestWorker(ctx, 1, firstWorkerCh, requester, repo)
	}()

	go func() {
		defer wg.Done()
		RequestWorker(ctx, 2, secondWorkerCh, requester, repo)
	}()

	RunPipeline(ctx, urls, requestChannel, repo)

	waitWithTimeout(t, &wg, time.Second)

	if repo.savePipelineCalls != 1 {
		t.Fatalf("expected SavePipeline to be called once, got %d", repo.savePipelineCalls)
	}

	if len(repo.pipelines) != 1 {
		t.Fatalf("expected 1 saved pipeline, got %d", len(repo.pipelines))
	}

	if repo.savePipelineTaskCalls != 1 {
		t.Fatalf("expected SavePipelineTask to be called once, got %d", repo.savePipelineTaskCalls)
	}

	if len(repo.tasks) != 1 {
		t.Fatalf("expected 1 saved task, got %d", len(repo.tasks))
	}

	if len(requester.calls) != 1 {
		t.Fatalf("expected requester to be called once, got %d", len(requester.calls))
	}

	if repo.saveStageResultCalls != 1 {
		t.Fatalf("expected SaveStageResult to be called once, got %d", repo.saveStageResultCalls)
	}

	if len(repo.stageResults) != 0 {
		t.Fatalf("expected no saved stage results, got %d", len(repo.stageResults))
	}
}
