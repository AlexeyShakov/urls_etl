package http_client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
	"urls_etl/internal/config"
	"urls_etl/internal/domain/pipeline"
)

// Максимальный размер тела ответа — 1 MiB.
// Значение лучше передавать из HTTP-конфига.
const maxResponseBodyBytes int64 = 1 << 20

type HTTPRequester struct {
	client *http.Client
	cfg    config.HTTPConfig
}

func (r *HTTPRequester) Do(
	ctx context.Context,
	pipelineData pipeline.PipelineData,
) pipeline.ResponseData {
	return makeRequestWithRetry(
		ctx,
		r.client,
		pipelineData.Request,
		r.cfg.MaxRetries,
	)
}

func NewHttpRequester(client *http.Client, cfg config.HTTPConfig) *HTTPRequester {
	return &HTTPRequester{client: client, cfg: cfg}
}

// makeRequestWithRetry выполняет HTTP-запрос с retry-политикой.
//
// Успешными считаются только:
//   - 200 OK;
//   - 201 Created;
//   - 204 No Content.
//
// Retry выполняется для:
//   - сетевых ошибок;
//   - временных HTTP-ошибок: 429, 500, 502, 503, 504.
//
// Non-retryable ошибки, например 400, 401, 403, 404,
// возвращаются сразу без повторных попыток, но с заполненным Err.
//
// В случае исчерпания всех попыток функция возвращает
// последний полученный ResponseData с максимально полной информацией
// для дальнейшего анализа и сохранения в БД.
func makeRequestWithRetry(
	ctx context.Context,
	client *http.Client,
	req pipeline.RequestData,
	maxRetries int,
) pipeline.ResponseData {
	var lastResp pipeline.ResponseData
	var lastErr error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		resp, err := makeRequest(ctx, client, req)

		if ctxErr := ctx.Err(); ctxErr != nil {
			if err != nil {
				return pipeline.ResponseData{
					URL: req.URL,
					Err: ctxErr,
				}
			}
			resp.Err = ctxErr
			return resp
		}

		if errors.Is(err, ErrResponseBodyTooLarge) {
			return resp
		}

		if err != nil {
			lastErr = err
			lastResp = pipeline.ResponseData{
				URL: req.URL,
				Err: err,
			}
		} else {
			lastResp = resp

			if isSuccessStatusCode(resp.StatusCode) {
				return resp
			}

			if !isRetryableStatusCode(resp.StatusCode) {
				resp.Err = fmt.Errorf("non-retryable status code: %d", resp.StatusCode)
				return resp
			}

			lastErr = fmt.Errorf("retryable status code received: %d", resp.StatusCode)
			lastResp.Err = lastErr
		}

		if attempt < maxRetries {
			delay := time.Duration(attempt) * 500 * time.Millisecond

			if err := waitBeforeRetry(ctx, delay); err != nil {
				lastResp.Err = err
				return lastResp
			}
		}
	}

	if lastResp.Err == nil {
		lastResp.Err = fmt.Errorf(
			"request failed after %d attempts: %w",
			maxRetries,
			lastErr,
		)
	}

	return lastResp
}

// waitBeforeRetry ожидает задержку или отмену контекста.
func waitBeforeRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// makeRequest создает и отправляет один HTTP-запрос.
//
// Функция возвращает ResponseData со статус-кодом и телом ответа.
// Response body всегда закрывается после чтения.
func makeRequest(
	ctx context.Context,
	client *http.Client,
	reqData pipeline.RequestData,
) (pipeline.ResponseData, error) {
	httpReq, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		reqData.URL,
		bytes.NewBufferString(reqData.Payload),
	)
	if err != nil {
		return pipeline.ResponseData{URL: reqData.URL, Err: err}, err
	}

	for key, value := range reqData.Headers {
		httpReq.Header.Set(key, value)
	}

	httpResp, err := client.Do(httpReq)
	if err != nil {
		return pipeline.ResponseData{URL: reqData.URL, Err: err}, err
	}
	defer func() {
		_ = httpResp.Body.Close()
	}()

	bodyBytes, err := io.ReadAll(io.LimitReader(httpResp.Body, maxResponseBodyBytes+1))

	if err != nil {
		return pipeline.ResponseData{
			URL:        reqData.URL,
			StatusCode: httpResp.StatusCode,
			Err:        err,
		}, err
	}

	if int64(len(bodyBytes)) > maxResponseBodyBytes {
		err := fmt.Errorf(
			"%w: limit is %d bytes",
			ErrResponseBodyTooLarge,
			maxResponseBodyBytes,
		)
		return pipeline.ResponseData{
			URL:        reqData.URL,
			StatusCode: httpResp.StatusCode,
			Err:        err,
		}, err
	}

	return pipeline.ResponseData{
		URL:        reqData.URL,
		StatusCode: httpResp.StatusCode,
		Body:       string(bodyBytes),
	}, nil
}

func isSuccessStatusCode(statusCode int) bool {
	switch statusCode {
	case http.StatusOK,
		http.StatusCreated,
		http.StatusNoContent:
		return true
	default:
		return false
	}
}

func isRetryableStatusCode(statusCode int) bool {
	switch statusCode {
	case http.StatusTooManyRequests,
		http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}
