package http

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
	"urls_etl/internal/config"
	"urls_etl/internal/domain"
)

type HTTPRequester struct {
	client *http.Client
	cfg    config.HTTPConfig
}

func (r *HTTPRequester) Do(
	ctx context.Context,
	pipelineData domain.PipelineData,
) domain.ResponseData {
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
	req domain.RequestData,
	maxRetries int,
) domain.ResponseData {
	var lastResp domain.ResponseData
	var lastErr error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		resp, err := makeRequest(ctx, client, req)

		if err != nil {
			lastErr = err
			lastResp = domain.ResponseData{
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
			time.Sleep(time.Duration(attempt) * 500 * time.Millisecond)
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

// makeRequest создает и отправляет один HTTP-запрос.
//
// Функция возвращает ResponseData со статус-кодом и телом ответа.
// Response body всегда закрывается после чтения.
func makeRequest(
	ctx context.Context,
	client *http.Client,
	reqData domain.RequestData,
) (domain.ResponseData, error) {
	httpReq, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		reqData.URL,
		bytes.NewBufferString(reqData.Payload),
	)
	if err != nil {
		return domain.ResponseData{URL: reqData.URL, Err: err}, err
	}

	for key, value := range reqData.Headers {
		httpReq.Header.Set(key, value)
	}

	httpResp, err := client.Do(httpReq)
	if err != nil {
		return domain.ResponseData{URL: reqData.URL, Err: err}, err
	}
	defer func() {
		_ = httpResp.Body.Close()
	}()

	bodyBytes, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return domain.ResponseData{
			URL:        reqData.URL,
			StatusCode: httpResp.StatusCode,
			Err:        err,
		}, err
	}

	return domain.ResponseData{
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
