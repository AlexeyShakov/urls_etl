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
	req domain.RequestData,
) domain.ResponseData {
	return makeRequestWithRetry(
		ctx,
		r.client,
		req,
		r.cfg.MaxRetries,
	)
}

func NewHttpRequester(client *http.Client, cfg config.HTTPConfig) *HTTPRequester {
	return &HTTPRequester{client: client, cfg: cfg}
}

// makeRequestWithRetry выполняет HTTP-запрос с retry.
//
// Сейчас retry выполняется для:
// - сетевых ошибок;
// - 5xx ответов.
//
// Между попытками используется линейный backoff.
func makeRequestWithRetry(
	ctx context.Context,
	client *http.Client,
	reqData domain.RequestData,
	maxRetries int,
) domain.ResponseData {
	var lastErr error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		resp, err := makeRequest(ctx, client, reqData)
		if err == nil && resp.StatusCode < 500 {
			return resp
		}

		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("server error: status code %d", resp.StatusCode)
		}

		time.Sleep(time.Duration(attempt) * 500 * time.Millisecond)
	}

	return domain.ResponseData{
		URL: reqData.URL,
		Err: lastErr,
	}
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
		return domain.ResponseData{URL: reqData.URL, StatusCode: httpResp.StatusCode, Err: err}, err
	}

	return domain.ResponseData{
		URL:        reqData.URL,
		StatusCode: httpResp.StatusCode,
		Body:       string(bodyBytes),
	}, nil
}
