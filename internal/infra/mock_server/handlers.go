package mock_server

import (
	"encoding/json"
	"log/slog"
	"net/http"
)

// GetItemsHandler имитирует внешний сервис, который возвращает item_ids по user_id.
//
// Handler принимает JSON вида:
//
//	{"user_id": 100}
//
// И возвращает:
//
//	{"item_ids": [1001, 1002, 1003]}
func GetItemsHandler(w http.ResponseWriter, r *http.Request) {
	var req GetItemsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	slog.Debug(
		"getItems request",
		"user_id", req.UserID,
	)

	if MaybeWriteRandomError(w) {
		return
	}

	resp := GetItemsResponse{
		ItemIDs: []int{
			req.UserID*10 + 1,
			req.UserID*10 + 2,
			req.UserID*10 + 3,
		},
	}
	slog.Debug(
		"getItems response",
		"user_id", req.UserID,
		"status_code", http.StatusOK,
		"response", resp,
	)

	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// FillItemsHandler имитирует внешний сервис обогащения item-ов.
//
// Handler принимает список item_ids и возвращает искусственные описания товаров.
// Иногда может вернуть случайную ошибку, чтобы pipeline мог отрабатывать
// retry и non-retryable сценарии.
func FillItemsHandler(w http.ResponseWriter, r *http.Request) {
	var req ItemIDsRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	slog.Debug(
		"fillItems request",
		"item_ids", req.ItemIDs,
	)

	if MaybeWriteRandomError(w) {
		return
	}

	resp := FillItemsResponse{
		FilledItems: []string{
			"item description 1",
			"item description 2",
			"item description 3",
		},
	}
	slog.Debug(
		"fillItems response",
		"item_ids", req.ItemIDs,
		"status_code", http.StatusOK,
		"response", resp,
	)

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(
			w,
			"failed to encode response",
			http.StatusInternalServerError,
		)
		return
	}
}

// ScoreItemsHandler имитирует внешний сервис скоринга item-ов.
//
// Handler принимает список item_ids и возвращает score для каждого item.
// Иногда может вернуть случайную ошибку, чтобы pipeline мог отрабатывать
// retry и non-retryable сценарии.
func ScoreItemsHandler(w http.ResponseWriter, r *http.Request) {

	var req ItemIDsRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	slog.Debug(
		"scoreItems request",
		"item_ids", req.ItemIDs,
	)
	if MaybeWriteRandomError(w) {
		return
	}

	scores := make(map[int]float64)

	for _, itemID := range req.ItemIDs {
		scores[itemID] = float64(itemID) / 10
	}

	resp := ScoreItemsResponse{
		Scores: scores,
	}
	slog.Debug(
		"scoreItems response",
		"item_ids", req.ItemIDs,
		"status_code", http.StatusOK,
		"response", resp,
	)

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(
			w,
			"failed to encode response",
			http.StatusInternalServerError,
		)
		return
	}
}

// LogItemsHandler имитирует внешний сервис логирования item-ов.
//
// Handler принимает список item_ids и возвращает признак успешного логирования.
// Иногда может вернуть случайную ошибку, чтобы pipeline мог отрабатывать
// retry и non-retryable сценарии.
func LogItemsHandler(w http.ResponseWriter, r *http.Request) {
	var req ItemIDsRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	slog.Debug(
		"logItems request",
		"item_ids", req.ItemIDs,
	)
	if MaybeWriteRandomError(w) {
		return
	}

	resp := LogItemsResponse{
		Logged: true,
	}
	slog.Debug(
		"logItems response",
		"item_ids", req.ItemIDs,
		"status_code", http.StatusOK,
		"response", resp,
	)
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(
			w,
			"failed to encode response",
			http.StatusInternalServerError,
		)
		return
	}
}
