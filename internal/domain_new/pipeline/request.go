package pipeline

// RequestData содержит параметры HTTP-запроса для выполнения стадии.
type RequestData struct {
	URL     string
	Headers map[string]string
	Payload string
}

// ResponseData содержит ответ сервиса или ошибку выполнения запроса.
type ResponseData struct {
	URL        string
	StatusCode int
	Body       string
	Err        error
}

// RequestDetails содержит заголовки и тело запроса.
type RequestDetails struct {
	Payload string
	Headers map[string]string
}
