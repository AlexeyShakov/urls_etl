package domain_new

type RequestData struct {
	URL     string
	Headers map[string]string
	Payload string
}

type ResponseData struct {
	URL        string
	StatusCode int
	Body       string
	Err        error
}

type RequestDetails struct {
	Payload string
	Headers map[string]string
}
