package mock_server

import "net/http"

var NonRetryableStatuses = []int{
	http.StatusBadRequest,   //400
	http.StatusUnauthorized, //401
	http.StatusForbidden,    //403
	http.StatusNotFound,     //404
}

var RetryableStatuses = []int{
	http.StatusTooManyRequests,     //429
	http.StatusInternalServerError, //500
	http.StatusBadGateway,          //502
	http.StatusServiceUnavailable,  //503
	http.StatusGatewayTimeout,      //504
}

var RetryableStatusSet = map[int]struct{}{
	http.StatusTooManyRequests:     {}, //429
	http.StatusInternalServerError: {}, //500
	http.StatusBadGateway:          {}, //502
	http.StatusServiceUnavailable:  {}, //503
	http.StatusGatewayTimeout:      {}, //504
}
