package replication

import (
	"net/http"
	"time"
)

var Client = &http.Client{
	Timeout: 2 * time.Second,
}
