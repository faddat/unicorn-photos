package main

import (
	"fmt"
	"io"
	"net/http"
	"time"
)

var defaultHTTPClient = &http.Client{
	Timeout: 30 * time.Second, // General timeout for requests
	Transport: &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     90 * time.Second,
	},
}

// HTTPGet performs a GET request with the default client and returns the body.
func HTTPGet(url string) ([]byte, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request for %s: %w", url, err)
	}
	// Consider adding a User-Agent
	// req.Header.Set("User-Agent", "UnicornPhotos/1.0")

	resp, err := defaultHTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute GET request to %s: %w", url, err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			logger.Printf("Error closing response body for %s: %v", url, err)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body) // Read body for error context
		return nil, fmt.Errorf("request to %s failed with status %d: %s", url, resp.StatusCode, string(bodyBytes))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body from %s: %w", url, err)
	}
	return body, nil
}
