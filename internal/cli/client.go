package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

type Client struct {
	BaseURL    string
	HTTP       *http.Client
	Token      string
}

func NewClient(cfg *Config) *Client {
	return &Client{
		BaseURL: cfg.BaseURL(),
		HTTP:    &http.Client{},
		Token:   cfg.AccessToken,
	}
}

func (c *Client) do(method, path string, body, out any) error {
	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal request: %w", err)
		}
		bodyReader = bytes.NewReader(data)
	}

	req, err := http.NewRequest(method, c.BaseURL+path, bodyReader)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		var apiErr struct {
			Title  string `json:"title"`
			Status int    `json:"status"`
			Detail string `json:"detail,omitempty"`
		}
		if json.Unmarshal(respBody, &apiErr) == nil && apiErr.Title != "" {
			msg := apiErr.Title
			if apiErr.Detail != "" {
				msg += ": " + apiErr.Detail
			}
			return fmt.Errorf("%s", msg)
		}
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}

	if out != nil && len(respBody) > 0 {
		if err := json.Unmarshal(respBody, out); err != nil {
			return fmt.Errorf("unmarshal response: %w", err)
		}
	}

	return nil
}

func (c *Client) GET(path string, out any) error {
	return c.do(http.MethodGet, path, nil, out)
}

func (c *Client) POST(path string, body, out any) error {
	return c.do(http.MethodPost, path, body, out)
}

func (c *Client) PUT(path string, body, out any) error {
	return c.do(http.MethodPut, path, body, out)
}

func (c *Client) PATCH(path string, body, out any) error {
	return c.do(http.MethodPatch, path, body, out)
}

func (c *Client) DELETE(path string, out any) error {
	return c.do(http.MethodDelete, path, nil, out)
}

func (c *Client) DELETEWithBody(path string, body, out any) error {
	return c.do(http.MethodDelete, path, body, out)
}
