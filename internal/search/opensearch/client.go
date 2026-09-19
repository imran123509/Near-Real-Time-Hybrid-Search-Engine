// Package opensearch stores documents in OpenSearch and runs BM25 keyword
// search over them.
//
// It is the only package that uses the OpenSearch SDK. Callers work with the
// Client, Document and SearchResult types defined here, so nothing else in the
// application depends on OpenSearch request or response types.
package opensearch

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	osgo "github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"

	"near-real-time-hybrid-search-engine/internal/config"
)

// Client reads and writes documents in one OpenSearch index. It is safe for
// concurrent use and meant to be shared for the life of the process.
type Client struct {
	api   *opensearchapi.Client
	index string
}

// New validates cfg and creates a client for cfg.Index. It does not contact
// OpenSearch; call Ping to check the cluster is reachable. The caller owns the
// client and must call Close.
func New(cfg config.OpenSearchConfig) (*Client, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, fmt.Errorf("invalid opensearch config: %w", err)
	}

	api, err := opensearchapi.NewClient(opensearchapi.Config{
		Client: osgo.Config{
			Addresses: []string{cfg.URL},
			Username:  cfg.Username,
			Password:  cfg.Password,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("create opensearch client: %w", err)
	}
	return &Client{api: api, index: cfg.Index}, nil
}

// Ping checks that the cluster is reachable and accepts the credentials.
func (c *Client) Ping(ctx context.Context) error {
	resp, err := c.api.Ping(ctx, nil)
	if err != nil {
		return fmt.Errorf("ping opensearch: %w", newRequestError(resp, err))
	}
	return nil
}

// Close releases the client's idle connections.
func (c *Client) Close() error {
	return c.api.Close()
}

// RequestError is an OpenSearch request that failed. StatusCode is 0 when no
// response arrived, for example after a network error or a cancelled context.
type RequestError struct {
	StatusCode int
	Err        error
}

func (e *RequestError) Error() string {
	if e.StatusCode == 0 {
		return e.Err.Error()
	}
	return fmt.Sprintf("status %d: %v", e.StatusCode, e.Err)
}

func (e *RequestError) Unwrap() error { return e.Err }

// Temporary reports whether retrying the request may succeed: network errors,
// timeouts, rate limiting and server errors. Other 4xx responses mean the
// request itself was rejected and will fail again.
func (e *RequestError) Temporary() bool {
	return e.StatusCode == 0 ||
		e.StatusCode == http.StatusRequestTimeout ||
		e.StatusCode == http.StatusTooManyRequests ||
		e.StatusCode >= http.StatusInternalServerError
}

func newRequestError(resp *osgo.Response, err error) *RequestError {
	return &RequestError{StatusCode: statusCode(resp), Err: err}
}

// statusCode returns the HTTP status of a response, or 0 if none arrived.
func statusCode(resp *osgo.Response) int {
	if resp == nil {
		return 0
	}
	return resp.StatusCode
}

// validateConfig checks the settings without echoing the URL, which could
// contain credentials.
func validateConfig(cfg config.OpenSearchConfig) error {
	u, err := url.Parse(cfg.URL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return errors.New("url must look like http(s)://host[:port]")
	}
	if u.User != nil {
		return errors.New("url must not contain credentials; set OPENSEARCH_USERNAME and OPENSEARCH_PASSWORD instead")
	}
	if (cfg.Username == "") != (cfg.Password == "") {
		return errors.New("username and password must be set together")
	}
	return validateIndexName(cfg.Index)
}

// validateIndexName applies OpenSearch's index naming rules, so a bad name
// fails at startup instead of on the first request.
func validateIndexName(name string) error {
	switch {
	case name == "":
		return errors.New("index name is required")
	case len(name) > 255:
		return errors.New("index name must be at most 255 bytes")
	case name != strings.ToLower(name):
		return fmt.Errorf("index name %q must be lowercase", name)
	case name == "." || name == "..":
		return fmt.Errorf("index name %q is reserved", name)
	case strings.ContainsAny(name[:1], "-_+"):
		return fmt.Errorf("index name %q must not start with -, _ or +", name)
	case strings.ContainsAny(name, `\/*?"<>| ,#:`):
		return fmt.Errorf(`index name %q must not contain \ / * ? " < > | space , # or :`, name)
	}
	return nil
}
