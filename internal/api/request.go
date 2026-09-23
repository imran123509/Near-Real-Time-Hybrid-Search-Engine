package api

import (
	"net/http"
	"net/url"
	"strconv"

	"near-real-time-hybrid-search-engine/internal/search/hybrid"
)

// requestError is a request the handler could not even turn into a search.
// Its message is fixed text, safe to return to the client.
type requestError struct {
	reason string
}

func (e *requestError) Error() string { return e.reason }

// parseSearchRequest reads q and limit from the query string.
//
// It only converts HTTP input into a hybrid.SearchRequest. Whether the query
// is empty or too long, and whether the limit is within the configured range,
// is decided by the search service, so those rules and their limits live in
// one place.
func parseSearchRequest(r *http.Request) (hybrid.SearchRequest, error) {
	params, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return hybrid.SearchRequest{}, &requestError{"query string is malformed"}
	}
	if !params.Has("q") {
		return hybrid.SearchRequest{}, &requestError{"query parameter q is required"}
	}

	req := hybrid.SearchRequest{Query: params.Get("q")}
	// An absent or empty limit means the service's default.
	if raw := params.Get("limit"); raw != "" {
		limit, err := strconv.Atoi(raw)
		if err != nil {
			return hybrid.SearchRequest{}, &requestError{"limit must be a whole number"}
		}
		req.Limit = limit
	}
	return req, nil
}
