package booking

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/Feliper83/transitflow-go/internal/httputil"
)

// HTTPTripClient is the synchronous REST client for trip-service.
type HTTPTripClient struct {
	baseURL string
	http    *http.Client
	log     *slog.Logger
}

// NewHTTPTripClient builds a trip client pointing at baseURL (e.g.
// http://trip-service:8081).
func NewHTTPTripClient(baseURL string, log *slog.Logger) *HTTPTripClient {
	return &HTTPTripClient{
		baseURL: baseURL,
		http:    &http.Client{Timeout: 5 * time.Second},
		log:     log,
	}
}

func (c *HTTPTripClient) do(ctx context.Context, method, path string, body any) (*http.Response, error) {
	var reader *bytes.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(b)
	} else {
		reader = bytes.NewReader(nil)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if cid := httputil.CorrelationID(ctx); cid != "" {
		req.Header.Set(httputil.CorrelationHeader, cid)
	}
	return c.http.Do(req)
}

// GetTrip fetches a trip for pricing.
func (c *HTTPTripClient) GetTrip(ctx context.Context, tripID int64) (*TripView, error) {
	resp, err := c.do(ctx, http.MethodGet, fmt.Sprintf("/api/trips/%d", tripID), nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("trip-service GET trip %d returned %d", tripID, resp.StatusCode)
	}
	var tv TripView
	if err := json.NewDecoder(resp.Body).Decode(&tv); err != nil {
		return nil, err
	}
	return &tv, nil
}

// Reserve holds a seat. Returns true on 2xx, false on 409 (no capacity), and an
// error for anything else.
func (c *HTTPTripClient) Reserve(ctx context.Context, tripID int64, bookingRef string, seatCount int) (bool, error) {
	body := map[string]any{"bookingRef": bookingRef, "seatCount": seatCount}
	resp, err := c.do(ctx, http.MethodPost, fmt.Sprintf("/api/trips/%d/reservations", tripID), body)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return true, nil
	case resp.StatusCode == http.StatusConflict:
		return false, nil
	default:
		return false, fmt.Errorf("trip-service reservation failed: %d", resp.StatusCode)
	}
}

// Release is the SAGA compensation: return a held seat. A non-2xx is logged but
// not treated as fatal (the hold may already be released — the operation is
// idempotent on the trip side).
func (c *HTTPTripClient) Release(ctx context.Context, bookingRef string) error {
	resp, err := c.do(ctx, http.MethodPost, fmt.Sprintf("/api/trips/reservations/%s/release", bookingRef), nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		c.log.Warn("seat release returned non-success", "bookingRef", bookingRef, "status", resp.StatusCode)
	}
	return nil
}
