package restapi

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"maglev.onebusaway.org/internal/app"
	"maglev.onebusaway.org/internal/clock"
	"maglev.onebusaway.org/internal/utils"
)

func TestValidateIDMiddleware(t *testing.T) {
	// Properly initialize the embedded Application struct
	api := &RestAPI{
		Application: &app.Application{
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
			Clock:  clock.NewMockClock(time.Now()),
		},
	}

	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, ok := utils.GetIDFromContext(r.Context())
		if !ok || id != "valid_id" {
			t.Errorf("Expected 'valid_id' in context, got %v", id)
		}
		w.WriteHeader(http.StatusOK)
	})

	handler := api.ValidateIDMiddleware(nextHandler)

	t.Run("Valid ID", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/where/agency/valid_id", nil)
		req.SetPathValue("id", "valid_id")
		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Errorf("Expected 200 OK, got %d", rr.Code)
		}
	})

	t.Run("Invalid ID Format", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/where/agency/", nil)
		req.SetPathValue("id", "") // Empty ID should fail basic validation
		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		if rr.Code != http.StatusBadRequest {
			t.Errorf("Expected 400 Bad Request for empty ID, got %d", rr.Code)
		}
	})
}

func TestValidateCombinedIDMiddleware(t *testing.T) {
	// Properly initialize the embedded Application struct
	api := &RestAPI{
		Application: &app.Application{
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
			Clock:  clock.NewMockClock(time.Now()),
		},
	}

	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		parsed, ok := utils.GetParsedIDFromContext(r.Context())
		if !ok {
			t.Errorf("Expected ParsedID in context")
		}
		if parsed.AgencyID != "1" || parsed.CodeID != "stop123" {
			t.Errorf("Expected AgencyID=1, CodeID=stop123, got %v", parsed)
		}
		w.WriteHeader(http.StatusOK)
	})

	handler := api.ValidateCombinedIDMiddleware(nextHandler)

	t.Run("Valid Combined ID", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/where/stop/1_stop123", nil)
		req.SetPathValue("id", "1_stop123")
		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Errorf("Expected 200 OK, got %d", rr.Code)
		}
	})

	t.Run("Invalid Combined ID (No Underscore)", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/where/stop/1stop123", nil)
		req.SetPathValue("id", "1stop123") // Missing underscore
		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		if rr.Code != http.StatusBadRequest {
			t.Errorf("Expected 400 Bad Request for missing underscore, got %d", rr.Code)
		}
	})
}
