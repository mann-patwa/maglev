package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"maglev.onebusaway.org/internal/appconf"
	"maglev.onebusaway.org/internal/gtfs"
	"maglev.onebusaway.org/internal/models"

	_ "github.com/mattn/go-sqlite3"
)

func TestHandlerLockSafety(t *testing.T) {
	tempDir := t.TempDir()

	// Initial setup with "raba.zip" (valid data)
	rabaPath := models.GetFixturePath(t, "raba.zip")
	gtfsConfig := gtfs.Config{
		GtfsURL:      rabaPath,
		GTFSDataPath: tempDir + "/gtfs.db",
		Env:          appconf.Development,
	}

	appConfig := appconf.Config{
		Port:      50001,
		RateLimit: 100,
		Env:       appconf.Test,
		ApiKeys:   []string{"TEST"},
	}

	application, err := BuildApplication(appConfig, gtfsConfig)
	require.NoError(t, err)

	srv, api := CreateServer(application, appConfig)

	// Create context for controlling server lifecycle
	serverCtx, serverCancel := context.WithCancel(context.Background())
	defer serverCancel() // Ensure cleanup if test fails early

	// Channel to signal when server is ready to accept requests
	readyChan := make(chan struct{})

	// Start server in a goroutine
	go func() {
		if err := Run(serverCtx, srv, application.GtfsManager, api, application.Logger); err != nil {
			t.Logf("Server exited with error: %v", err)
		}
	}()

	// Wait for server to start serving
	go func() {
		for {
			select {
			case <-serverCtx.Done():
				return
			default:
				// Try to connect to the server
				resp, err := http.Get("http://localhost:50001/api/where/current-time.json?key=TEST")
				if err == nil {
					resp.Body.Close()
					if resp.StatusCode == http.StatusOK {
						close(readyChan)
						return
					}
				}
				time.Sleep(50 * time.Millisecond)
			}
		}
	}()

	select {
	case <-readyChan:
		t.Log("Server is ready")
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for server to start")
	}

	// Helper to fetch agency ID via HTTP
	getAgencyViaHTTP := func() string {
		resp, err := http.Get("http://localhost:50001/api/where/agencies-with-coverage.json?key=TEST")
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)

		var result struct {
			Data struct {
				List []struct {
					ID string `json:"agencyId"`
				} `json:"list"`
			} `json:"data"`
		}
		err = json.NewDecoder(resp.Body).Decode(&result)
		require.NoError(t, err)
		require.NotEmpty(t, result.Data.List)
		return result.Data.List[0].ID
	}

	// Verify initial agency
	initialAgencyID := getAgencyViaHTTP()
	t.Logf("Initial Agency ID: %s", initialAgencyID)
	
	// Use a known existing stop ID from raba.zip to generate traffic
	// We don't need to discover this via HTTP (which requires traversing routes), 
	// as valid data is fixed for the test fixture.
	stopID := "25_1049"
	t.Logf("Using Stop ID: %s", stopID)





	// Prepare for concurrent requests
	var wg sync.WaitGroup
	readerCount := 10
	wg.Add(readerCount)
	errChan := make(chan error, readerCount)

	// Context to signaling readers to stop
	// We use a separate context for readers so we can stop them before stopping the server
	readerCtx, readerCancel := context.WithCancel(context.Background())

	// Start readers
	t.Log("Starting readers...")
	client := &http.Client{
		Timeout: 2 * time.Second,
	}

	for i := 0; i < readerCount; i++ {
		go func(id int) {
			t.Log("reader", id, "started")
			defer wg.Done()
			url := fmt.Sprintf("http://localhost:50001/api/where/stop/%s.json?key=TEST", stopID)

			for {
				select {
				case <-readerCtx.Done():
					t.Log("cancelled reader context for reader", id)
					return
				default:
					req, err := http.NewRequestWithContext(readerCtx, "GET", url, nil)
					if err != nil {
						if readerCtx.Err() == nil {
							errChan <- fmt.Errorf("reader %d failed to create request: %w", id, err)
						}
						return
					}

					resp, err := client.Do(req)
					if err != nil {
						// Ignore errors during cancel
					} else {
						if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound && resp.StatusCode != http.StatusTooManyRequests {
							if readerCtx.Err() == nil {
								errChan <- fmt.Errorf("reader %d unexpected status: %d", id, resp.StatusCode)
								resp.Body.Close()
								return
							}
						}
						resp.Body.Close()
					}

					time.Sleep(10 * time.Millisecond) // small delay to not hammer too hard
				}
			}
		}(i)
	}

	time.Sleep(100 * time.Millisecond)

	gtfsZipPath := models.GetFixturePath(t, "gtfs.zip")
	application.GtfsManager.Config.GtfsURL = gtfsZipPath

	t.Log("Triggering ForceUpdate...")
	updateErr := application.GtfsManager.ForceUpdate(context.Background())
	require.NoError(t, updateErr, "ForceUpdate should succeed")
	t.Log("ForceUpdate completed.")

	time.Sleep(100 * time.Millisecond)

	// Stop readers
	readerCancel()
	wg.Wait()
	close(errChan)

	// Check for errors
	for err := range errChan {
		assert.NoError(t, err)
	}

	// Verify the switch happened
	// Verify the switch happened via HTTP
	finalAgencyID := getAgencyViaHTTP()
	t.Logf("Final Agency ID: %s", finalAgencyID)
	assert.Equal(t, "40", finalAgencyID, "Should have switched to agency 40 from gtfs.zip")

	// Stop server
	serverCancel()
}
