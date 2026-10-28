package webui

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/IDisposable/docker-wyze-bridge/internal/wyzeapi"
)

func TestAPICamerasCarriesGwellStatus(t *testing.T) {
	srv, _ := testServer(t)
	srv.cfg.StateDir = t.TempDir()
	injectGwellCam(srv, "D03F27AABBCC", "window", "10.10.10.50")

	path := srv.cfg.GwellStatusFile()
	os.MkdirAll(filepath.Dir(path), 0755)
	status := fmt.Sprintf(`{"version":1,"updated_at":%q,"cameras":{"d03f27aabbcc":{"stage":"forwarding","generation":4,"last_video_at":null}}}`,
		time.Now().UTC().Format(time.RFC3339Nano))
	if err := os.WriteFile(path, []byte(status), 0644); err != nil {
		t.Fatal(err)
	}

	w := httptest.NewRecorder()
	srv.handleAPICameras(w, httptest.NewRequest("GET", "/api/cameras", nil))
	var cams []map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&cams); err != nil {
		t.Fatal(err)
	}
	if len(cams) != 1 {
		t.Fatalf("cams %v", cams)
	}
	g, ok := cams[0]["gwell"].(map[string]interface{})
	if !ok {
		t.Fatalf("no gwell status: %v", cams[0])
	}
	if g["stage"] != "forwarding" || g["generation"] != float64(4) || g["stale"] != false {
		t.Fatalf("gwell %v", g)
	}
	if cams[0]["state"] == nil || cams[0]["mac"] != "D03F27AABBCC" {
		t.Fatal("existing fields changed")
	}
}

func TestAPICamerasWithoutStatusFileUnchanged(t *testing.T) {
	srv, _ := testServer(t)
	srv.cfg.StateDir = t.TempDir()
	injectGwellCam(srv, "D03F27AABBCC", "window", "10.10.10.50")
	w := httptest.NewRecorder()
	srv.handleAPICameras(w, httptest.NewRequest("GET", "/api/cameras", nil))
	var cams []map[string]interface{}
	json.NewDecoder(w.Body).Decode(&cams)
	if _, ok := cams[0]["gwell"]; ok {
		t.Fatal("gwell key without a status file")
	}
}

func TestShim_CameraToken_RefusedIs403(t *testing.T) {
	srv, _ := testServer(t)
	injectGwellCam(srv, "BEBEBEBEBEBE", "window", "")
	srv.SetMarsMinter(&fakeMarsMinter{err: &wyzeapi.MarsHTTPError{StatusCode: 401, Status: "401 Unauthorized"}})
	w := httptest.NewRecorder()
	srv.handleShimCameraToken(w, newShimReq("GET", "/internal/wyze/Camera/CameraToken?deviceId=BEBEBEBEBEBE"))
	if w.Code != http.StatusForbidden {
		t.Fatalf("status %d, want 403 for a refused credential", w.Code)
	}
	srv.SetMarsMinter(&fakeMarsMinter{err: &wyzeapi.MarsHTTPError{StatusCode: 500, Status: "500"}})
	w = httptest.NewRecorder()
	srv.handleShimCameraToken(w, newShimReq("GET", "/internal/wyze/Camera/CameraToken?deviceId=BEBEBEBEBEBE"))
	if w.Code != http.StatusBadGateway {
		t.Fatalf("status %d, want 502 for a server error", w.Code)
	}
}
