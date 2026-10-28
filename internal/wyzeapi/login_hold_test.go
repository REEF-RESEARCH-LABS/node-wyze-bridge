package wyzeapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// loginServer answers every password sign-in with `answer` and lists one
// camera for a signed-in client.
func loginServer(t *testing.T, answer func(w http.ResponseWriter)) (*Client, *atomic.Int32, *atomic.Int32) {
	t.Helper()
	logins, refreshes := &atomic.Int32{}, &atomic.Int32{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/user/login":
			logins.Add(1)
			answer(w)
		case "/user/refresh_token":
			refreshes.Add(1)
			json.NewEncoder(w).Encode(map[string]interface{}{"code": "1", "data": map[string]interface{}{
				"access_token": "refreshed", "refresh_token": "refreshed_ref"}})
		case "/v2/home_page/get_object_list":
			json.NewEncoder(w).Encode(map[string]interface{}{"code": "1", "data": map[string]interface{}{
				"device_list": []interface{}{map[string]interface{}{
					"product_type": "Camera", "product_model": "HL_CAM4", "mac": "AABBCCDDEEFF",
					"nickname": "Tank", "enr": "test_enr", "device_params": map[string]interface{}{"p2p_id": "UID1", "ip": "10.0.0.5"},
				}}}})
		default:
			http.Error(w, "unexpected "+r.URL.Path, http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	c := NewClient(Credentials{Email: "a@b.com", Password: "p", APIID: "i", APIKey: "k"}, "v", zerolog.Nop())
	c.AuthURL = srv.URL
	c.WyzeURL = srv.URL
	return c, logins, refreshes
}

func refuse(w http.ResponseWriter) {
	json.NewEncoder(w).Encode(map[string]interface{}{"code": "1000",
		"msg": "Invalid credentials, please check username, password, keyid or apikey"})
}

func tooMany(w http.ResponseWriter) {
	w.WriteHeader(http.StatusTooManyRequests) // Wyze's 429 comes with an empty body
}

func welcome(w http.ResponseWriter) {
	json.NewEncoder(w).Encode(map[string]interface{}{"code": "1", "data": map[string]interface{}{
		"access_token": "tok", "refresh_token": "ref", "user_id": "uid"}})
}

// One bridge start lists cameras twice before anything else (the
// discovery before go2rtc, then the discovery loop's first pass). On
// 2026-10-26 each pass signed in, so every failed start cost two sign-ins
// and an owner's Wyze account locked.
func TestOneStartSignsInOnceWhenWyzeRefuses(t *testing.T) {
	for name, answer := range map[string]func(http.ResponseWriter){"refused": refuse, "too many": tooMany} {
		t.Run(name, func(t *testing.T) {
			c, logins, _ := loginServer(t, answer)
			failures := 0
			c.SetAuthObserver(func(error) { failures++ }, nil)
			for i := 0; i < 3; i++ { // setupGo2RTC, RunDiscoveryLoop, an owner's Discover
				if _, err := c.GetCameraList(); err == nil {
					t.Fatalf("pass %d: listed cameras with a failed sign-in", i)
				}
			}
			if n := logins.Load(); n != 1 {
				t.Fatalf("sign-ins = %d, want 1", n)
			}
			if failures != 1 {
				t.Fatalf("auth failures reported = %d, want 1 (a held try keeps Wyze's own answer)", failures)
			}
		})
	}
}

func TestAHeldSignInSaysWhatWyzeAnswered(t *testing.T) {
	c, _, _ := loginServer(t, refuse)
	c.GetCameraList()
	_, err := c.GetCameraList()
	var held *LoginHeldError
	if !errors.As(err, &held) || !held.Refused {
		t.Fatalf("err = %v, want a refused hold", err)
	}
	var apiErr *WyzeAPIError
	if !errors.As(err, &apiErr) || apiErr.Code != "1000" {
		t.Fatalf("held error lost Wyze's answer: %v", err)
	}
}

// A refused login (wrong details) is never tried again in this run by
// itself: the next start of the bridge, or ClearLoginHold, tries once.
func TestARefusedLoginIsNotTriedAgainThisRun(t *testing.T) {
	c, logins, _ := loginServer(t, refuse)
	c.GetCameraList()
	c.loginHeldUntil = time.Now().Add(-time.Minute) // well past any timed hold
	c.GetCameraList()
	if n := logins.Load(); n != 1 {
		t.Fatalf("sign-ins = %d, want 1", n)
	}
}

// Too many tries says nothing about the login: the discovery refresh
// after the hold tries once more, and a good answer lists cameras.
func TestTooManyTriesIsTriedAgainOnlyAfterTheHold(t *testing.T) {
	answer := tooMany
	c, logins, _ := loginServer(t, func(w http.ResponseWriter) { answer(w) })
	c.GetCameraList()
	if c.loginHeldUntil.Before(time.Now().Add(DefaultLoginHolds[0] - time.Minute)) {
		t.Fatalf("held until %v, want about %v from now", c.loginHeldUntil, DefaultLoginHolds[0])
	}
	c.GetCameraList()
	if n := logins.Load(); n != 1 {
		t.Fatalf("sign-ins inside the hold = %d, want 1", n)
	}
	answer = welcome
	c.loginHeldUntil = time.Now().Add(-time.Second)
	cams, err := c.GetCameraList()
	if err != nil || len(cams) != 1 {
		t.Fatalf("after the hold: cams=%v err=%v", cams, err)
	}
	if n := logins.Load(); n != 2 {
		t.Fatalf("sign-ins = %d, want 2", n)
	}
	if c.loginErr != nil || c.loginRefused {
		t.Fatal("a good sign-in must clear the hold")
	}
}

// A saved token that has run out is refreshed, never a password sign-in.
func TestAnExpiredSavedTokenIsRefreshedNotSignedIn(t *testing.T) {
	c, logins, refreshes := loginServer(t, refuse)
	c.SetAuth(&AuthState{AccessToken: "old", RefreshToken: "ref", PhoneID: "ph", ExpiresAt: time.Now().Add(-time.Hour)})
	if _, err := c.GetCameraList(); err != nil {
		t.Fatalf("GetCameraList: %v", err)
	}
	if logins.Load() != 0 || refreshes.Load() != 1 {
		t.Fatalf("logins=%d refreshes=%d, want 0 and 1", logins.Load(), refreshes.Load())
	}
}

func answerCode(code string) func(http.ResponseWriter) {
	return func(w http.ResponseWriter) {
		json.NewEncoder(w).Encode(map[string]interface{}{"code": code, "msg": "no"})
	}
}

// A passing failure is tried again after a hold that grows each time
// (2, 5, 10, 20, then 30 minutes), never left for good. 3019 and 3024
// (TOTP, locked account) heal with time, so they are held, not refused.
func TestPassingFailuresAreHeldLongerEachTime(t *testing.T) {
	for _, code := range []string{"429", "1003", "3019", "3024"} {
		t.Run(code, func(t *testing.T) {
			answer := tooMany
			if code != "429" {
				answer = answerCode(code)
			}
			c, logins, _ := loginServer(t, answer)
			want := []time.Duration{2, 5, 10, 20, 30, 30}
			for i, m := range want {
				c.GetCameraList()
				if n := int(logins.Load()); n != i+1 {
					t.Fatalf("try %d: sign-ins = %d", i, n)
				}
				st := c.LoginStatus()
				left := time.Until(st.HoldUntil)
				if st.State != "hold" || left < m*time.Minute-time.Minute || left > m*time.Minute {
					t.Fatalf("try %d: status %+v (hold %v), want hold of %d min", i, st, left, m)
				}
				c.GetCameraList() // inside the hold: no sign-in
				c.loginMu.Lock()
				c.loginHeldUntil = time.Now().Add(-time.Second)
				c.loginMu.Unlock()
			}
		})
	}
}

func TestLoginStatusSaysRefusedAndClearLoginHoldTriesAgain(t *testing.T) {
	answer := refuse
	c, logins, _ := loginServer(t, func(w http.ResponseWriter) { answer(w) })
	c.GetCameraList()
	st := c.LoginStatus()
	if st.State != "refused" || !st.HoldUntil.IsZero() || st.Error == "" {
		t.Fatalf("status = %+v, want refused with Wyze's words", st)
	}
	answer = welcome
	c.ClearLoginHold()
	if _, err := c.GetCameraList(); err != nil {
		t.Fatalf("after ClearLoginHold: %v", err)
	}
	if logins.Load() != 2 || c.LoginStatus().State != "ok" {
		t.Fatalf("logins=%d status=%+v", logins.Load(), c.LoginStatus())
	}
}

// Wyze rejecting the token mid-call (2001) is mended with the refresh
// token; the password is not used.
func TestARejectedTokenIsRefreshedNotSignedIn(t *testing.T) {
	c, logins, refreshes := loginServer(t, refuse)
	c.SetAuth(&AuthState{AccessToken: "old", RefreshToken: "ref", PhoneID: "ph", ExpiresAt: time.Now().Add(time.Hour)})
	first := true
	resp, err := c.withFreshAuth(func() (map[string]interface{}, error) {
		if first {
			first = false
			return nil, &WyzeAPIError{Code: "2001"}
		}
		return map[string]interface{}{"token": c.auth.AccessToken}, nil
	})
	if err != nil || resp["token"] != "refreshed" {
		t.Fatalf("resp=%v err=%v", resp, err)
	}
	if logins.Load() != 0 || refreshes.Load() != 1 {
		t.Fatalf("logins=%d refreshes=%d, want 0 and 1", logins.Load(), refreshes.Load())
	}
}
