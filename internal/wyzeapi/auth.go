package wyzeapi

import (
	"crypto/hmac"
	"crypto/md5"
	cryptoRand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// Default Wyze API base URLs (production).
const (
	DefaultAuthAPI  = "https://auth-prod.api.wyze.com"
	DefaultWyzeAPI  = "https://api.wyzecam.com/app"
	DefaultCloudAPI = "https://app-core.cloud.wyze.com/app"
	// DefaultNewWyzeAPI is the "v4" app host used for modern endpoints
	// like /v4/camera/get_streams which mints KVS/WebRTC signaling URLs
	// per camera. Distinct from DefaultWyzeAPI (legacy v2 host) and
	// DefaultCloudAPI (app-core host used for /v4/device/get_event_list).
	DefaultNewWyzeAPI = "https://app.wyzecam.com/app"

	appVersion = "3.6.5.5"
	iosVersion = "17.7.2"

	scaleUserAgent = "Wyze/" + appVersion + " (iPhone; iOS " + iosVersion + "; Scale/3.00)"
	wyzeAppAPIKey  = "WMXHYf79Nr5gIlt3r0r7p9Tcw5bvs6BB4U8O8nGJ"
)

// Endpoint-specific sc/sv values for Wyze API.
var scSV = map[string][2]string{
	"default":         {"9f275790cab94a72bd206c8876429f3c", "e1fe392906d54888a9b99b88de4162d7"},
	"run_action":      {"01dd431d098546f9baf5233724fa2ee2", "2c0edc06d4c5465b8c55af207144f0d9"},
	"get_device_Info": {"01dd431d098546f9baf5233724fa2ee2", "0bc2c3bedf6c4be688754c9ad42bbf2e"},
	"get_event_list":  {"9f275790cab94a72bd206c8876429f3c", "782ced6909a44d92a1f70d582bbe88be"},
	"set_device_Info": {"01dd431d098546f9baf5233724fa2ee2", "e8e1db44128f4e31a2047a8f5f80b2bd"},
}

// Known app_id → secret mappings.
var appKeys = map[string]string{
	"9319141212m2ik": "wyze_app_secret_key_132",
}

// Credentials holds the Wyze account credentials needed for login.
type Credentials struct {
	Email    string
	Password string // plain or "md5:"-prefixed triple-hash
	APIID    string
	APIKey   string
	TOTPKey  string
}

// IsSet returns true if all required credentials are present.
func (c Credentials) IsSet() bool {
	return c.Email != "" && c.Password != "" && c.APIID != "" && c.APIKey != ""
}

// AuthState holds the current authentication tokens.
type AuthState struct {
	AccessToken  string    `json:"access_token"`
	RefreshToken string    `json:"refresh_token"`
	UserID       string    `json:"user_id"`
	PhoneID      string    `json:"phone_id"`
	ExpiresAt    time.Time `json:"expires_at"`
}

// WyzeAccount holds user profile information.
type WyzeAccount struct {
	PhoneID    string `json:"phone_id"`
	Email      string `json:"email"`
	Nickname   string `json:"nickname"`
	UserCode   string `json:"user_code"`
	OpenUserID string `json:"open_user_id"`
	Logo       string `json:"logo"`
}

// Client is the Wyze API client.
type Client struct {
	log           zerolog.Logger
	httpClient    *http.Client
	creds         Credentials
	auth          *AuthState
	reloginMu     sync.Mutex // serializes login, refresh, and forced re-login
	bridgeVer     string
	metrics       *apiMetrics
	onAuthFailure func(error) // optional; fires when EnsureAuth exhausts retries
	onAuthRecover func()      // optional; fires when EnsureAuth succeeds after a prior failure
	AuthURL       string      // base URL for auth endpoints (default: DefaultAuthAPI)
	WyzeURL       string      // base URL for wyze app endpoints (default: DefaultWyzeAPI)
	CloudURL      string      // base URL for cloud endpoints (default: DefaultCloudAPI)
	NewWyzeURL    string      // base URL for "v4" app endpoints incl. get_streams (default: DefaultNewWyzeAPI)

	// Password sign-in hold (Node One, 2026-10-26). Guarded by reloginMu.
	loginRetryAfter time.Duration // how long a failed sign-in holds the next one
	loginErr        error         // the last failed sign-in, nil after a good one
	loginHeldUntil  time.Time     // no password sign-in before this
	loginRefused    bool          // Wyze refused the login itself: none again this run
}

// SetAuthObserver attaches callbacks for auth lifecycle events.
// Either may be nil.
func (c *Client) SetAuthObserver(onFailure func(error), onRecover func()) {
	c.onAuthFailure = onFailure
	c.onAuthRecover = onRecover
}

// NewClient creates a new Wyze API client.
func NewClient(creds Credentials, bridgeVersion string, log zerolog.Logger) *Client {
	return &Client{
		log:        log,
		httpClient: &http.Client{Timeout: 30 * time.Second},
		creds:      creds,
		bridgeVer:  bridgeVersion,
		metrics:    newAPIMetrics(),
		AuthURL:    DefaultAuthAPI,
		WyzeURL:    DefaultWyzeAPI,
		CloudURL:   DefaultCloudAPI,
		NewWyzeURL: DefaultNewWyzeAPI,
		// Under REFRESH_INTERVAL's 30 min default, so a sign-in Wyze
		// turned away for a passing reason is tried again on the next
		// discovery refresh, never sooner.
		loginRetryAfter: DefaultLoginRetryAfter,
	}
}

// Auth returns the current auth state (may be nil).
func (c *Client) Auth() *AuthState {
	return c.auth
}

// SetAuth sets the auth state (e.g., restored from state file).
func (c *Client) SetAuth(auth *AuthState) {
	c.auth = auth
}

// Login authenticates with the Wyze API.
func (c *Client) Login() (*AuthState, error) {
	c.log.Info().Msg("logging in to Wyze API")

	headers := c.loginHeaders()
	body := map[string]interface{}{
		"email":    strings.TrimSpace(c.creds.Email),
		"password": hashPassword(c.creds.Password),
	}

	resp, err := c.postJSON(c.AuthURL+"/api/user/login", headers, body)
	if err != nil {
		return nil, fmt.Errorf("login: %w", err)
	}

	auth := &AuthState{
		PhoneID: headers["phone-id"],
	}
	if at, ok := resp["access_token"].(string); ok {
		auth.AccessToken = at
	}
	if rt, ok := resp["refresh_token"].(string); ok {
		auth.RefreshToken = rt
	}
	if uid, ok := resp["user_id"].(string); ok {
		auth.UserID = uid
	}

	// Check for MFA requirement
	if auth.AccessToken == "" {
		mfaOpts, _ := resp["mfa_options"].([]interface{})
		if len(mfaOpts) > 0 && c.creds.TOTPKey != "" {
			c.log.Info().Msg("MFA required, attempting TOTP verification")
			mfaDetails, _ := resp["mfa_details"].(map[string]interface{})
			return c.completeMFA(auth, mfaDetails)
		}
		if len(mfaOpts) > 0 {
			return nil, fmt.Errorf("MFA required but no TOTP key configured — set WYZE_TOTP_KEY env var")
		}
	}

	// Token usually valid for ~24h; we'll refresh at ExpiresAt - 5min
	auth.ExpiresAt = time.Now().Add(23 * time.Hour)

	c.auth = auth
	c.log.Info().Msg("login successful")
	return auth, nil
}

// completeMFA completes MFA login using TOTP.
func (c *Client) completeMFA(auth *AuthState, mfaDetails map[string]interface{}) (*AuthState, error) {
	totpCode, err := generateTOTP(c.creds.TOTPKey)
	if err != nil {
		return nil, fmt.Errorf("generate TOTP: %w", err)
	}

	appID := ""
	if mfaDetails != nil {
		appID, _ = mfaDetails["totp_apps"].(string)
		if appID == "" {
			appID, _ = mfaDetails["app_id"].(string)
		}
	}

	body := map[string]interface{}{
		"email":             strings.TrimSpace(c.creds.Email),
		"password":          hashPassword(c.creds.Password),
		"mfa_type":          "TotpVerificationCode",
		"verification_id":   appID,
		"verification_code": totpCode,
	}

	headers := map[string]string{
		"X-API-Key":  wyzeAppAPIKey,
		"phone-id":   auth.PhoneID,
		"user-agent": "wyze_ios_" + appVersion,
	}

	resp, err := c.postJSON(c.AuthURL+"/user/login", headers, body)
	if err != nil {
		return nil, fmt.Errorf("mfa login: %w", err)
	}

	if at, ok := resp["access_token"].(string); ok {
		auth.AccessToken = at
	}
	if rt, ok := resp["refresh_token"].(string); ok {
		auth.RefreshToken = rt
	}
	if uid, ok := resp["user_id"].(string); ok {
		auth.UserID = uid
	}

	auth.ExpiresAt = time.Now().Add(23 * time.Hour)
	c.auth = auth
	c.log.Info().Msg("MFA login successful")
	return auth, nil
}

// RefreshToken refreshes the access token.
func (c *Client) RefreshToken() (*AuthState, error) {
	if c.auth == nil {
		return nil, fmt.Errorf("no auth state to refresh")
	}

	c.log.Info().Msg("refreshing access token")

	payload := c.authenticatedPayload("default")
	payload["refresh_token"] = c.auth.RefreshToken

	resp, err := c.postJSON(c.WyzeURL+"/user/refresh_token", c.defaultHeaders(), payload)
	if err != nil {
		return nil, fmt.Errorf("refresh_token: %w", err)
	}

	if at, ok := resp["access_token"].(string); ok {
		c.auth.AccessToken = at
	}
	if rt, ok := resp["refresh_token"].(string); ok {
		c.auth.RefreshToken = rt
	}
	c.auth.ExpiresAt = time.Now().Add(23 * time.Hour)

	c.log.Info().Msg("token refreshed")
	return c.auth, nil
}

// NeedsRefresh returns true if the token should be refreshed.
func (c *Client) NeedsRefresh() bool {
	if c.auth == nil {
		return true
	}
	return time.Now().After(c.auth.ExpiresAt.Add(-5 * time.Minute))
}

// EnsureAuth ensures we have a valid token, refreshing if needed.
// Fires the auth observer on failure / recovery.
func (c *Client) EnsureAuth() error {
	c.reloginMu.Lock()
	defer c.reloginMu.Unlock()
	return c.ensureAuthLocked()
}

func (c *Client) ensureAuthLocked() error {
	if c.auth == nil {
		c.log.Debug().Msg("no auth state, initiating login")
		if err := c.passwordLogin(); err != nil {
			c.notifyLoginFailure(fmt.Errorf("initial login: %w", err))
			return err
		}
		c.notifyAuthRecover()
		return nil
	}
	if c.NeedsRefresh() {
		// The refresh token first: it costs no password sign-in.
		if c.auth.RefreshToken != "" {
			c.log.Debug().Time("expires_at", c.auth.ExpiresAt).Msg("token nearing expiry, refreshing")
			_, err := c.RefreshToken()
			if err == nil {
				c.notifyAuthRecover()
				return nil
			}
			c.log.Warn().Err(err).Msg("token refresh failed, re-logging in")
		}
		// Keep the phone id so the next login stays the same device.
		c.auth = &AuthState{PhoneID: c.auth.PhoneID}
		if err := c.passwordLogin(); err != nil {
			c.notifyLoginFailure(fmt.Errorf("token refresh + relogin both failed: %w", err))
			return err
		}
		c.notifyAuthRecover()
	}
	return nil
}

// forceRelogin drops a token Wyze has rejected and logs in again.
// staleToken is the access token that just failed. If another caller
// already replaced it, this returns nil and the caller retries.
func (c *Client) forceRelogin(staleToken string) error {
	c.reloginMu.Lock()
	defer c.reloginMu.Unlock()
	if c.auth != nil && c.auth.AccessToken != "" && c.auth.AccessToken != staleToken {
		return nil
	}
	phoneID := ""
	if c.auth != nil {
		phoneID = c.auth.PhoneID
	}
	// Keep the phone id so the next login stays the same device.
	c.auth = &AuthState{PhoneID: phoneID}
	if err := c.passwordLogin(); err != nil {
		c.auth = &AuthState{PhoneID: phoneID, ExpiresAt: time.Now().Add(-time.Hour)}
		c.notifyLoginFailure(fmt.Errorf("re-login after expired access token: %w", err))
		return err
	}
	c.notifyAuthRecover()
	return nil
}

// DefaultLoginRetryAfter is how long a failed password sign-in holds
// the next one.
const DefaultLoginRetryAfter = 20 * time.Minute

// LoginHeldError is returned instead of signing in again while a failed
// sign-in holds the next one. Last is that failure.
type LoginHeldError struct {
	Last    error
	Until   time.Time // zero when held until the bridge restarts
	Refused bool
}

func (e *LoginHeldError) Error() string {
	if e.Refused {
		return fmt.Sprintf("Wyze sign-in not tried again until the bridge restarts (Wyze refused the login: %v)", e.Last)
	}
	return fmt.Sprintf("Wyze sign-in not tried again before %s (last try: %v)", e.Until.Format(time.RFC3339), e.Last)
}

func (e *LoginHeldError) Unwrap() error { return e.Last }

// passwordLogin is the only door to a password sign-in. Caller holds
// reloginMu.
//
// Wyze locks an account after repeated failed sign-ins. On 2026-10-26 a
// Node One owner was locked out: every start signed in twice, once for
// the discovery before go2rtc and again for the discovery loop's first
// pass a moment later, because a failed Login left no auth and the next
// caller simply tried again. Now a failed sign-in holds every later one
// for loginRetryAfter, and a refusal of the login itself (wrong
// password or key, locked account) for the rest of the run. The next
// start of the bridge tries once.
func (c *Client) passwordLogin() error {
	if c.loginErr != nil && (c.loginRefused || time.Now().Before(c.loginHeldUntil)) {
		held := &LoginHeldError{Last: c.loginErr, Refused: c.loginRefused}
		if !c.loginRefused {
			held.Until = c.loginHeldUntil
		}
		c.log.Info().Err(c.loginErr).Bool("refused", c.loginRefused).Time("until", held.Until).
			Msg("not signing in to Wyze again yet; the last sign-in failed")
		return held
	}
	if _, err := c.Login(); err != nil {
		c.loginErr = err
		c.loginRefused = isLoginRefused(err)
		c.loginHeldUntil = time.Now().Add(c.loginRetryAfter)
		return err
	}
	c.loginErr, c.loginRefused, c.loginHeldUntil = nil, false, time.Time{}
	return nil
}

// isLoginRefused is true when Wyze turned the login itself away, so the
// same login can only be refused again (and feed Wyze's lockout).
// 1000 is what Wyze answers a wrong email, password, Key ID or API Key
// with ("Invalid credentials, please check username, password, keyid or
// apikey"), whatever its generic hint says.
func isLoginRefused(err error) bool {
	var apiErr *WyzeAPIError
	if !errors.As(err, &apiErr) {
		return false
	}
	switch apiErr.Code {
	case "1000", "1001", "1003", "3019", "3024":
		return true
	}
	return false
}

// notifyLoginFailure reports a failed sign-in, but not a held one: the
// issue already says what Wyze answered, and a hold would hide it.
func (c *Client) notifyLoginFailure(err error) {
	var held *LoginHeldError
	if errors.As(err, &held) {
		return
	}
	c.notifyAuthFailure(err)
}

func (c *Client) notifyAuthFailure(err error) {
	if c.onAuthFailure != nil {
		c.onAuthFailure(err)
	}
}

func (c *Client) notifyAuthRecover() {
	if c.onAuthRecover != nil {
		c.onAuthRecover()
	}
}

// GetUserInfo retrieves the authenticated user's profile.
func (c *Client) GetUserInfo() (*WyzeAccount, error) {
	resp, err := c.withFreshAuth(func() (map[string]interface{}, error) {
		return c.postJSON(c.WyzeURL+"/user/get_user_info", c.defaultHeaders(), c.authenticatedPayload("default"))
	})
	if err != nil {
		return nil, fmt.Errorf("get_user_info: %w", err)
	}

	account := &WyzeAccount{
		PhoneID: c.auth.PhoneID,
	}
	if v, ok := resp["email"].(string); ok {
		account.Email = v
	}
	if v, ok := resp["nickname"].(string); ok {
		account.Nickname = v
	}
	if v, ok := resp["user_code"].(string); ok {
		account.UserCode = v
	}
	if v, ok := resp["open_user_id"].(string); ok {
		account.OpenUserID = v
	}
	if v, ok := resp["logo"].(string); ok {
		account.Logo = v
	}

	return account, nil
}

// loginHeaders returns headers for the login endpoint specifically.
func (c *Client) loginHeaders() map[string]string {
	var phoneID string
	if c.auth != nil {
		phoneID = c.auth.PhoneID
	}
	if phoneID == "" {
		phoneID = generatePhoneID()
	}
	return map[string]string{
		"apikey":     c.creds.APIKey,
		"keyid":      c.creds.APIID,
		"user-agent": fmt.Sprintf("docker-wyze-bridge/%s", c.bridgeVer),
		"phone-id":   phoneID,
	}
}

// defaultHeaders returns standard headers for authenticated API calls.
func (c *Client) defaultHeaders() map[string]string {
	return map[string]string{
		"user-agent": scaleUserAgent,
		"appversion": appVersion,
		"env":        "prod",
	}
}

// authenticatedPayload returns the standard payload fields for authenticated endpoints.
func (c *Client) authenticatedPayload(endpoint string) map[string]interface{} {
	sv := scSV["default"]
	if v, ok := scSV[endpoint]; ok {
		sv = v
	}
	return map[string]interface{}{
		"sc":                sv[0],
		"sv":                sv[1],
		"app_ver":           "com.hualai.WyzeCam___" + appVersion,
		"app_version":       appVersion,
		"app_name":          "com.hualai.WyzeCam",
		"phone_system_type": 1,
		"ts":                time.Now().UnixMilli(),
		"access_token":      c.auth.AccessToken,
		"phone_id":          c.auth.PhoneID,
	}
}

// signPayloadHeaders returns headers with HMAC signature for v4 API calls.
func (c *Client) signPayloadHeaders(appID string, payload string) map[string]string {
	return map[string]string{
		"content-type": "application/json",
		"phoneid":      c.auth.PhoneID,
		"user-agent":   "wyze_ios_" + appVersion,
		"appinfo":      "wyze_ios_" + appVersion,
		"appversion":   appVersion,
		"access_token": c.auth.AccessToken,
		"appid":        appID,
		"env":          "prod",
		"signature2":   signMsg(appID, payload, c.auth.AccessToken),
	}
}

// hashPassword performs triple MD5 hashing of the password.
// Supports "md5:" or "hashed:" prefix to skip hashing.
func hashPassword(password string) string {
	password = strings.TrimSpace(password)
	for _, prefix := range []string{"hashed:", "md5:"} {
		if strings.HasPrefix(strings.ToLower(password), prefix) {
			return password[len(prefix):]
		}
	}
	encoded := password
	for i := 0; i < 3; i++ {
		h := md5.Sum([]byte(encoded))
		encoded = hex.EncodeToString(h[:])
	}
	return encoded
}

// signMsg computes the HMAC-MD5 signature used by Wyze API v4.
//
//	key = MD5(token + secret)
//	sig = HMAC-MD5(key, msg)
func signMsg(appID, msg, token string) string {
	secret := os.Getenv(appID)
	if secret == "" {
		if s, ok := appKeys[appID]; ok {
			secret = s
		} else {
			secret = appID
		}
	}
	keySource := token + secret
	keyHash := md5.Sum([]byte(keySource))
	key := []byte(hex.EncodeToString(keyHash[:]))

	mac := hmac.New(md5.New, key)
	mac.Write([]byte(msg))
	return hex.EncodeToString(mac.Sum(nil))
}

// sortDict produces a deterministic JSON string with sorted keys.
func sortDict(payload map[string]interface{}) string {
	// Collect keys and sort
	keys := make([]string, 0, len(payload))
	for k := range payload {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Build JSON manually to match Python's separators=(",", ":")
	var b strings.Builder
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		kb, _ := json.Marshal(k)
		b.Write(kb)
		b.WriteByte(':')
		vb, _ := json.Marshal(payload[k])
		b.Write(vb)
	}
	b.WriteByte('}')
	return b.String()
}

func generatePhoneID() string {
	b := make([]byte, 16)
	cryptoRand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40 // Version 4
	b[8] = (b[8] & 0x3f) | 0x80 // Variant 10
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
