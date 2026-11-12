// Package identity issues in-memory Ed25519 tokens for the lab audience.
// The private key is not written to disk. No OIDC provider is contacted.
package identity

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"strings"
	"time"

	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/apperr"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/clock"
	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/reason"
)

const IssuerName = "https://syn.lab/perf-lab"

type Claims struct {
	Issuer    string `json:"iss"`
	Subject   string `json:"sub"`
	Audience  string `json:"aud"`
	ExpiresAt int64  `json:"exp"`
	IssuedAt  int64  `json:"iat"`
	TenantID  string `json:"tenant_id"`
}

type Issuer struct {
	clock   *clock.Clock
	public  ed25519.PublicKey
	private ed25519.PrivateKey
}

func NewIssuer(clk *clock.Clock) (*Issuer, error) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	return &Issuer{clock: clk, public: public, private: private}, nil
}

func (i *Issuer) Issue(subject, audience, tenant string, ttl time.Duration) (string, error) {
	now := i.clock.Now()
	claims := Claims{
		Issuer: IssuerName, Subject: subject, Audience: audience, TenantID: tenant,
		IssuedAt: now.Unix(), ExpiresAt: now.Add(ttl).Unix(),
	}
	body, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"EdDSA","typ":"JWT"}`))
	payload := base64.RawURLEncoding.EncodeToString(body)
	signed := header + "." + payload
	sig := ed25519.Sign(i.private, []byte(signed))
	return signed + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}

func (i *Issuer) Parse(raw string) (Claims, error) {
	parts := strings.Split(raw, ".")
	if len(parts) != 3 {
		return Claims{}, apperr.New(reason.TokenInvalid, "token is not a compact JWT", 401)
	}
	signed := parts[0] + "." + parts[1]
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || !ed25519.Verify(i.public, []byte(signed), sig) {
		return Claims{}, apperr.New(reason.SignatureInvalid, "token signature did not verify", 401)
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return Claims{}, apperr.New(reason.TokenInvalid, "token payload is not valid", 401)
	}
	var claims Claims
	if err := json.Unmarshal(payload, &claims); err != nil {
		return Claims{}, apperr.New(reason.TokenInvalid, "token payload is not valid JSON", 401)
	}
	if claims.Issuer != IssuerName || claims.Subject == "" || claims.Audience == "" {
		return Claims{}, apperr.New(reason.TokenInvalid, "token claims are incomplete", 401)
	}
	if i.clock.Now().Unix() >= claims.ExpiresAt {
		return Claims{}, apperr.New(reason.TokenExpired, "token is expired", 401)
	}
	return claims, nil
}

// ParseFor verifies the token and checks the lab audience and tenant.
func (i *Issuer) ParseFor(raw, audience, tenant string) (Claims, error) {
	claims, err := i.Parse(raw)
	if err != nil {
		return Claims{}, err
	}
	if audience != "" && claims.Audience != audience {
		return Claims{}, apperr.New(reason.AudienceMismatch, "token audience does not match", 401)
	}
	if tenant != "" && claims.TenantID != tenant {
		return Claims{}, apperr.New(reason.TokenInvalid, "token tenant does not match", 401)
	}
	return claims, nil
}
