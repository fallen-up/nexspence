package service

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"strconv"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

// Package managers (npm, pnpm, Maven, pip) send the password with every
// request. One bcrypt compare at the default cost 12 takes about 250 ms of
// CPU, and an LDAP login opens a connection and binds twice. A single
// `pnpm install` therefore made the server CPU-bound. A password that verified
// recently skips that work for the configured TTL (auth.basic_auth_cache_ttl).
const defaultBasicAuthTTL = time.Minute

// lastLoginInterval limits the users.last_login write on the data routes to
// one per user per interval, instead of one per request.
const lastLoginInterval = time.Minute

// basicAuthMaxEntries bounds both maps. Only verified credentials enter the
// cache, so a password-guessing client cannot fill it. When it is full, a new
// credential is not cached and is verified on every request, as before.
const basicAuthMaxEntries = 10_000

type basicAuthKey [sha256.Size]byte

// basicAuthCache remembers which Basic-auth credentials verified recently.
// It is per process: each replica verifies a credential once per TTL.
type basicAuthCache struct {
	// secret keys the HMAC, so the map never holds anything that can be
	// tested against a guessed password outside this process.
	secret []byte
	ttl    time.Duration // 0 turns the cache off
	now    func() time.Time
	flight singleflight.Group

	mu        sync.Mutex
	verified  map[basicAuthKey]time.Time // key → expiry
	lastLogin map[string]time.Time       // username → last write
}

func newBasicAuthCache(ttl time.Duration) *basicAuthCache {
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		panic("basic auth cache: " + err.Error())
	}
	return &basicAuthCache{
		secret:    secret,
		ttl:       ttl,
		now:       time.Now,
		verified:  make(map[basicAuthKey]time.Time),
		lastLogin: make(map[string]time.Time),
	}
}

// key binds the password to the stored state that proves it. For a local
// user that is the password hash: a password change writes a new hash, so
// the entry for the old password can no longer match.
func (c *basicAuthCache) key(username, password, proof string) basicAuthKey {
	m := hmac.New(sha256.New, c.secret)
	// Length-prefixed (netstring), so no two (username, password) pairs
	// encode to the same bytes.
	for _, part := range []string{username, password, proof} {
		m.Write([]byte(strconv.Itoa(len(part)) + ":" + part + ","))
	}
	var k basicAuthKey
	copy(k[:], m.Sum(nil))
	return k
}

func (c *basicAuthCache) hit(k basicAuthKey) bool {
	if c.ttl <= 0 {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	exp, ok := c.verified[k]
	if !ok {
		return false
	}
	if !c.now().Before(exp) {
		delete(c.verified, k)
		return false
	}
	return true
}

func (c *basicAuthCache) store(k basicAuthKey) {
	if c.ttl <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	if len(c.verified) >= basicAuthMaxEntries {
		for old, exp := range c.verified {
			if !now.Before(exp) {
				delete(c.verified, old)
			}
		}
		if len(c.verified) >= basicAuthMaxEntries {
			return
		}
	}
	c.verified[k] = now.Add(c.ttl)
}

// verify runs check once for all concurrent callers with the same key: a
// client opens many connections at once with the same password. A successful
// check is cached, a failed one is not.
func (c *basicAuthCache) verify(k basicAuthKey, check func() (any, error)) (any, error) {
	v, err, _ := c.flight.Do(string(k[:]), func() (any, error) {
		v, err := check()
		if err == nil {
			c.store(k)
		}
		return v, err
	})
	return v, err
}

// lastLoginDue reports whether the last_login write for username is due, and
// records it as done when it is.
func (c *basicAuthCache) lastLoginDue(username string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	if t, ok := c.lastLogin[username]; ok && now.Sub(t) < lastLoginInterval {
		return false
	}
	if len(c.lastLogin) >= basicAuthMaxEntries {
		clear(c.lastLogin)
	}
	c.lastLogin[username] = now
	return true
}
