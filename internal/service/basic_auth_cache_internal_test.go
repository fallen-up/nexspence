package service

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/nexspence-oss/nexspence/internal/auth"
	"github.com/nexspence-oss/nexspence/internal/config"
	"github.com/nexspence-oss/nexspence/internal/domain"
	"github.com/nexspence-oss/nexspence/internal/testutil"
)

// lastLoginCountingRepo counts the last_login writes, which the fake repo
// otherwise ignores.
type lastLoginCountingRepo struct {
	*testutil.UserRepo
	lastLogins atomic.Int32
}

func (r *lastLoginCountingRepo) UpdateLastLogin(ctx context.Context, username string) error {
	r.lastLogins.Add(1)
	return r.UserRepo.UpdateLastLogin(ctx, username)
}

// countingLDAP accepts one password and counts the binds.
type countingLDAP struct {
	password string
	binds    atomic.Int32
}

func (l *countingLDAP) Authenticate(_ context.Context, username, password string) (*auth.LDAPUser, error) {
	l.binds.Add(1)
	if password != l.password {
		return nil, errors.New("invalid credentials")
	}
	return &auth.LDAPUser{Username: username}, nil
}

func (l *countingLDAP) TestConnection(context.Context) error { return nil }

type verifyFixture struct {
	svc      *UserService
	repo     *lastLoginCountingRepo
	compares *atomic.Int32
	now      time.Time
}

// newVerifyFixture builds a UserService that counts its bcrypt compares and
// reads a clock that the test moves with advance.
func newVerifyFixture(t *testing.T, users ...*domain.User) *verifyFixture {
	t.Helper()
	authSvc := auth.NewService("test-secret-32-chars-long-here!!", 1, 4)
	f := &verifyFixture{
		repo:     &lastLoginCountingRepo{UserRepo: testutil.NewUserRepo(users...)},
		compares: &atomic.Int32{},
		now:      time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	f.svc = NewUserService(f.repo, testutil.NewRoleRepo(), authSvc, zap.NewNop().Sugar())
	f.svc.checkPassword = func(hash, plain string) error {
		f.compares.Add(1)
		return authSvc.CheckPassword(hash, plain)
	}
	f.svc.basic.now = func() time.Time { return f.now }
	return f
}

func (f *verifyFixture) advance(d time.Duration) { f.now = f.now.Add(d) }

func localVerifyUser(t *testing.T, username, password string) *domain.User {
	t.Helper()
	hash, err := auth.NewService("test-secret-32-chars-long-here!!", 1, 4).HashPassword(password)
	require.NoError(t, err)
	return &domain.User{
		ID:           "id-" + username,
		Username:     username,
		PasswordHash: hash,
		Status:       domain.UserStatusActive,
		Source:       domain.UserSourceLocal,
	}
}

// disable stores a disabled copy, so the fixture's own pointer stays intact.
func disable(t *testing.T, repo *lastLoginCountingRepo, username string) {
	t.Helper()
	u, err := repo.Get(context.Background(), username)
	require.NoError(t, err)
	disabled := *u
	disabled.Status = domain.UserStatusDisabled
	require.NoError(t, repo.Update(context.Background(), &disabled))
}

func TestVerifyPassword_SecondCallSkipsBcrypt(t *testing.T) {
	f := newVerifyFixture(t, localVerifyUser(t, "alice", "secret-1"))
	ctx := context.Background()

	for range 3 {
		u, err := f.svc.VerifyPassword(ctx, "alice", "secret-1")
		require.NoError(t, err)
		assert.Equal(t, "alice", u.Username)
	}
	assert.Equal(t, int32(1), f.compares.Load())
}

func TestVerifyPassword_WrongPasswordIsNotCached(t *testing.T) {
	f := newVerifyFixture(t, localVerifyUser(t, "alice", "secret-1"))
	ctx := context.Background()

	for range 2 {
		_, err := f.svc.VerifyPassword(ctx, "alice", "wrong")
		require.Error(t, err)
	}
	assert.Equal(t, int32(2), f.compares.Load(), "every wrong password must cost a compare")

	_, err := f.svc.VerifyPassword(ctx, "alice", "secret-1")
	require.NoError(t, err)
}

func TestVerifyPassword_PasswordChangeRefusesOldPassword(t *testing.T) {
	f := newVerifyFixture(t, localVerifyUser(t, "alice", "secret-1"))
	ctx := context.Background()

	_, err := f.svc.VerifyPassword(ctx, "alice", "secret-1")
	require.NoError(t, err)
	require.NoError(t, f.svc.SetPassword(ctx, "alice", "secret-2"))

	_, err = f.svc.VerifyPassword(ctx, "alice", "secret-1")
	require.Error(t, err, "the cache entry for the old password must not match the new hash")
	_, err = f.svc.VerifyPassword(ctx, "alice", "secret-2")
	require.NoError(t, err)
}

func TestVerifyPassword_DisabledUserRefusedWhileCached(t *testing.T) {
	f := newVerifyFixture(t, localVerifyUser(t, "alice", "secret-1"))
	ctx := context.Background()

	_, err := f.svc.VerifyPassword(ctx, "alice", "secret-1")
	require.NoError(t, err)
	disable(t, f.repo, "alice")

	_, err = f.svc.VerifyPassword(ctx, "alice", "secret-1")
	require.ErrorIs(t, err, ErrInvalidInput)
}

func TestVerifyPassword_UnknownUser(t *testing.T) {
	f := newVerifyFixture(t)

	_, err := f.svc.VerifyPassword(context.Background(), "ghost", "secret-1")
	require.ErrorIs(t, err, ErrNotFound)
}

func TestVerifyPassword_EntryExpiresAfterTTL(t *testing.T) {
	f := newVerifyFixture(t, localVerifyUser(t, "alice", "secret-1"))
	ctx := context.Background()

	_, err := f.svc.VerifyPassword(ctx, "alice", "secret-1")
	require.NoError(t, err)
	f.advance(defaultBasicAuthTTL - time.Second)
	_, err = f.svc.VerifyPassword(ctx, "alice", "secret-1")
	require.NoError(t, err)
	require.Equal(t, int32(1), f.compares.Load())

	f.advance(time.Second)
	_, err = f.svc.VerifyPassword(ctx, "alice", "secret-1")
	require.NoError(t, err)
	assert.Equal(t, int32(2), f.compares.Load())
}

func TestVerifyPassword_ZeroTTLTurnsCacheOff(t *testing.T) {
	f := newVerifyFixture(t, localVerifyUser(t, "alice", "secret-1"))
	f.svc.WithBasicAuthCacheTTL(0)

	for range 3 {
		_, err := f.svc.VerifyPassword(context.Background(), "alice", "secret-1")
		require.NoError(t, err)
	}
	assert.Equal(t, int32(3), f.compares.Load())
}

func TestVerifyPassword_ConcurrentCallsShareOneCompare(t *testing.T) {
	f := newVerifyFixture(t, localVerifyUser(t, "alice", "secret-1"))
	check := f.svc.checkPassword
	f.svc.checkPassword = func(hash, plain string) error {
		// Hold the compare long enough for every goroutine to join it.
		time.Sleep(100 * time.Millisecond)
		return check(hash, plain)
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for range 16 {
		wg.Go(func() {
			<-start
			_, err := f.svc.VerifyPassword(context.Background(), "alice", "secret-1")
			errs <- err
		})
	}
	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
	assert.Equal(t, int32(1), f.compares.Load())
}

func TestVerifyPassword_WritesLastLoginOncePerInterval(t *testing.T) {
	f := newVerifyFixture(t, localVerifyUser(t, "alice", "secret-1"))
	ctx := context.Background()

	for range 3 {
		_, err := f.svc.VerifyPassword(ctx, "alice", "secret-1")
		require.NoError(t, err)
	}
	require.Equal(t, int32(1), f.repo.lastLogins.Load())

	f.advance(lastLoginInterval)
	_, err := f.svc.VerifyPassword(ctx, "alice", "secret-1")
	require.NoError(t, err)
	assert.Equal(t, int32(2), f.repo.lastLogins.Load())
}

func TestBasicAuthCache_KeyIsLengthPrefixed(t *testing.T) {
	c := newBasicAuthCache(time.Minute)

	assert.NotEqual(t, c.key("ab", "c", "h"), c.key("a", "bc", "h"))
	assert.NotEqual(t, c.key("a", "b", "c"), newBasicAuthCache(time.Minute).key("a", "b", "c"),
		"each process must use its own secret")
}

func TestBasicAuthCache_FullCacheStoresOnlyAfterExpiredEntriesGo(t *testing.T) {
	c := newBasicAuthCache(time.Minute)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	c.now = func() time.Time { return now }
	for i := range basicAuthMaxEntries {
		c.store(c.key("user", strconv.Itoa(i), "h"))
	}

	extra := c.key("user", "extra", "h")
	c.store(extra)
	assert.False(t, c.hit(extra), "a full cache of live entries must not grow")

	now = now.Add(time.Minute)
	c.store(extra)
	assert.True(t, c.hit(extra), "expired entries must make room")
	assert.Len(t, c.verified, 1)
}

func TestBasicAuthCache_LastLoginMapIsBounded(t *testing.T) {
	c := newBasicAuthCache(time.Minute)
	for i := range basicAuthMaxEntries {
		require.True(t, c.lastLoginDue(strconv.Itoa(i)))
	}

	assert.True(t, c.lastLoginDue("one-more"))
	assert.Len(t, c.lastLogin, 1)
}

// ── LDAP ──────────────────────────────────────────────────────

func newLDAPVerifyFixture(t *testing.T, users ...*domain.User) (*verifyFixture, *countingLDAP) {
	t.Helper()
	f := newVerifyFixture(t, users...)
	ldap := &countingLDAP{password: "dir-secret"}
	f.svc.WithLDAP(ldap, config.LDAPConfig{})
	return f, ldap
}

func TestVerifyPassword_LDAP_FirstCallCreatesUserThenSkipsBind(t *testing.T) {
	f, ldap := newLDAPVerifyFixture(t)
	ctx := context.Background()

	for range 3 {
		u, err := f.svc.VerifyPassword(ctx, "svc-ci", "dir-secret")
		require.NoError(t, err)
		assert.Equal(t, "svc-ci", u.Username)
	}
	assert.Equal(t, int32(1), ldap.binds.Load())

	stored, err := f.repo.Get(ctx, "svc-ci")
	require.NoError(t, err)
	assert.Equal(t, domain.UserSourceLDAP, stored.Source)
}

func TestVerifyPassword_LDAP_WrongPasswordIsNotCached(t *testing.T) {
	f, ldap := newLDAPVerifyFixture(t)
	ctx := context.Background()

	_, err := f.svc.VerifyPassword(ctx, "svc-ci", "dir-secret")
	require.NoError(t, err)
	for range 2 {
		_, err := f.svc.VerifyPassword(ctx, "svc-ci", "wrong")
		require.Error(t, err)
	}
	assert.Equal(t, int32(3), ldap.binds.Load())
}

func TestVerifyPassword_LDAP_DisabledUserRefusedWhileCached(t *testing.T) {
	f, _ := newLDAPVerifyFixture(t)
	ctx := context.Background()

	_, err := f.svc.VerifyPassword(ctx, "svc-ci", "dir-secret")
	require.NoError(t, err)
	disable(t, f.repo, "svc-ci")

	_, err = f.svc.VerifyPassword(ctx, "svc-ci", "dir-secret")
	require.ErrorIs(t, err, ErrInvalidInput)
}

func TestVerifyPassword_LDAP_NameCaseSharesOneEntry(t *testing.T) {
	f, ldap := newLDAPVerifyFixture(t)
	ctx := context.Background()

	for _, name := range []string{"SvcCI", "svcci", "SVCCI"} {
		u, err := f.svc.VerifyPassword(ctx, name, "dir-secret")
		require.NoError(t, err, name)
		assert.Equal(t, "svcci", u.Username)
	}
	assert.Equal(t, int32(1), ldap.binds.Load())
}

func TestVerifyPassword_LDAP_LocalAccountConflictIsRefused(t *testing.T) {
	f, _ := newLDAPVerifyFixture(t, localVerifyUser(t, "bob", "secret-1"))

	// "Bob" is unknown, so it routes to LDAP, and its lowercased name is a
	// local account that a directory entry must not log in as.
	for range 2 {
		_, err := f.svc.VerifyPassword(context.Background(), "Bob", "dir-secret")
		require.ErrorIs(t, err, ErrProvisioningConflict)
	}
}
