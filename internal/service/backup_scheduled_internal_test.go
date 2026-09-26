package service

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/nexspence-oss/nexspence/internal/distlock"
	"github.com/nexspence-oss/nexspence/internal/domain"
	"github.com/nexspence-oss/nexspence/internal/storage"
	"github.com/nexspence-oss/nexspence/internal/testutil"
)

// White-box (package service) so runOnce — the actual cron-triggered path —
// can be exercised directly instead of waiting on a real schedule.

func newInternalBackupSvc(repos ...*domain.Repository) (*BackupService, *testutil.BackupSettingsRepo, *testutil.AuditRepo) {
	settings := testutil.NewBackupSettingsRepo()
	audit := testutil.NewAuditRepo()
	svc := &BackupService{
		BlobStores: testutil.NewBlobStoreRepo(),
		Repos:      testutil.NewRepoRepo(repos...),
		Users:      testutil.NewUserRepo(),
		Roles:      testutil.NewRoleRepo(),
		Policies:   testutil.NewCleanupPolicyRepo(),
		Components: testutil.NewComponentRepo(),
		Assets:     testutil.NewAssetRepo(),
		BlobStore:  testutil.NewBlobStore(),
	}
	svc.WithSettings(settings).WithAudit(audit)
	return svc, settings, audit
}

func TestBackupService_RunOnce_Success_WritesAuditEvent(t *testing.T) {
	ctx := context.Background()
	svc, settings, audit := newInternalBackupSvc(testutil.SimpleRepo("r1", "raw"))

	dest := testutil.NewBlobStore()
	svc.Resolver = testutil.NewFakeResolver(dest)
	bs := &domain.BlobStore{ID: "bs-1", Name: "dest", Type: "s3"}
	if err := svc.BlobStores.Create(ctx, bs); err != nil {
		t.Fatalf("create blob store: %v", err)
	}
	if err := settings.Upsert(ctx, &domain.BackupSettings{Enabled: true, ScheduleCron: "0 3 * * *", BlobStoreID: bs.ID, RetentionCount: 7}); err != nil {
		t.Fatalf("upsert settings: %v", err)
	}

	svc.runOnce(ctx)

	events := audit.Snapshot()
	if len(events) != 1 {
		t.Fatalf("audit events = %d, want 1", len(events))
	}
	e := events[0]
	if e.Result != "success" {
		t.Errorf("Result = %q, want success", e.Result)
	}
	if e.Domain != "SYSTEM" || e.Action != "BACKUP" {
		t.Errorf("Domain/Action = %q/%q, want SYSTEM/BACKUP", e.Domain, e.Action)
	}
	if e.EntityName == "" {
		t.Error("EntityName (the backup key) must not be empty on success")
	}
}

func TestBackupService_RunOnce_Failure_WritesAuditEventWithError(t *testing.T) {
	ctx := context.Background()
	svc, settings, audit := newInternalBackupSvc(testutil.SimpleRepo("r1", "raw"))
	svc.Resolver = failingResolverForInternalTest{}

	bs := &domain.BlobStore{ID: "bs-broken", Name: "broken", Type: "s3"}
	if err := svc.BlobStores.Create(ctx, bs); err != nil {
		t.Fatalf("create blob store: %v", err)
	}
	if err := settings.Upsert(ctx, &domain.BackupSettings{Enabled: true, ScheduleCron: "0 3 * * *", BlobStoreID: bs.ID, RetentionCount: 7}); err != nil {
		t.Fatalf("upsert settings: %v", err)
	}

	svc.runOnce(ctx)

	events := audit.Snapshot()
	if len(events) != 1 {
		t.Fatalf("audit events = %d, want 1", len(events))
	}
	e := events[0]
	if e.Result != "failure" {
		t.Errorf("Result = %q, want failure", e.Result)
	}
	if e.Context["error"] == nil || e.Context["error"] == "" {
		t.Error("Context[\"error\"] must carry the failure reason")
	}
}

func TestBackupService_RunOnce_NoopDoesNotWriteAuditEvent(t *testing.T) {
	ctx := context.Background()
	svc, _, audit := newInternalBackupSvc()

	svc.runOnce(ctx) // disabled by default (Settings.Get returns Enabled=false)

	if got := len(audit.Snapshot()); got != 0 {
		t.Errorf("audit events = %d, want 0 for a disabled/no-destination no-op", got)
	}
}

type failingResolverForInternalTest struct{}

func (failingResolverForInternalTest) Get(context.Context, storage.BlobStoreDescriptor) (storage.BlobStore, error) {
	return nil, errors.New("resolver: simulated failure")
}

// newInternalBackupSvcWithDest wires a resolvable destination store and
// enabled settings on schedule cronExpr.
func newInternalBackupSvcWithDest(t *testing.T, cronExpr string) (*BackupService, *testutil.BackupSettingsRepo, *testutil.AuditRepo) {
	t.Helper()
	ctx := context.Background()
	svc, settings, audit := newInternalBackupSvc(testutil.SimpleRepo("r1", "raw"))
	svc.Resolver = testutil.NewFakeResolver(testutil.NewBlobStore())
	bs := &domain.BlobStore{ID: "bs-1", Name: "dest", Type: "s3"}
	if err := svc.BlobStores.Create(ctx, bs); err != nil {
		t.Fatalf("create blob store: %v", err)
	}
	if err := settings.Upsert(ctx, &domain.BackupSettings{Enabled: true, ScheduleCron: cronExpr, BlobStoreID: bs.ID, RetentionCount: 7}); err != nil {
		t.Fatalf("upsert settings: %v", err)
	}
	return svc, settings, audit
}

// A destination deleted after scheduling was enabled must surface as a
// recorded failure on every tick, not stop backups silently.
func TestBackupService_RunOnce_NoDestination_RecordsFailure(t *testing.T) {
	ctx := context.Background()
	svc, settings, audit := newInternalBackupSvc()
	if err := settings.Upsert(ctx, &domain.BackupSettings{Enabled: true, ScheduleCron: "0 3 * * *", RetentionCount: 7}); err != nil {
		t.Fatalf("upsert settings: %v", err)
	}

	svc.runOnce(ctx)

	events := audit.Snapshot()
	if len(events) != 1 || events[0].Result != "failure" {
		t.Fatalf("audit events = %+v, want one failure", events)
	}
	got, _ := settings.Get(ctx)
	if got.LastRunError == "" || got.LastRunAt == nil {
		t.Errorf("last run must record the failure, got at=%v err=%q", got.LastRunAt, got.LastRunError)
	}
}

// A replica still holding the previous schedule's cron entry (it did not
// serve the PUT) must not run at the old time.
func TestBackupService_RunScheduledTick_StaleScheduleDoesNotRun(t *testing.T) {
	ctx := context.Background()
	svc, _, audit := newInternalBackupSvcWithDest(t, "0 3 * * *")

	svc.runScheduledTick(ctx, "0 1 * * *")

	if got := len(audit.Snapshot()); got != 0 {
		t.Errorf("audit events = %d, want 0 for a stale cron entry", got)
	}
}

func TestBackupService_RunScheduledTick_CurrentScheduleRuns(t *testing.T) {
	ctx := context.Background()
	svc, _, audit := newInternalBackupSvcWithDest(t, "0 3 * * *")

	svc.runScheduledTick(ctx, "0 3 * * *")

	if events := audit.Snapshot(); len(events) != 1 || events[0].Result != "success" {
		t.Fatalf("audit events = %+v, want one success", events)
	}
}

// Two replicas firing the same cron slot: the lock only covers a run in
// progress, so the second one must notice the slot already has a run.
func TestBackupService_RunOnce_SkipsASlotThatAlreadyRan(t *testing.T) {
	ctx := context.Background()
	svc, settings, audit := newInternalBackupSvcWithDest(t, "0 3 * * *")
	if err := settings.RecordRun(ctx, time.Now(), "backups/other-replica.tar.gz", ""); err != nil {
		t.Fatalf("record run: %v", err)
	}

	svc.runOnce(ctx)

	if got := len(audit.Snapshot()); got != 0 {
		t.Errorf("audit events = %d, want 0: this slot already has a run", got)
	}
}

func TestBackupService_RunOnce_RunsWhenLastRunIsAnEarlierSlot(t *testing.T) {
	ctx := context.Background()
	svc, settings, audit := newInternalBackupSvcWithDest(t, "* * * * *")
	if err := settings.RecordRun(ctx, time.Now().Add(-2*time.Minute), "backups/earlier.tar.gz", ""); err != nil {
		t.Fatalf("record run: %v", err)
	}

	svc.runOnce(ctx)

	if events := audit.Snapshot(); len(events) != 1 || events[0].Result != "success" {
		t.Fatalf("audit events = %+v, want one success", events)
	}
}

// last_run_at is the run's start, so a long export cannot push it into the
// next cron slot and make that slot look already done.
func TestBackupService_RunOnce_RecordsStartTime(t *testing.T) {
	ctx := context.Background()
	svc, settings, _ := newInternalBackupSvcWithDest(t, "0 3 * * *")
	before := time.Now()

	svc.runOnce(ctx)

	got, _ := settings.Get(ctx)
	if got.LastRunAt == nil || got.LastRunAt.Before(before) {
		t.Fatalf("LastRunAt = %v, want >= %v", got.LastRunAt, before)
	}
	if got.LastRunKey == "" {
		t.Error("LastRunKey must be set after a successful run")
	}
}

// syncSchedule is how a replica that did not serve the PUT learns about a
// change: enabling (it had no entry at all), a new schedule, disabling.
func TestBackupService_SyncSchedule_FollowsStoredSettings(t *testing.T) {
	ctx := context.Background()
	svc, settings, _ := newInternalBackupSvc()
	svc.sched.cronScheduler = cron.New()
	registered := func() (string, bool) {
		svc.sched.mu.Lock()
		defer svc.sched.mu.Unlock()
		return svc.sched.registered, svc.sched.hasEntry
	}
	set := func(enabled bool, expr string) {
		t.Helper()
		if err := settings.Upsert(ctx, &domain.BackupSettings{Enabled: enabled, ScheduleCron: expr, BlobStoreID: "bs-1", RetentionCount: 7}); err != nil {
			t.Fatalf("upsert: %v", err)
		}
	}

	set(true, "0 3 * * *") // enabled through another replica
	svc.syncSchedule(ctx)
	if got, ok := registered(); !ok || got != "0 3 * * *" {
		t.Fatalf("after enable: registered=%q hasEntry=%v", got, ok)
	}
	idBefore := svc.sched.entryID

	svc.syncSchedule(ctx) // nothing changed
	if svc.sched.entryID != idBefore {
		t.Error("an unchanged schedule must not be re-registered")
	}

	set(true, "0 1 * * *")
	svc.syncSchedule(ctx)
	if got, _ := registered(); got != "0 1 * * *" {
		t.Fatalf("after change: registered=%q", got)
	}
	if n := len(svc.sched.cronScheduler.Entries()); n != 1 {
		t.Errorf("cron entries = %d, want 1 (the old schedule must be removed)", n)
	}

	set(false, "0 1 * * *")
	svc.syncSchedule(ctx)
	if got, ok := registered(); ok || got != "" {
		t.Fatalf("after disable: registered=%q hasEntry=%v", got, ok)
	}
	if n := len(svc.sched.cronScheduler.Entries()); n != 0 {
		t.Errorf("cron entries = %d, want 0", n)
	}
}

// A run still going on this replica — e.g. from the entry of a schedule that
// has since changed, which SkipIfStillRunning does not know about — must make
// the next tick skip, not start a second export next to it.
func TestBackupService_RunOnce_SkipsWhileAnotherRunIsInProgress(t *testing.T) {
	ctx := context.Background()
	svc, settings, audit := newInternalBackupSvcWithDest(t, "* * * * *")
	svc.sched.running.Store(true)

	svc.runOnce(ctx)

	if got := len(audit.Snapshot()); got != 0 {
		t.Errorf("audit events = %d, want 0 while a run is in progress", got)
	}
	if got, _ := settings.Get(ctx); got.LastRunAt != nil {
		t.Errorf("LastRunAt = %v, want nil: nothing may have run", got.LastRunAt)
	}
	if !svc.sched.running.Load() {
		t.Error("a skipped tick must not clear the in-progress run's flag")
	}
}

// refreshLock is a distlock.Lock + distlock.Refresher whose Refresh returns
// the queued results in order, then keeps returning the last one.
type refreshLock struct {
	mu      sync.Mutex
	results []error
	calls   int
}

func (l *refreshLock) Release(context.Context) error { return nil }

func (l *refreshLock) Refresh(context.Context, time.Duration) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls++
	if len(l.results) == 0 {
		return nil
	}
	err := l.results[0]
	if len(l.results) > 1 {
		l.results = l.results[1:]
	}
	return err
}

func (l *refreshLock) Calls() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.calls
}

type singleLockLocker struct{ lock distlock.Lock }

func (s singleLockLocker) Acquire(context.Context, string, time.Duration) (distlock.Lock, error) {
	return s.lock, nil
}
func (singleLockLocker) ForceRelease(context.Context, string) error { return nil }

func shortLockRefresh(t *testing.T) {
	t.Helper()
	prev := backupLockRefresh
	backupLockRefresh = 5 * time.Millisecond
	t.Cleanup(func() { backupLockRefresh = prev })
}

func TestBackupService_KeepLock_RefreshesUntilLost(t *testing.T) {
	shortLockRefresh(t)
	svc, _, _ := newInternalBackupSvc()
	// A transient error is retried; only a lost lock stops the run.
	lock := &refreshLock{results: []error{nil, errors.New("redis timeout"), nil, distlock.ErrLockLost}}

	ctx, stop := svc.keepLock(context.Background(), lock)
	defer stop()
	select {
	case <-ctx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("run context was not canceled after the lock was lost")
	}
	if cause := context.Cause(ctx); !errors.Is(cause, distlock.ErrLockLost) {
		t.Errorf("cause = %v, want ErrLockLost", cause)
	}
	if got := lock.Calls(); got != 4 {
		t.Errorf("refresh calls = %d, want 4", got)
	}
}

func TestBackupService_KeepLock_StopEndsRefreshing(t *testing.T) {
	shortLockRefresh(t)
	svc, _, _ := newInternalBackupSvc()
	lock := &refreshLock{}

	ctx, stop := svc.keepLock(context.Background(), lock)
	time.Sleep(30 * time.Millisecond)
	stop()
	if lock.Calls() == 0 {
		t.Fatal("lock was never refreshed")
	}
	if !errors.Is(context.Cause(ctx), context.Canceled) {
		t.Errorf("after stop, cause = %v, want context.Canceled", context.Cause(ctx))
	}
	time.Sleep(20 * time.Millisecond)
	after := lock.Calls()
	time.Sleep(30 * time.Millisecond)
	if got := lock.Calls(); got != after {
		t.Errorf("refresh calls went %d → %d after stop", after, got)
	}
}

// blockingGetStore holds every read until the caller's context ends, standing
// in for an export that outlives its lock.
type blockingGetStore struct{ *testutil.BlobStore }

func (blockingGetStore) Get(ctx context.Context, _ string) (io.ReadCloser, int64, error) {
	<-ctx.Done()
	return nil, 0, ctx.Err()
}

// ctxCheckingSettings fails RecordRun on a canceled context, as Postgres does.
type ctxCheckingSettings struct{ *testutil.BackupSettingsRepo }

func (s ctxCheckingSettings) RecordRun(ctx context.Context, at time.Time, key, runErr string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.BackupSettingsRepo.RecordRun(ctx, at, key, runErr)
}

// A run that loses its lock mid-export stops, and is still recorded as the
// failure it is — not left looking like it never happened.
func TestBackupService_RunOnce_LockLostMidExport_StopsAndRecordsFailure(t *testing.T) {
	shortLockRefresh(t)
	ctx := context.Background()
	svc, settings, audit := newInternalBackupSvcWithDest(t, "0 3 * * *")
	svc.Settings = ctxCheckingSettings{settings}
	svc.Resolver = testutil.NewFakeResolver(blockingGetStore{testutil.NewBlobStore()})
	svc.WithLocker(singleLockLocker{&refreshLock{results: []error{distlock.ErrLockLost}}})
	comp := &domain.Component{RepositoryID: "repo-r1", Repository: "r1", Format: "raw", Name: "a", Version: "1"}
	if err := svc.Components.Create(ctx, comp); err != nil {
		t.Fatal(err)
	}
	if err := svc.Assets.Create(ctx, &domain.Asset{ComponentID: comp.ID, RepositoryID: "repo-r1", Repository: "r1", Path: "/a", BlobKey: "aa/bb/a", BlobStoreID: "bs-1"}); err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() { svc.runOnce(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("runOnce did not stop after losing its lock")
	}

	got, _ := settings.Get(ctx)
	if got.LastRunError == "" || !strings.Contains(got.LastRunError, "lock no longer held") {
		t.Errorf("LastRunError = %q, want the lost lock as the cause", got.LastRunError)
	}
	if events := audit.Snapshot(); len(events) != 1 || events[0].Result != "failure" {
		t.Errorf("audit events = %+v, want one failure", events)
	}
}
