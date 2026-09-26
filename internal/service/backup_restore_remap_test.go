package service_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nexspence-oss/nexspence/internal/domain"
	"github.com/nexspence-oss/nexspence/internal/testutil"
)

// idAssigningBlobStoreRepo assigns a fresh id on Create, the way the real
// table does (INSERT … RETURNING id). testutil.BlobStoreRepo keeps whatever
// id the caller passed — "" after restoreBlobStores clears it — which is
// exactly what hid the old-id → new-id remap bug from every existing test.
type idAssigningBlobStoreRepo struct {
	*testutil.BlobStoreRepo
	mu     sync.Mutex
	n      int
	failOn string // Create of a store with this name fails
}

func (r *idAssigningBlobStoreRepo) Create(ctx context.Context, s *domain.BlobStore) error {
	if s.Name == r.failOn {
		return errors.New("simulated create failure")
	}
	r.mu.Lock()
	r.n++
	if s.ID == "" {
		s.ID = fmt.Sprintf("restored-bs-%d", r.n)
	}
	r.mu.Unlock()
	return r.BlobStoreRepo.Create(ctx, s)
}

// countingBlobStoreRepo counts GetByID calls.
type countingBlobStoreRepo struct {
	*testutil.BlobStoreRepo
	mu    sync.Mutex
	calls int
}

func (r *countingBlobStoreRepo) GetByID(ctx context.Context, id string) (*domain.BlobStore, error) {
	r.mu.Lock()
	r.calls++
	r.mu.Unlock()
	return r.BlobStoreRepo.GetByID(ctx, id)
}

// exportWithS3Repo builds a source instance holding one repo pinned to a
// non-default store ("s3-prod", id "bs-old") with one asset, and exports it.
func exportWithS3Repo(t *testing.T) *bytes.Buffer {
	t.Helper()
	ctx := context.Background()
	oldID := "bs-old"
	repo := testutil.SimpleRepo("s3repo", "raw")
	repo.BlobStoreID = &oldID

	src := buildBackupSvc(repo)
	store := testutil.NewBlobStore()
	src.Resolver = testutil.NewFakeResolver(store)
	require.NoError(t, src.BlobStores.Create(ctx, &domain.BlobStore{ID: "bs-default", Name: "default", Type: "local"}))
	require.NoError(t, src.BlobStores.Create(ctx, &domain.BlobStore{ID: oldID, Name: "s3-prod", Type: "s3", Config: map[string]any{"bucket": "prod"}}))

	content := []byte("artifact-on-s3")
	require.NoError(t, store.Put(ctx, "ab/cd/k1", bytes.NewReader(content), int64(len(content))))
	comp := &domain.Component{RepositoryID: repo.ID, Repository: repo.Name, Format: "raw", Name: "f.txt", Version: "1"}
	require.NoError(t, src.Components.Create(ctx, comp))
	require.NoError(t, src.Assets.Create(ctx, &domain.Asset{
		ComponentID: comp.ID, RepositoryID: repo.ID, Repository: repo.Name,
		Path: "/f.txt", BlobKey: "ab/cd/k1", BlobStoreID: oldID,
		SizeBytes: int64(len(content)), ContentType: "text/plain",
	}))

	var buf bytes.Buffer
	require.NoError(t, src.Export(ctx, &buf))
	return &buf
}

// Disaster recovery: the destination is empty, so the non-default store is
// re-created with a NEW id. The repo and its asset must follow it by name.
// Before the fix the remap map was keyed by the new id, the repo kept the
// stale archived id (an FK violation on a real database, so the whole repo
// was silently dropped) and its asset landed on an arbitrary store.
func TestBackup_Restore_RecreatedStoreGetsNewID_RepoAndAssetsFollowIt(t *testing.T) {
	ctx := context.Background()
	buf := exportWithS3Repo(t)

	dst := buildBackupSvc()
	stores := &idAssigningBlobStoreRepo{BlobStoreRepo: testutil.NewBlobStoreRepo()}
	dst.BlobStores = stores
	dstStore := testutil.NewBlobStore()
	dst.Resolver = testutil.NewFakeResolver(dstStore)

	stats, err := dst.Restore(ctx, buf)
	require.NoError(t, err)
	// "default" already exists on the destination (seeded) and is skipped;
	// only "s3-prod" has to be re-created.
	assert.Equal(t, 1, stats.BlobStores)
	assert.Equal(t, 1, stats.Repos)

	recreated, err := stores.Get(ctx, "s3-prod")
	require.NoError(t, err)
	require.NotEqual(t, "bs-old", recreated.ID, "precondition: the store must come back under a new id")

	repo, err := dst.Repos.Get(ctx, "s3repo")
	require.NoError(t, err)
	require.NotNil(t, repo.BlobStoreID)
	assert.Equal(t, recreated.ID, *repo.BlobStoreID, "repo must point at the re-created store, not the archived id")

	asset, err := dst.Assets.GetByPath(ctx, "s3repo", "/f.txt")
	require.NoError(t, err)
	require.NotNil(t, asset)
	assert.Equal(t, recreated.ID, asset.BlobStoreID, "asset must point at the re-created store")

	rc, _, err := dstStore.Get(ctx, "ab/cd/k1")
	require.NoError(t, err, "blob bytes must be written to the resolved store")
	_ = rc.Close()
}

// When the archived store cannot be restored at all, the repo must still be
// restored — on the default store — instead of being dropped over a stale id,
// and its assets must go to "default" too, not to an arbitrary store.
func TestBackup_Restore_UnrestorableStore_FallsBackToDefault(t *testing.T) {
	ctx := context.Background()
	buf := exportWithS3Repo(t)

	dst := buildBackupSvc()
	stores := &idAssigningBlobStoreRepo{BlobStoreRepo: testutil.NewBlobStoreRepo(), failOn: "s3-prod"}
	dst.BlobStores = stores
	require.NoError(t, stores.BlobStoreRepo.Create(ctx, &domain.BlobStore{ID: "dst-default", Name: "default", Type: "local"}))
	require.NoError(t, stores.BlobStoreRepo.Create(ctx, &domain.BlobStore{ID: "dst-aaa", Name: "aaa-first-by-name", Type: "local"}))
	dst.Resolver = testutil.NewFakeResolver(testutil.NewBlobStore())

	stats, err := dst.Restore(ctx, buf)
	require.NoError(t, err)
	assert.Equal(t, 1, stats.Repos)

	repo, err := dst.Repos.Get(ctx, "s3repo")
	require.NoError(t, err)
	assert.Nil(t, repo.BlobStoreID, "an unmappable store id must be dropped so the repo uses the default store")

	asset, err := dst.Assets.GetByPath(ctx, "s3repo", "/f.txt")
	require.NoError(t, err)
	require.NotNil(t, asset)
	assert.Equal(t, "dst-default", asset.BlobStoreID)
}

// storeFor resolves each store once per Export, not once per asset.
func TestBackup_Export_ResolvesEachStoreOncePerOperation(t *testing.T) {
	ctx := context.Background()
	repo := testutil.SimpleRepo("many", "raw")
	svc := buildBackupSvc(repo)
	counting := &countingBlobStoreRepo{BlobStoreRepo: testutil.NewBlobStoreRepo()}
	svc.BlobStores = counting
	store := testutil.NewBlobStore()
	svc.Resolver = testutil.NewFakeResolver(store)
	require.NoError(t, counting.Create(ctx, &domain.BlobStore{ID: "bs-s3", Name: "s3", Type: "s3"}))

	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("ab/cd/k%d", i)
		require.NoError(t, store.Put(ctx, key, bytes.NewReader([]byte("x")), 1))
		comp := &domain.Component{RepositoryID: repo.ID, Repository: repo.Name, Format: "raw", Name: fmt.Sprintf("f%d", i), Version: "1"}
		require.NoError(t, svc.Components.Create(ctx, comp))
		require.NoError(t, svc.Assets.Create(ctx, &domain.Asset{
			ComponentID: comp.ID, RepositoryID: repo.ID, Repository: repo.Name,
			Path: fmt.Sprintf("/f%d", i), BlobKey: key, BlobStoreID: "bs-s3", SizeBytes: 1,
		}))
	}

	var buf bytes.Buffer
	require.NoError(t, svc.Export(ctx, &buf))
	assert.Equal(t, 1, counting.calls, "5 assets on one store must cost one blob_stores lookup")
}

// A group store names its members by id. After a disaster-recovery restore
// its members come back under new ids, so the group's member_ids must be
// remapped too — otherwise the restored group points at stores that no
// longer exist. The archive lists the group before its member ("a-group" <
// "z-member") to prove creation order does not matter.
func TestBackup_Restore_GroupMembersRemappedToNewIDs(t *testing.T) {
	ctx := context.Background()
	src := buildBackupSvc()
	require.NoError(t, src.BlobStores.Create(ctx, &domain.BlobStore{ID: "old-member", Name: "z-member", Type: "s3"}))
	require.NoError(t, src.BlobStores.Create(ctx, &domain.BlobStore{
		ID: "old-group", Name: "a-group", Type: "group",
		Config: map[string]any{"member_ids": []any{"old-member", "old-gone"}, "fill_policy": "round_robin"},
	}))
	var buf bytes.Buffer
	require.NoError(t, src.Export(ctx, &buf))

	dst := buildBackupSvc()
	stores := &idAssigningBlobStoreRepo{BlobStoreRepo: testutil.NewBlobStoreRepo()}
	dst.BlobStores = stores
	stats, err := dst.Restore(ctx, &buf)
	require.NoError(t, err)
	assert.Equal(t, 2, stats.BlobStores)

	member, err := stores.Get(ctx, "z-member")
	require.NoError(t, err)
	group, err := stores.Get(ctx, "a-group")
	require.NoError(t, err)
	assert.Equal(t, []string{member.ID}, group.Config["member_ids"],
		"member_ids must hold the member's new id; an id that exists nowhere is dropped")
	assert.Equal(t, "round_robin", group.Config["fill_policy"])
}
