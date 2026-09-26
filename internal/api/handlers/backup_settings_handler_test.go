package handlers_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nexspence-oss/nexspence/internal/api/handlers"
	"github.com/nexspence-oss/nexspence/internal/domain"
	"github.com/nexspence-oss/nexspence/internal/service"
	"github.com/nexspence-oss/nexspence/internal/testutil"
)

const (
	destStoreID  = "11111111-1111-1111-1111-111111111111"
	groupStoreID = "22222222-2222-2222-2222-222222222222"
)

func buildSettingsHandler(t *testing.T) (*handlers.BackupHandler, *testutil.BackupSettingsRepo) {
	t.Helper()
	settings := testutil.NewBackupSettingsRepo()
	svc := &service.BackupService{
		BlobStores: testutil.NewBlobStoreRepo(
			&domain.BlobStore{ID: destStoreID, Name: "backups", Type: "s3"},
			&domain.BlobStore{ID: groupStoreID, Name: "grp", Type: "group"},
		),
		BlobStore: testutil.NewBlobStore(),
	}
	svc.WithSettings(settings)
	return handlers.NewBackupHandler(svc), settings
}

func putSettings(t *testing.T, h *handlers.BackupHandler, body map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	r := ginTestRouter()
	r.PUT("/api/v1/backup/settings", h.UpdateSettings)
	b, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPut, "/api/v1/backup/settings", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func TestBackupHandler_UpdateSettings_Valid(t *testing.T) {
	h, settings := buildSettingsHandler(t)
	w := putSettings(t, h, map[string]any{"enabled": true, "scheduleCron": " 0 3 * * * ", "blobStoreId": destStoreID, "retentionCount": 5})
	require.Equal(t, http.StatusNoContent, w.Code, w.Body.String())

	got, err := settings.Get(context.Background())
	require.NoError(t, err)
	assert.True(t, got.Enabled)
	assert.Equal(t, "0 3 * * *", got.ScheduleCron, "surrounding whitespace is trimmed")
	assert.Equal(t, destStoreID, got.BlobStoreID)
	assert.Equal(t, 5, got.RetentionCount)
}

func TestBackupHandler_UpdateSettings_DisabledWithoutStoreIsFine(t *testing.T) {
	h, _ := buildSettingsHandler(t)
	w := putSettings(t, h, map[string]any{"enabled": false, "scheduleCron": "0 3 * * *", "retentionCount": 7})
	assert.Equal(t, http.StatusNoContent, w.Code, w.Body.String())
}

// Every invalid body is rejected before anything is saved, so a working
// schedule is never replaced by a broken one.
func TestBackupHandler_UpdateSettings_RejectsInvalidWithoutSaving(t *testing.T) {
	cases := []struct {
		name string
		body map[string]any
		want string
	}{
		{"invalid cron", map[string]any{"enabled": true, "scheduleCron": "* * * *", "blobStoreId": destStoreID, "retentionCount": 7}, "invalid scheduleCron"},
		{"empty cron while enabled", map[string]any{"enabled": true, "scheduleCron": "", "blobStoreId": destStoreID, "retentionCount": 7}, "scheduleCron is required"},
		{"enabled without store", map[string]any{"enabled": true, "scheduleCron": "0 3 * * *", "retentionCount": 7}, "blobStoreId is required"},
		{"store id not a uuid", map[string]any{"enabled": true, "scheduleCron": "0 3 * * *", "blobStoreId": "not-a-uuid", "retentionCount": 7}, "not found"},
		{"unknown store", map[string]any{"enabled": true, "scheduleCron": "0 3 * * *", "blobStoreId": "33333333-3333-3333-3333-333333333333", "retentionCount": 7}, "not found"},
		{"group store", map[string]any{"enabled": true, "scheduleCron": "0 3 * * *", "blobStoreId": groupStoreID, "retentionCount": 7}, "group store"},
		{"negative retention", map[string]any{"enabled": true, "scheduleCron": "0 3 * * *", "blobStoreId": destStoreID, "retentionCount": -1}, "retentionCount"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, settings := buildSettingsHandler(t)
			ctx := context.Background()
			working := &domain.BackupSettings{Enabled: true, ScheduleCron: "0 2 * * *", BlobStoreID: destStoreID, RetentionCount: 3}
			require.NoError(t, settings.Upsert(ctx, working))

			w := putSettings(t, h, tc.body)
			assert.Equal(t, http.StatusBadRequest, w.Code)
			assert.Contains(t, w.Body.String(), tc.want)
			assert.NotContains(t, w.Body.String(), "SQLSTATE", "no raw database errors in the response")

			got, err := settings.Get(ctx)
			require.NoError(t, err)
			assert.Equal(t, "0 2 * * *", got.ScheduleCron, "the working schedule must be untouched")
			assert.Equal(t, destStoreID, got.BlobStoreID)
			assert.Equal(t, 3, got.RetentionCount)
		})
	}
}
