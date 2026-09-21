package group_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/nexspence-oss/nexspence/internal/domain"
	"github.com/nexspence-oss/nexspence/internal/formats"
	"github.com/nexspence-oss/nexspence/internal/formats/group"
	"github.com/nexspence-oss/nexspence/internal/formats/helm"
	"github.com/nexspence-oss/nexspence/internal/testutil"
)

// Hosted Helm answers 200 on /index.yaml even when empty. Without a
// GroupIndexMerger that 200 hid every later member — the same #99 shadowing
// pypi/maven already fixed. Two hosted members must both appear in the group
// catalog, which is what `helm search` / `helm pull --repo <group>` read.
func TestGroupMerge_HelmEndToEnd(t *testing.T) {
	m1 := testutil.SimpleRepo("helm-hosted", "helm")
	m2 := testutil.SimpleRepo("helm-proxy", "helm")
	g := &domain.Repository{
		ID: "repo-helm", Name: "helm", Format: "helm",
		Type: domain.TypeGroup, Online: true,
		FormatConfig: map[string]any{"member_names": []interface{}{"helm-hosted", "helm-proxy"}},
	}

	repoRepo := testutil.NewRepoRepo(m1, m2, g)
	d := formats.Deps{
		Repos:      repoRepo,
		Blobs:      testutil.NewBlobStoreRepo(),
		Components: testutil.NewComponentRepo(),
		Assets:     testutil.NewAssetRepo(),
		BlobStore:  testutil.NewBlobStore(),
		BaseURL:    "http://localhost:8080",
	}
	helmH := helm.New(d)
	registry := map[string]formats.FormatHandler{"helm": helmH}
	groupH := group.New(d, registry)

	r := gin.New()
	r.Any("/repository/:repoName/*path", func(c *gin.Context) {
		repo, _ := repoRepo.Get(c.Request.Context(), c.Param("repoName"))
		if repo != nil && repo.Type == domain.TypeGroup {
			groupH.ServeHTTP(c)
			return
		}
		helmH.ServeHTTP(c)
	})

	putChart := func(repoName, filename, content string) {
		req := httptest.NewRequest(http.MethodPut, "/repository/"+repoName+"/"+filename,
			strings.NewReader(content))
		req.Header.Set("Content-Type", "application/x-tar")
		req.ContentLength = int64(len(content))
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		require.Contains(t, []int{http.StatusCreated, http.StatusOK}, w.Code, w.Body.String())
	}
	putChart("helm-hosted", "widget-1.2.3.tgz", "hosted-chart")
	putChart("helm-proxy", "ingress-4.0.0.tgz", "proxy-chart")

	w := get(r, "/repository/helm/index.yaml")
	assert.Equal(t, http.StatusOK, w.Code, w.Body.String())
	assert.Equal(t, "helm-hosted,helm-proxy", w.Header().Get("X-Nexspence-Source"))

	var doc struct {
		Entries map[string][]map[string]any `yaml:"entries"`
	}
	require.NoError(t, yaml.Unmarshal(w.Body.Bytes(), &doc))
	require.Contains(t, doc.Entries, "widget")
	require.Contains(t, doc.Entries, "ingress", "proxy member must not be shadowed by hosted index.yaml 200")

	urls, _ := doc.Entries["widget"][0]["urls"].([]any)
	require.NotEmpty(t, urls)
	assert.Equal(t, "http://localhost:8080/repository/helm/widget-1.2.3.tgz", urls[0])
}
