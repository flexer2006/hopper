package testutil_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/flexer2006/hopper/internal/testutil"
)

func TestModuleRootFindsGoMod(t *testing.T) {
	t.Parallel()

	root := testutil.ModuleRoot(t)
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("ModuleRoot() = %q: %v", root, err)
	}
}

func TestResolveProductDocsPrefersSibling(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	mod := filepath.Join(root, "mod")
	sib := filepath.Join(root, "docs", "api")
	nested := filepath.Join(mod, "docs", "api")

	err := os.MkdirAll(sib, 0o755)
	if err != nil {
		t.Fatal(err)
	}

	err = os.MkdirAll(nested, 0o755)
	if err != nil {
		t.Fatal(err)
	}

	err = os.WriteFile(filepath.Join(sib, "openapi.yaml"), []byte("sibling"), 0o600)
	if err != nil {
		t.Fatal(err)
	}

	err = os.WriteFile(filepath.Join(nested, "openapi.yaml"), []byte("nested"), 0o600)
	if err != nil {
		t.Fatal(err)
	}

	got, ok := testutil.ResolveProductDocs(mod, "api", "openapi.yaml")
	if !ok || got != filepath.Join(sib, "openapi.yaml") {
		t.Fatalf("got %q ok=%v, want sibling", got, ok)
	}
}

func TestResolveProductDocsFallsBackToModule(t *testing.T) {
	t.Parallel()

	mod := t.TempDir()
	nested := filepath.Join(mod, "docs", "contracts")

	err := os.MkdirAll(nested, 0o755)
	if err != nil {
		t.Fatal(err)
	}

	want := filepath.Join(nested, "enqueue-message.schema.json")
	err = os.WriteFile(want, []byte("{}"), 0o600)
	if err != nil {
		t.Fatal(err)
	}

	got, ok := testutil.ResolveProductDocs(mod, "contracts", "enqueue-message.schema.json")
	if !ok || got != want {
		t.Fatalf("got %q ok=%v, want module docs", got, ok)
	}
}
