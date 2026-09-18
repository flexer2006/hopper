package testutil

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func ModuleRoot(tb testing.TB) string {
	tb.Helper()

	_, file, _, ok := runtime.Caller(0)
	if !ok {
		tb.Fatal("runtime.Caller")
	}

	dir := filepath.Dir(file)
	for range 12 {
		_, err := os.Stat(filepath.Join(dir, "go.mod"))
		if err == nil {
			return dir
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}

		dir = parent
	}

	tb.Fatal("go.mod not found")

	return ""
}

func ResolveProductDocs(moduleRoot string, parts ...string) (string, bool) {
	candidates := []string{
		filepath.Join(append([]string{moduleRoot, "..", "docs"}, parts...)...),
		filepath.Join(append([]string{moduleRoot, "docs"}, parts...)...),
	}

	for _, path := range candidates {
		_, err := os.Stat(path)
		if err == nil {
			return path, true
		}
	}

	return "", false
}

func ProductDocs(tb testing.TB, parts ...string) string {
	tb.Helper()

	path, ok := ResolveProductDocs(ModuleRoot(tb), parts...)
	if !ok {
		tb.Fatalf("product docs not found for %s (tried ../docs then module docs/)", filepath.Join(parts...))
	}

	return path
}
