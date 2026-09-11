package persist //nolint:testpackage // inspect unexported listExpiredLeases source

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

func TestListExpiredLeasesUsesIDProjection(t *testing.T) {
	t.Parallel()

	fset := token.NewFileSet()

	file, err := parser.ParseFile(fset, "mongo.go", nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatal(err)
	}

	raw, err := os.ReadFile("mongo.go")
	if err != nil {
		t.Fatal(err)
	}

	var body string

	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "listExpiredLeases" || fn.Recv == nil || len(fn.Recv.List) == 0 {
			continue
		}

		start := fset.Position(fn.Body.Pos()).Offset
		end := fset.Position(fn.Body.End()).Offset
		body = string(raw[start:end])

		break
	}

	if body == "" {
		t.Fatal("listExpiredLeases not found")
	}

	if strings.Contains(body, "findList") {
		t.Fatal("listExpiredLeases must not call findList")
	}

	if !strings.Contains(body, "SetProjection") {
		t.Fatal("listExpiredLeases must use SetProjection")
	}

	if !strings.Contains(body, "_id") {
		t.Fatal("listExpiredLeases projection must include _id")
	}
}
