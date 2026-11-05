// Package purity checks the constitution rule that parsing, rules, transform and output code do no
// I/O: no database, HTTP, filesystem, process or clock access (constitution #6, M1 exit criteria).
package purity

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var pure = []string{"doc", "rules", "paxafe", "provider", "provider/tive", "config"}

var bannedImports = map[string]bool{
	"net": true, "net/http": true, "database/sql": true, "os": true, "os/exec": true,
	"io/ioutil": true, "syscall": true, "log": true,
}

func TestPureCoreHasNoIO(t *testing.T) {
	for _, pkg := range pure {
		files, err := filepath.Glob(filepath.Join("..", filepath.FromSlash(pkg), "*.go"))
		if err != nil {
			t.Fatal(err)
		}
		if len(files) == 0 {
			t.Fatalf("no Go files found for %s", pkg)
		}
		for _, f := range files {
			if strings.HasSuffix(f, "_test.go") {
				continue
			}
			src, err := os.ReadFile(f)
			if err != nil {
				t.Fatal(err)
			}
			tree, err := parser.ParseFile(token.NewFileSet(), f, src, 0)
			if err != nil {
				t.Fatal(err)
			}
			for _, imp := range tree.Imports {
				path := strings.Trim(imp.Path.Value, `"`)
				if bannedImports[path] || strings.Contains(path, "jackc/pgx") {
					t.Errorf("%s imports %s", f, path)
				}
			}
			ast.Inspect(tree, func(n ast.Node) bool {
				sel, ok := n.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				if id, ok := sel.X.(*ast.Ident); ok && id.Name == "time" && (sel.Sel.Name == "Now" || sel.Sel.Name == "Since" || sel.Sel.Name == "Until") {
					t.Errorf("%s uses time.%s: pass time in instead", f, sel.Sel.Name)
				}
				return true
			})
		}
	}
}
