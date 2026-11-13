package main

import (
	"errors"
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"golang.org/x/tools/go/packages"
)

// A layer may import the layers below it, never the ones above or beside
// it. forbidden lists, per layer, the layers of the same module it must not
// import and why.
var layerRules = map[string]struct {
	layers []string
	libs   []string
	why    string
}{
	"domain": {
		layers: []string{"usecase", "service", "endpoint", "transport", "data", "app", "cmd"},
		libs:   infraLibs,
		why:    "domain types are shared by every layer and must stand alone",
	},
	"usecase": {
		layers: []string{"service", "endpoint", "transport", "data", "app", "cmd"},
		libs:   infraLibs,
		why:    "business rules reach the outside only through ports (interfaces declared in usecase)",
	},
	"service": {
		layers: []string{"endpoint", "transport", "data", "app", "cmd"},
		libs:   infraLibs,
		why:    "the service is the API contract and knows no transport or database",
	},
	"endpoint": {
		layers: []string{"transport", "data", "app", "cmd"},
		libs:   []string{"net/http", "database/sql", "github.com/gsoultan/storm", "google.golang.org/grpc"},
		why:    "endpoints are shared by every transport",
	},
	"transport": {
		layers: []string{"usecase", "data", "app", "cmd"},
		libs:   []string{"database/sql", "github.com/gsoultan/storm"},
		why:    "transports bind endpoints and nothing else",
	},
	"data": {
		layers: []string{"service", "endpoint", "transport", "app", "cmd"},
		libs:   []string{"net/http", "google.golang.org/grpc", "github.com/gsoultan/kakarot/transport"},
		why:    "repositories implement usecase ports and know no transport",
	},
}

// infraLibs are the libraries the inner layers must not touch.
var infraLibs = []string{
	"net/http", "database/sql",
	"github.com/gsoultan/storm",
	"google.golang.org/grpc",
	"github.com/gsoultan/kakarot/transport",
	"github.com/gsoultan/kakarot/data",
	"github.com/gsoultan/kakarot/queue",
}

func under(path, prefix string) bool {
	return path == prefix || strings.HasPrefix(path, prefix+"/")
}

// check verifies a project: the layer import rules hold, every //kakarot:
// comment is valid, and the generated code matches the service interface.
// It prints one problem per line as file:line: message and returns an error
// when it found any.
func check(dir string) error {
	cfg := &packages.Config{Mode: packages.NeedName | packages.NeedFiles | packages.NeedModule, Dir: dir}
	pkgs, err := packages.Load(cfg, "./...")
	if err != nil {
		return fmt.Errorf("kakarot check: %w", err)
	}
	if len(pkgs) == 0 {
		return fmt.Errorf("kakarot check: no Go packages in %s", dir)
	}
	var problems []string
	fset := token.NewFileSet()
	serviceDirs := map[string]bool{}
	for _, p := range pkgs {
		for _, e := range p.Errors {
			problems = append(problems, e.Error())
		}
		if p.Module == nil {
			continue
		}
		rel := strings.TrimPrefix(strings.TrimPrefix(p.PkgPath, p.Module.Path), "/")
		layer, _, _ := strings.Cut(rel, "/")
		rule, ruled := layerRules[layer]
		for _, file := range p.GoFiles {
			if strings.HasSuffix(file, "_gen.go") || strings.HasSuffix(file, ".gen.go") {
				continue
			}
			if layer == "service" && rel == "service" {
				if src, err := os.ReadFile(file); err == nil && strings.Contains(string(src), "//kakarot:service") {
					serviceDirs[filepath.Dir(file)] = true
				}
			}
			if !ruled {
				continue
			}
			f, err := parser.ParseFile(fset, file, nil, parser.ImportsOnly)
			if err != nil {
				problems = append(problems, err.Error())
				continue
			}
			for _, imp := range f.Imports {
				path, _ := strconv.Unquote(imp.Path.Value)
				bad := ""
				for _, l := range rule.layers {
					if under(path, p.Module.Path+"/"+l) {
						bad = l + "/"
					}
				}
				for _, lib := range rule.libs {
					if under(path, lib) {
						bad = path
					}
				}
				if bad != "" {
					problems = append(problems, fmt.Sprintf("%s: %s must not import %s: %s",
						relPos(fset.Position(imp.Pos())), layer, bad, rule.why))
				}
			}
		}
	}

	// Generated code: regenerate in memory and compare.
	var outdated []string
	dirs := make([]string, 0, len(serviceDirs))
	for d := range serviceDirs {
		dirs = append(dirs, d)
	}
	slices.Sort(dirs)
	for _, d := range dirs {
		stale = &outdated
		err := generate(d)
		stale = nil
		if err != nil {
			problems = append(problems, err.Error())
		}
	}
	for _, f := range outdated {
		problems = append(problems, fmt.Sprintf("%s: out of date with the service interface: run go generate ./...", relPath(f)))
	}

	if len(problems) > 0 {
		for _, p := range problems {
			fmt.Fprintln(os.Stderr, p)
		}
		return fmt.Errorf("kakarot check: %d problem(s)", len(problems))
	}
	fmt.Printf("ok: %d packages, layer rules hold, generated code is current\n", len(pkgs))
	return nil
}

func relPath(p string) string {
	wd, err := os.Getwd()
	if err != nil {
		return p
	}
	if abs, err := filepath.Abs(p); err == nil {
		if r, err := filepath.Rel(wd, abs); err == nil && !strings.HasPrefix(r, "..") {
			return r
		}
	}
	return p
}

func relPos(pos token.Position) string {
	return relPath(pos.Filename) + ":" + strconv.Itoa(pos.Line)
}

var errUsage = errors.New(`kakarot: tools for kakarot services

  kakarot new <module path> [dir]   create a new project with every layer
  kakarot gen [service dir]         write endpoint/ and transport/ from the service interface
  kakarot check [project dir]       verify layer rules, //kakarot: comments and generated code
  kakarot help                      show this text

In a project, run them through the Go tool:

  go generate ./...                 runs kakarot gen and storm generate
  go tool kakarot check             good as a CI step and before a commit`)
