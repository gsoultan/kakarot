package main

import (
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"text/template"
)

// skeleton holds the project template. Files end in .tmpl so the Go tool
// does not compile them as part of this module.
//
//go:embed all:skeleton
var skeleton embed.FS

// scaffold writes a new layered project for module into dir (default: the
// last element of the module path). It refuses to overwrite files.
func scaffold(module, dir string) error {
	if module == "" || strings.ContainsAny(module, " \\") {
		return fmt.Errorf("kakarot new: invalid module path %q", module)
	}
	name := path.Base(module)
	if dir == "" {
		dir = name
	}
	data := map[string]string{"Module": module, "Name": name}
	err := fs.WalkDir(skeleton, "skeleton", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel := strings.TrimSuffix(strings.TrimPrefix(p, "skeleton/"), ".tmpl")
		dst := filepath.Join(dir, filepath.FromSlash(rel))
		if _, err := os.Stat(dst); err == nil {
			return fmt.Errorf("kakarot new: %s already exists", dst)
		} else if !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		src, err := skeleton.ReadFile(p)
		if err != nil {
			return err
		}
		t, err := template.New(rel).Delims("[[", "]]").Parse(string(src))
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return err
		}
		f, err := os.Create(dst)
		if err != nil {
			return err
		}
		if err := t.Execute(f, data); err != nil {
			f.Close()
			return err
		}
		return f.Close()
	})
	if err != nil {
		return err
	}
	fmt.Printf(`Created %s. Next:

  cd %s
  go get -tool github.com/gsoultan/kakarot/cmd/kakarot github.com/gsoultan/storm/cmd/storm
  go generate ./...
  go mod tidy
  STORM_DSN=postgres://localhost/%s AUTO_MIGRATE=1 go run ./cmd/server
`, dir, dir, name)
	return nil
}
