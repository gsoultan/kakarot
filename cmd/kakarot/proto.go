package main

import (
	"bytes"
	"cmp"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"text/template"

	"golang.org/x/tools/go/packages"
	goimports "golang.org/x/tools/imports"
)

// writeProtos writes api/<group>.proto in the project for every service
// with //kakarot:grpc methods.
//
// The file is the output of grpcx's Service.Proto, the same function a
// server calls at run time, so the two cannot disagree: gen writes a small
// program that registers each gRPC method with its Go types, runs it with
// go run in the project's module, and keeps what it prints.
func writeProtos(pkg *packages.Package, svcs []serviceDecl, root string) error {
	im := newImports()
	rendered := renderServices(pkg, svcs, "main", im)
	im.add("fmt", "fmt")
	im.add("os", "os")
	im.add("github.com/gsoultan/kakarot/transport/grpcx", "grpcx")

	groups := map[string]int{}
	for _, s := range rendered {
		if s.HasGRPC() {
			groups[s.Group]++
		}
	}
	var withGRPC []service
	var paths []string
	for _, s := range rendered {
		if !s.HasGRPC() {
			continue
		}
		name := s.Group
		if groups[s.Group] > 1 { // two services share a group: tell their files apart
			name += "_" + strings.ToLower(s.Short)
		}
		withGRPC = append(withGRPC, s)
		paths = append(paths, filepath.Join(root, "api", name+".proto"))
	}
	if len(withGRPC) == 0 {
		return nil
	}

	var buf bytes.Buffer
	if err := protoMainTmpl.Execute(&buf, map[string]any{"Services": withGRPC, "Imports": im.list(), "Sep": protoSep}); err != nil {
		return err
	}
	tmp, err := os.MkdirTemp("", "kakarot-proto-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	main := filepath.Join(tmp, "main.go")
	src, err := goimports.Process(main, buf.Bytes(), &goimports.Options{Comments: true, TabIndent: true, TabWidth: 8})
	if err != nil {
		return fmt.Errorf("kakarot gen: .proto program: %w\n%s", err, buf.Bytes())
	}
	if err := os.WriteFile(main, src, 0o644); err != nil {
		return err
	}
	cmd := exec.Command("go", "run", main)
	cmd.Dir = root
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		hint := ""
		if strings.Contains(msg, "transport/grpcx") || strings.Contains(msg, "go.sum") || strings.Contains(msg, "go mod tidy") {
			hint = "\nthe project needs the grpcx module: run go get github.com/gsoultan/kakarot/transport/grpcx && go mod tidy, then go generate ./... again"
		}
		return fmt.Errorf("kakarot gen: writing the .proto files: %s%s", cmp.Or(msg, err.Error()), hint)
	}

	// The program prints each file after a line "--- <index>".
	parts := strings.Split(stdout.String(), protoSep)
	if len(parts) != len(paths)+1 {
		return fmt.Errorf("kakarot gen: .proto program printed %d files, want %d", len(parts)-1, len(paths))
	}
	for i, p := range paths {
		if err := writeFile(p, []byte(parts[i+1])); err != nil {
			return err
		}
	}
	return nil
}

// protoSep comes before each file the .proto program prints. No .proto
// text contains it, because Proto never writes a NUL byte.
const protoSep = "\x00kakarot-proto\x00\n"

var protoMainTmpl = template.Must(template.New("proto").Funcs(funcs).Parse(`// Command main prints the .proto file of each gRPC service. kakarot gen
// writes it to a temporary directory and runs it; it is never kept.
package main

import (
{{- range .Imports}}
	"{{.}}"
{{- end}}
)

func main() {
	var svc *grpcx.Service
{{- range $s := .Services}}

	svc = grpcx.NewService({{printf "%q" $s.GRPCName}})
{{- range $s.Methods}}{{if .GRPC}}
	grpcx.{{.GRPCAdd}}(svc, "{{.Name}}", kakarot.{{.Kind}}[{{.In}}, {{.Out}}](nil))
{{- end}}{{end}}
	emit(svc)
{{- end}}
}

func emit(svc *grpcx.Service) {
	text, err := svc.Proto()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Print({{printf "%q" .Sep}} + text)
}
`))
