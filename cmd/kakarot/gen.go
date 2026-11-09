package main

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"go/format"
	"go/types"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"text/template"

	"golang.org/x/tools/go/packages"
)

type method struct {
	Name     string
	Kind     string // Endpoint, ServerStream, ClientStream, BidiStream
	In, Out  string
	Validate bool
	HTTP     string // "GET /orders/{id}", empty when not exposed
}

type service struct {
	Name    string // interface name
	Short   string // Name without a "Service" suffix, used in generated names
	Group   string // op service name
	Methods []method
}

// generate reads the service package in dir and writes
// ../endpoint/endpoints_gen.go and ../transport/http_gen.go.
func generate(dir string) error {
	cfg := &packages.Config{Mode: packages.NeedName | packages.NeedTypes | packages.NeedSyntax | packages.NeedTypesInfo, Dir: dir}
	pkgs, err := packages.Load(cfg, ".")
	if err != nil {
		return err
	}
	if len(pkgs) != 1 || len(pkgs[0].Errors) > 0 {
		for _, p := range pkgs {
			for _, e := range p.Errors {
				fmt.Fprintln(os.Stderr, e)
			}
		}
		return errors.New("kakarot gen: service package does not load")
	}
	pkg := pkgs[0]
	svcs, err := findServices(pkg)
	if err != nil {
		return err
	}
	if len(svcs) == 0 {
		return fmt.Errorf("kakarot gen: no interface with a //kakarot:service comment in %s", pkg.PkgPath)
	}
	base := strings.TrimSuffix(pkg.PkgPath, "/"+filepath.Base(pkg.PkgPath))

	// endpoint/endpoints_gen.go
	epIm := newImports()
	rendered := renderServices(pkg, svcs, base+"/endpoint", epIm)
	anyValidate, anyBidi, anyHTTP := false, false, false
	for _, s := range rendered {
		for _, m := range s.Methods {
			anyValidate = anyValidate || m.Validate
			anyBidi = anyBidi || (m.Kind == "BidiStream" && m.HTTP != "")
			anyHTTP = anyHTTP || m.HTTP != ""
		}
	}
	if anyValidate {
		epIm.add("github.com/gsoultan/kakarot/middleware", "middleware")
	}
	if err := writeGo(filepath.Join(dir, "..", "endpoint", "endpoints_gen.go"), endpointsTmpl, map[string]any{
		"Services": rendered, "SvcName": pkg.Name, "Imports": epIm.list(),
	}); err != nil {
		return err
	}
	if !anyHTTP {
		return nil
	}

	// transport/http_gen.go
	trIm := newImports()
	trIm.add("github.com/gsoultan/kakarot", "kakarot")
	trIm.add("github.com/gsoultan/kakarot/transport/httpx", "httpx")
	trIm.add(base+"/endpoint", "endpoint")
	if anyBidi {
		trIm.add("github.com/gsoultan/kakarot/transport/wsx", "wsx")
	}
	return writeGo(filepath.Join(dir, "..", "transport", "http_gen.go"), httpTmpl, map[string]any{
		"Services": rendered, "Imports": trIm.list(),
	})
}

func writeGo(path string, tmpl *template.Template, data map[string]any) error {
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return err
	}
	src, err := format.Source(buf.Bytes())
	if err != nil {
		return fmt.Errorf("kakarot gen: %s: %w\n%s", path, err, buf.Bytes())
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(path, src, 0o644); err != nil {
		return err
	}
	fmt.Println("→", path)
	return nil
}

// findServices returns interfaces marked //kakarot:service, classifying each
// method by shape. Methods must take context.Context first.
func findServices(pkg *packages.Package) ([]serviceDecl, error) {
	var out []serviceDecl
	scope := pkg.Types.Scope()
	comments := collectComments(pkg)
	for _, name := range scope.Names() {
		tn, ok := scope.Lookup(name).(*types.TypeName)
		if !ok {
			continue
		}
		iface, ok := tn.Type().Underlying().(*types.Interface)
		if !ok {
			continue
		}
		doc := comments[name]
		group, ok := directive(doc, "kakarot:service")
		if !ok {
			continue
		}
		if group == "" {
			group = strings.ToLower(strings.TrimSuffix(name, "Service"))
		}
		sd := serviceDecl{Name: name, Group: group}
		for i := range iface.NumMethods() {
			m := iface.Method(i)
			md, err := classify(m)
			if err != nil {
				return nil, fmt.Errorf("%s.%s: %w", name, m.Name(), err)
			}
			md.HTTP, _ = directive(comments[name+"."+m.Name()], "kakarot:http")
			sd.Methods = append(sd.Methods, md)
		}
		slices.SortFunc(sd.Methods, func(a, b methodDecl) int { return int(a.Pos - b.Pos) })
		out = append(out, sd)
	}
	return out, nil
}

type serviceDecl struct {
	Name, Group string
	Methods     []methodDecl
}

type methodDecl struct {
	Name     string
	Kind     string
	In, Out  types.Type
	Validate bool
	HTTP     string
	Pos      int
}

var validatable = types.NewInterfaceType([]*types.Func{
	types.NewFunc(0, nil, "Validate", types.NewSignatureType(nil, nil, nil, nil,
		types.NewTuple(types.NewVar(0, nil, "", types.Universe.Lookup("error").Type())), false)),
}, nil).Complete()

func classify(m *types.Func) (methodDecl, error) {
	sig := m.Type().(*types.Signature)
	md := methodDecl{Name: m.Name(), Pos: int(m.Pos())}
	p, r := sig.Params(), sig.Results()
	if p.Len() < 2 || !isNamed(p.At(0).Type(), "context", "Context") {
		return md, errors.New("first parameter must be context.Context")
	}
	errT := types.Universe.Lookup("error").Type()
	seqElem := func(t types.Type) (types.Type, bool) {
		n, ok := t.(*types.Named)
		if !ok || !isNamed(n, "iter", "Seq2") || n.TypeArgs().Len() != 2 || !types.Identical(n.TypeArgs().At(1), errT) {
			return nil, false
		}
		return n.TypeArgs().At(0), true
	}
	switch {
	case p.Len() == 2 && r.Len() == 2 && types.Identical(r.At(1).Type(), errT):
		if in, ok := seqElem(p.At(1).Type()); ok {
			md.Kind, md.In, md.Out = "ClientStream", in, r.At(0).Type()
		} else {
			md.Kind, md.In, md.Out = "Endpoint", p.At(1).Type(), r.At(0).Type()
		}
	case p.Len() == 2 && r.Len() == 1:
		out, ok := seqElem(r.At(0).Type())
		if !ok {
			return md, errors.New("single result must be iter.Seq2[T, error]")
		}
		md.Kind, md.In, md.Out = "ServerStream", p.At(1).Type(), out
	case p.Len() == 3 && r.Len() == 1 && types.Identical(r.At(0).Type(), errT):
		in, ok := seqElem(p.At(1).Type())
		send, ok2 := p.At(2).Type().(*types.Signature)
		if !ok || !ok2 || send.Params().Len() != 1 {
			return md, errors.New("bidi method must be (ctx, iter.Seq2[In, error], func(Out) error) error")
		}
		md.Kind, md.In, md.Out = "BidiStream", in, send.Params().At(0).Type()
	default:
		return md, errors.New("unsupported method shape")
	}
	md.Validate = md.Kind == "Endpoint" && types.Implements(md.In, validatable)
	return md, nil
}

func isNamed(t types.Type, pkg, name string) bool {
	n, ok := t.(*types.Named)
	if !ok {
		if a, ok := t.(*types.Alias); ok {
			return isNamed(types.Unalias(a), pkg, name)
		}
		return false
	}
	o := n.Obj()
	return o.Pkg() != nil && o.Pkg().Path() == pkg && o.Name() == name
}

func collectComments(pkg *packages.Package) map[string]string {
	out := map[string]string{}
	for _, f := range pkg.Syntax {
		for _, d := range f.Decls {
			collectDecl(d, out)
		}
	}
	return out
}

func directive(doc, key string) (string, bool) {
	for line := range strings.SplitSeq(doc, "\n") {
		line = strings.TrimSpace(line)
		if rest, ok := strings.CutPrefix(line, key); ok {
			return strings.TrimSpace(rest), true
		}
	}
	return "", false
}

// imports tracks the packages a generated file needs.
type imports struct{ paths map[string]string }

func newImports() *imports { return &imports{paths: map[string]string{}} }

func (im *imports) qualifier(dest string) types.Qualifier {
	return func(p *types.Package) string {
		if p.Path() == dest {
			return ""
		}
		im.paths[p.Path()] = p.Name()
		return p.Name()
	}
}

func (im *imports) add(path, name string) { im.paths[path] = name }

func (im *imports) list() []string {
	var out []string
	for p := range im.paths {
		out = append(out, p)
	}
	slices.Sort(out)
	return out
}

func renderServices(pkg *packages.Package, svcs []serviceDecl, dest string, im *imports) []service {
	q := im.qualifier(dest)
	var out []service
	for _, s := range svcs {
		rs := service{Name: s.Name, Short: cmp.Or(strings.TrimSuffix(s.Name, "Service"), s.Name), Group: s.Group}
		for _, m := range s.Methods {
			rs.Methods = append(rs.Methods, method{
				Name: m.Name, Kind: m.Kind, Validate: m.Validate, HTTP: m.HTTP,
				In: types.TypeString(m.In, q), Out: types.TypeString(m.Out, q),
			})
		}
		out = append(out, rs)
	}
	im.add(pkg.PkgPath, pkg.Name)
	im.add("github.com/gsoultan/kakarot", "kakarot")
	return out
}
