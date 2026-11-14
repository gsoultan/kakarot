# Tutorial: a service from nothing

You need Go 1.27.4 or newer. A database is only needed to run the server, not to develop or test.

## 1. Scaffold

```console
$ go run github.com/gsoultan/kakarot/cmd/kakarot new example.com/acme/notes
$ cd notes
$ go get -tool github.com/gsoultan/kakarot/cmd/kakarot github.com/gsoultan/storm/cmd/storm
$ go generate ./...
$ go mod tidy
$ go test ./...
$ go tool kakarot check
```

The project has one package per layer:

| Package | Layer | Written by |
|---|---|---|
| `domain/` | DTOs and validation | you |
| `usecase/` | business rules and the ports (interfaces) they need | you |
| `service/` | the API, annotated with `//kakarot:` comments | you |
| `endpoint/` | typed endpoints | `kakarot gen` |
| `transport/` | HTTP routes | `kakarot gen` |
| `data/` | storm repositories; `data/model` holds the database types | you and `storm generate` |

## 2. Add an operation, test first

Say notes can be archived. Start with the domain type and its validation:

```go
// domain/note.go
type ArchiveNote struct {
    ID string `path:"id"`
}

func (a ArchiveNote) Validate() error {
    if a.ID == "" {
        return kakarot.Invalid("invalid request", map[string]string{"id": "required"})
    }
    return nil
}
```

Write the usecase test before the usecase. The repository is an interface the usecase declares, so the test uses a small in-memory one and no database:

```go
// usecase/archive_test.go
func TestArchiveUnknownNote(t *testing.T) {
    uc := &usecase.ArchiveNote{Notes: newMemNotes()}

    _, err := uc.Execute(t.Context(), domain.ArchiveNote{ID: "missing"})

    kakarottest.WantCode(t, err, kakarot.CodeNotFound)
}
```

Then the usecase, with the port method it needs added to `NoteRepository`:

```go
// usecase/note.go
type ArchiveNote struct{ Notes NoteRepository }

func (u *ArchiveNote) Execute(ctx context.Context, in domain.ArchiveNote) (domain.Note, error) {
    return u.Notes.Archive(ctx, in.ID)
}
```

## 3. Expose it

Add one method to the service interface, with the route as a comment, and implement it by calling the usecase:

```go
// service/service.go
//kakarot:http POST /notes/{id}/archive
ArchiveNote(ctx context.Context, in domain.ArchiveNote) (domain.Note, error)
```

```console
$ go generate ./...
```

`endpoint/` and `transport/` now carry the new operation. A mistyped comment is an error that names the file, the line and the fix.

## 4. Test the API

`kakarottest.Do` sends one request through the real router: decoding, validation, the usecase and the error mapping.

```go
res := kakarottest.Do[domain.Note](t, router, "POST", "/notes/n-1/archive", nil)
if res.Status != http.StatusCreated || !res.Body.Archived { ... }

missing := kakarottest.Do[kakarottest.None](t, router, "POST", "/notes/nope/archive", nil)
if missing.Status != http.StatusNotFound { ... }
```

## 5. Store it

Implement `Archive` in `data/note_repo.go` with storm. Storm models stay inside `data/`; the repository returns domain types. Repository tests are the only ones that need a database.

## 6. Check and run

```console
$ go test ./...
$ go tool kakarot check
$ STORM_DSN=postgres://localhost/notes AUTO_MIGRATE=1 go run ./cmd/server
$ curl -XPOST localhost:8080/notes -d '{"title":"hi"}'
```

`kakarot check` fails when a layer imports something it must not (a usecase importing `net/http`, for example), when a `//kakarot:` comment is wrong, or when generated code is out of date. Run it in CI.

## Where to go next

- More transports for the same method: add `//kakarot:grpc` or `//kakarot:queue <topic>`.
- [Events](events.md) for work that happens after the response.
- `examples/checkout` for a full flow with payment, shipping, compensation and expiry.
