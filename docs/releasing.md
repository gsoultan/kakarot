# Releasing

The repository holds several Go modules, so that a project downloads only the libraries it uses. Each module is versioned by its own tag: the module's directory, a slash, and the version.

| Module | Tag for v0.1.0 |
|---|---|
| `github.com/gsoultan/kakarot` | `v0.1.0` |
| `github.com/gsoultan/kakarot/data` | `data/v0.1.0` |
| `github.com/gsoultan/kakarot/transport/grpcx` | `transport/grpcx/v0.1.0` |
| `github.com/gsoultan/kakarot/transport/wsx` | `transport/wsx/v0.1.0` |
| `github.com/gsoultan/kakarot/otelx` | `otelx/v0.1.0` |
| `github.com/gsoultan/kakarot/natsx` | `natsx/v0.1.0` |
| `github.com/gsoultan/kakarot/cmd/kakarot` | `cmd/kakarot/v0.1.0` |

The examples are not released.

## Steps

1. Every sub-module's `go.mod` requires the core at the version being released (`require github.com/gsoultan/kakarot v0.1.0`). The `replace` lines beside it point at the local directory and are ignored by projects that depend on the module, so they stay.
2. Update `CHANGELOG.md`.
3. Merge to `main` with CI green.
4. Tag the merge commit, the core first, and push the tags:

```console
$ git checkout main && git pull
$ for m in "" data/ transport/grpcx/ transport/wsx/ otelx/ natsx/ cmd/kakarot/; do git tag "${m}v0.1.0"; done
$ git push origin --tags
```

5. Check that a clean project resolves it:

```console
$ go run github.com/gsoultan/kakarot/cmd/kakarot@v0.1.0 new example.com/acme/notes
```

## Versions

Before v1.0.0 a minor version may change the API; the changelog says what to change. Release all modules together under the same number, so one version of kakarot is one consistent set.
