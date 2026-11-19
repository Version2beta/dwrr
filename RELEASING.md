# Releasing

This repository publishes the Go module `github.com/version2beta/dwrr`.

## Local preparation (no publication)

The working-tree API is unreleased; v0.10.1 is the latest published release.
No next version has been selected. Local checks do not authorize commits, tags,
pushes or publication. Preserve uncommitted work and keep raw logs outside the
repository. See [HANDOFF.md](HANDOFF.md) for the current scoped evidence and gaps.

- [ ] Inspect status/diffs, record HEAD, source state and toolchain/configuration,
  and run `git diff --check`.
- [ ] Check README migration notes and changelog against the actual API, including
  the breaking `Take` signature, callable errors, concurrency and performance.
- [ ] Run the following with installed toolchains only. For offline preparation,
  set `GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off`; missing cached dependencies are
  a validation gap, not permission to download or update dependencies.

  ```sh
  go test ./... -count=1
  go test -race ./... -count=1
  go vet ./...
  go test ./... -run 'TestDynamic|TestSetQuantumSources|TestDeadlineQuantum|TestPublic|TestGeneration|Example' -count=1
  GOMAXPROCS=2 go test -run '^$' -fuzz '^FuzzDifferential$' -fuzztime=60s -parallel=2
  go test -run '^$' -bench . -benchmem -benchtime=200ms -count=3
  ```

- [ ] Exercise Go 1.22.2 (the declared minimum) and current stable; record any
  unavailable toolchain as unproven. The CI matrix tests 1.22.x and stable, not
  necessarily the exact minimum patch version.
- [ ] Run standalone README examples from a temporary consumer using a local
  `replace`, and compare actual output with the documented output.
- [ ] Review benchmark measurements without regression claims unless a verified,
  comparable baseline exists. Confirm application acceptance of speculative
  policies, explicit retries and callable overhead.
- [ ] Obtain independent review of the intended changes and validation evidence.

## Before authorized publication

- Start from a clean, up-to-date `main` checkout with the intended changes reviewed
  and merged, only after authorization to commit/push.
- Confirm CI is green for the exact release commit on Go 1.22.x and stable; local
  testing alone does not establish remote CI or minimum-version compatibility.
- Choose a semantic version with the maintainer, accounting for the breaking API;
  replace the Unreleased heading and align README version/install/examples with
  that exact release. Do not label the working-tree API as v0.10.1.
- Choose the module path for the selected version. While the module is `v0.x`, its API is unstable; `v1.0.0` denotes the stable API. For a future major version `v2` or later, update the module path in `go.mod` and imports to include the major suffix, for example `github.com/version2beta/dwrr/v2`, before tagging.

## Publish (requires separate authorization)

These steps use network access and change published state; they are not part of
local preparation.

1. Create and push an annotated version tag, for example:

   ```sh
   git tag -a v0.1.0 -m "Release v0.1.0"
   git push origin v0.1.0
   ```

   Use the intended `vX.Y.Z` tag; `go.mod` has no release-version field (its `go`
   directive is the minimum Go version). Pushing a `v*` tag runs CI. Never move or retag a published version; publish a new version instead.
2. Create a GitHub Release for that exact tag and include concise release notes.
3. Verify the exact version resolves from the Go module proxy and that a consumer can fetch it:

   ```sh
   GOPROXY=https://proxy.golang.org go list -m github.com/version2beta/dwrr@v0.1.0
   tmp="$(mktemp -d)"
   cd "$tmp"
   go mod init example.com/dwrr-consumer
   GOPROXY=https://proxy.golang.org go get github.com/version2beta/dwrr@v0.1.0
   ```

   Substitute the release version. For v2+, use the `/v2` module path. Confirm the resolved module and version match the release; `go get` may be run in a temporary consumer module to avoid changing this repository's module state.
4. Check that the version appears on [pkg.go.dev](https://pkg.go.dev/github.com/version2beta/dwrr). The Go proxy and pkg.go.dev index new versions asynchronously; request indexing on pkg.go.dev if needed.
