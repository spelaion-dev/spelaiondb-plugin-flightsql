# SpelaionDB's Flight SQL plugin — rules for agents

Arrow Flight SQL for [SpelaionDB](https://github.com/spelaion-dev/spelaiondb): one driver for
every engine that speaks it, and a small program that serves it to SpelaionDB over its plugin
protocol. Apache-2.0.

**The rules of SpelaionDB's repository apply here** (its `AGENTS.md`: all the code is written
by an agent, the compiler, the tests and CI are the only reviewer; everything in English;
sign-off; one task, one short branch; commit subjects that read well in a changelog). This file
says only what is different in this repository.

## What this is
- `src/`: the driver. It implements `Driver` and `Connection` of `spelaion-driver-api`
  (SpelaionDB's ADR-005) over Flight SQL, for the engines `flightsql` (any server), `dremio` and
  `gizmosql` (the same code: they differ in what SpelaionDB's form offers):
  - `channel.rs` gives tonic our own connector: TCP to `connect_addr` when it is given (the
    local end of SpelaionDB's tunnel), never through a system proxy, then TLS as `tls.rs` says
    (libpq's modes, offering `h2`), verified against `host`.
  - `lib.rs`: the login is Flight's handshake (a bearer token after it); a statement is a
    `CommandStatementQuery`, its results read with `DoGet` from each endpoint. **Results on
    another server (an endpoint with a location) are refused**: nothing leaves SpelaionDB's
    route. A cancel ends the gRPC call and sends `CancelFlightInfo`. Failures are told apart by
    their gRPC status (`Unavailable` loses the connection). The catalog comes from Flight SQL's
    metadata calls; their name filters are patterns, so only exact names are kept.
  - `types.rs`: each Arrow column cast to its logical type's Arrow type (the plugin converts,
    never the core); what Arrow cannot cast is written as Arrow writes it.
- `src/bin/spelaiondb-plugin-flightsql.rs`: the program. It hands the drivers to
  `spelaion_plugin_sdk::run` and nothing else.
- `tests/flightsql.rs`: values and their types, large results, errors, cancel, the catalog, the
  login and the route, against GizmoSQL (as `gizmosql` and as `flightsql`) and Dremio.
  `tests/conformance.rs`: SpelaionDB's conformance set for each engine, and `plugin.toml`.
- `testing/compose.yaml` and `ci/services.sh`: the test servers (GizmoSQL on 31337, Dremio on
  32010 with its first user made by the script), pinned by digest.
- `plugin.toml`: the protocol version, the engines and the lowest SpelaionDB of this plugin.

## A plugin is the wire, not the rules
SpelaionDB's core splits, classifies and guards statements, keeps secrets, opens tunnels and
writes the audit (ADR-041 there). This plugin connects, runs what it is sent, cancels and
introspects. So, and these are not negotiable (`docs/plugin-protocol.md` in SpelaionDB's
repository, "What a plugin must never do"):

- **Standard output is the protocol and nothing else.** Never `println!`. What must be said
  goes to standard error, a line at a time.
- **The password arrives in `connect` and is written nowhere**: not to a log, an error, a
  file, the environment or a request's address.
- **It runs what it is sent.** It never decides whether a statement is a read, and never
  changes one.
- **Nothing is run again** after a connection is lost: a request lost on the way is not sent
  again.
- **It ends when its standard input closes** (the SDK does this).
- It reads no configuration file, keychain or store of SpelaionDB's, and opens no tunnel: it
  connects to `connect_addr` when it is given one and verifies TLS against `host`.

## The contract comes from SpelaionDB, by tag
`spelaion-driver-api`, `spelaion-plugin-sdk` and, for the tests, `spelaion-plugin-host` are git
dependencies on a tag of SpelaionDB's repository (`plugin-sdk-vX.Y.Z`), all on the same tag.
They are not on crates.io.

- **A protocol change is made there first** (the new version, with its tests), then here: move
  the tag, adapt, release. SpelaionDB then moves its pin.
- Arrow must stay the version SpelaionDB uses: batches cross between the two unchanged.
- `plugin.toml`'s `protocol` is what the SDK of that tag speaks; `tests/conformance.rs` fails
  if they differ.

## Releases
Pushing a tag `vX.Y.Z` (the version in `Cargo.toml`) runs the release every plugin shares
(the template's repository, called by commit): the four platforms, each archive with its
SHA-256, and notes that carry the pin in the form SpelaionDB's `plugins.toml` wants. **Nothing
is signed and no secret lives here**: a release reaches users only when SpelaionDB's
repository pins its hashes in a pull request of its own, whose CI runs SpelaionDB's tests
through that exact build (ADR-042 there).

## Branches
`main` is protected: it changes only through pull requests whose CI is green, and cannot be
force-pushed or deleted. A pull request that touches a file in `.github/CODEOWNERS` waits for
an owner. The work is tracked in SpelaionDB's tracker (Plane, project `SDB`): write the item's
identifier in the pull request's description, never in a commit subject.

## Before calling something done
- `cargo fmt --all`, `cargo clippy --all-targets -- -D warnings`.
- The tests against a real server (without the variables they are skipped, and that does
  **not** count as tested): `ci/services.sh` and
  `SPELAION_TEST_GIZMOSQL=127.0.0.1:31337 SPELAION_TEST_DREMIO=127.0.0.1:32010 cargo test`.
- `cargo deny check` if dependencies change.
- A change to how values are written is checked against SpelaionDB too: build this program and
  run SpelaionDB's tests through it (`SPELAION_DEV_PLUGIN_FLIGHTSQL`, `…_DREMIO` and
  `…_GIZMOSQL=<this program>` with a development build there).
- If something could not be tested, say so.
