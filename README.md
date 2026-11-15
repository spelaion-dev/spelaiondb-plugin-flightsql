# spelaiondb-plugin-flightsql

Arrow Flight SQL for [SpelaionDB](https://github.com/spelaion-dev/spelaiondb), the desktop
database client: one driver for every engine that speaks it, served to SpelaionDB as a plugin.

SpelaionDB's engines are programs it starts and talks to over their standard input and output
(its ADR-041). This one serves a generic `flightsql` connection and named ones for Dremio
(`dremio`) and GizmoSQL (`gizmosql`). Flight SQL's results are Arrow, which is what SpelaionDB
itself uses: they reach it without passing through any text. It is built here, in its own
repository, and SpelaionDB pins by hash the exact release it was tested with (its ADR-042,
ADR-055).

- **Protocol:** [SpelaionDB's plugin protocol](https://github.com/spelaion-dev/spelaiondb/blob/dev/docs/plugin-protocol.md),
  version 1.1. Experimental until SpelaionDB 1.0.
- **Engines:** `flightsql`, `dremio` and `gizmosql`, over gRPC (Arrow's own `arrow-flight`
  client on tonic, with rustls).
- **License:** Apache-2.0.

## Working on it

```bash
ci/services.sh
```

starts the test servers (GizmoSQL and Dremio OSS in Docker, and Dremio's first user), and

```bash
SPELAION_TEST_GIZMOSQL=127.0.0.1:31337 SPELAION_TEST_DREMIO=127.0.0.1:32010 cargo test
```

runs the tests and the protocol's conformance set against this plugin's own program, for each
engine. Without the variables the tests that need a server are skipped, which does not count as
tested.

To try a build with SpelaionDB, point a development build of it at the program
(`SPELAION_DEV_PLUGIN_FLIGHTSQL`, `…_DREMIO`, `…_GIZMOSQL`, in SpelaionDB's repository; a
release build ignores those variables).

`AGENTS.md` has the rules. Reports and questions go to
[SpelaionDB's issues](https://github.com/spelaion-dev/spelaiondb/issues).
