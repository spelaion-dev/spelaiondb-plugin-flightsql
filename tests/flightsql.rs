//! Tests against real Flight SQL servers: GizmoSQL (DuckDB behind Flight SQL) and Dremio.
//!
//! They need `SPELAION_TEST_GIZMOSQL=host:port` (user `spelaion`, password `spelaion`) and
//! `SPELAION_TEST_DREMIO=host:port` (user `spelaion`, password `spelaion123`, made by
//! `ci/services.sh`). Without the variables they are skipped with a notice, which does not count
//! as tested. The generic `flightsql` engine is tested against GizmoSQL.

use std::time::{Duration, Instant};

use futures_util::TryStreamExt;
use spelaion_driver_api::{
    ConnectParams, Connection, Driver, DriverError, ExecEvent, ExecOptions, LogicalType,
    ResultSchema, Secret, TlsMode, Value,
};
use spelaion_driver_flightsql::FlightSqlDriver;

struct Server {
    name: &'static str,
    driver: FlightSqlDriver,
    host: String,
    port: u16,
    password: &'static str,
}

fn servers() -> Vec<Server> {
    let mut out = Vec::new();
    for (variable, name, driver, password) in [
        (
            "SPELAION_TEST_GIZMOSQL",
            "gizmosql",
            FlightSqlDriver::gizmosql(),
            "spelaion",
        ),
        (
            "SPELAION_TEST_GIZMOSQL",
            "flightsql",
            FlightSqlDriver::generic(),
            "spelaion",
        ),
        (
            "SPELAION_TEST_DREMIO",
            "dremio",
            FlightSqlDriver::dremio(),
            "spelaion123",
        ),
    ] {
        match std::env::var(variable) {
            Ok(addr) => {
                let (host, port) = addr.rsplit_once(':').expect("host:port");
                out.push(Server {
                    name,
                    driver,
                    host: host.into(),
                    port: port.parse().expect("a port"),
                    password,
                });
            }
            Err(_) => eprintln!("{variable} not set: {name} skipped"),
        }
    }
    out
}

impl Server {
    fn params(&self) -> ConnectParams {
        ConnectParams {
            host: self.host.clone(),
            port: self.port,
            user: "spelaion".into(),
            password: Some(Secret::new(self.password)),
            tls: TlsMode::Disable,
            ..Default::default()
        }
    }

    async fn connect(&self) -> Box<dyn Connection> {
        self.driver
            .connect(&self.params())
            .await
            .unwrap_or_else(|e| panic!("{}: {e}", self.name))
    }
}

async fn run(conn: &dyn Connection, sql: &str) -> Result<Vec<ExecEvent>, DriverError> {
    conn.execute(sql, &ExecOptions::default())
        .await?
        .try_collect()
        .await
}

async fn query(conn: &dyn Connection, sql: &str) -> (ResultSchema, Vec<Vec<Value>>) {
    let events = run(conn, sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}\n→ {e}"));
    let mut schema: Option<ResultSchema> = None;
    let mut rows = Vec::new();
    for event in events {
        match event {
            ExecEvent::Columns(s) => schema = Some((*s).clone()),
            ExecEvent::Batch(batch) => {
                let columns = &schema.as_ref().expect("columns come first").columns;
                for r in 0..batch.num_rows() {
                    rows.push(
                        (0..batch.num_columns())
                            .map(|c| {
                                Value::from_array(batch.column(c), r, columns[c].logical)
                                    .expect("a cell")
                            })
                            .collect(),
                    );
                }
            }
            ExecEvent::Notice(_) | ExecEvent::Done { .. } => {}
        }
    }
    (schema.unwrap_or_else(|| panic!("{sql}: no result")), rows)
}

/// Values come as Arrow and become cells of their logical types: each checked against the
/// value the server computed and wrote as text in the same row.
#[tokio::test]
async fn values_arrive_as_their_logical_types() {
    for server in servers() {
        let conn = server.connect().await;
        let conn = conn.as_ref();
        let name = server.name;
        let cases: Vec<(&str, LogicalType, &str)> = if name == "dremio" {
            vec![
                ("CAST(-7 AS INTEGER)", LogicalType::Int, "-7"),
                (
                    "CAST(9007199254740993 AS BIGINT)",
                    LogicalType::Int,
                    "9007199254740993",
                ),
                ("CAST(1.5 AS DOUBLE)", LogicalType::Float, "1.5"),
                (
                    "CAST(-12.34 AS DECIMAL(10, 2))",
                    LogicalType::Decimal,
                    "-12.34",
                ),
                ("'tab\tñ'", LogicalType::Text, "tab\tñ"),
                ("TRUE", LogicalType::Bool, "true"),
                ("DATE '2024-02-29'", LogicalType::Date, "2024-02-29"),
                (
                    "TIMESTAMP '2024-02-29 23:59:58.123'",
                    LogicalType::Timestamp,
                    "2024-02-29 23:59:58.123",
                ),
            ]
        } else {
            vec![
                ("CAST(-7 AS TINYINT)", LogicalType::Int, "-7"),
                (
                    "CAST(9007199254740993 AS BIGINT)",
                    LogicalType::Int,
                    "9007199254740993",
                ),
                (
                    "CAST(18446744073709551615 AS UBIGINT)",
                    LogicalType::Decimal,
                    "18446744073709551615",
                ),
                ("CAST(1.5 AS DOUBLE)", LogicalType::Float, "1.5"),
                (
                    "CAST(-12.34 AS DECIMAL(10, 2))",
                    LogicalType::Decimal,
                    "-12.34",
                ),
                ("'tab\tñ'", LogicalType::Text, "tab\tñ"),
                ("TRUE", LogicalType::Bool, "true"),
                ("DATE '2024-02-29'", LogicalType::Date, "2024-02-29"),
                (
                    "TIMESTAMP '2024-02-29 23:59:58.123456'",
                    LogicalType::Timestamp,
                    "2024-02-29 23:59:58.123456",
                ),
                ("'\\x00\\xff'::BLOB", LogicalType::Bytes, "\\x00ff"),
                ("[1, 2, NULL]", LogicalType::Native, "[1, 2, ]"),
            ]
        };
        for (expr, logical, text) in cases {
            let (schema, rows) = query(conn, &format!("SELECT {expr} AS v")).await;
            assert_eq!(
                schema.columns[0].logical, logical,
                "{name}: {expr}: {:?}",
                schema.columns[0]
            );
            assert_eq!(rows[0][0].to_string(), text, "{name}: {expr}");
        }
        let (_, rows) = query(conn, "SELECT CAST(NULL AS INTEGER) AS v").await;
        assert_eq!(rows[0][0], Value::Null, "{name}");
    }
}

#[tokio::test]
async fn a_large_result_comes_in_batches() {
    for server in servers() {
        let conn = server.connect().await;
        let sql = if server.name == "dremio" {
            "SELECT * FROM (VALUES (1)) t(a) CROSS JOIN (SELECT 1 AS b FROM sys.options) LIMIT 1"
                .to_string()
        } else {
            "SELECT range AS n, range::VARCHAR AS t FROM range(300000)".to_string()
        };
        let opts = ExecOptions {
            batch_size: 10_000,
            ..Default::default()
        };
        let events: Vec<ExecEvent> = conn
            .execute(&sql, &opts)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let rows: usize = events
            .iter()
            .map(|e| match e {
                ExecEvent::Batch(b) => {
                    assert!(b.num_rows() <= 10_000);
                    b.num_rows()
                }
                _ => 0,
            })
            .sum();
        if server.name != "dremio" {
            assert_eq!(rows, 300_000, "{}", server.name);
        }
        assert!(matches!(events.first(), Some(ExecEvent::Columns(_))));
        assert!(matches!(events.last(), Some(ExecEvent::Done { .. })));
    }
}

#[tokio::test]
async fn refusals_are_the_servers_errors() {
    for server in servers() {
        let conn = server.connect().await;
        match run(conn.as_ref(), "SELECT * FROM a_table_that_is_not_there").await {
            Err(DriverError::Database { message, .. }) => {
                assert!(!message.is_empty(), "{}", server.name);
            }
            other => panic!("{}: {other:?}", server.name),
        }
        // The connection is still usable.
        let (_, rows) = query(conn.as_ref(), "SELECT 1 AS v").await;
        assert_eq!(rows.len(), 1);
    }
}

#[tokio::test]
async fn a_cancel_stops_reading_the_result() {
    for server in servers().into_iter().filter(|s| s.name != "dremio") {
        let conn = server.connect().await;
        let cancel = conn.cancel_handle();
        let started = Instant::now();
        let (result, ()) = tokio::join!(
            tokio::time::timeout(
                Duration::from_secs(30),
                run(
                    conn.as_ref(),
                    "SELECT count(*) FROM range(1000000000) a, range(1000000) b"
                )
            ),
            async {
                tokio::time::sleep(Duration::from_millis(1500)).await;
                cancel.cancel().await.unwrap();
            }
        );
        let result = result.expect("the cancel was obeyed");
        assert!(
            matches!(result, Err(DriverError::Cancelled)),
            "{}: {result:?}",
            server.name
        );
        assert!(started.elapsed() < Duration::from_secs(25));
        let (_, rows) = query(conn.as_ref(), "SELECT 42 AS v").await;
        assert_eq!(rows[0][0], Value::Int(42));
    }
}

/// Catalogs, schemas and tables from Flight SQL's metadata calls.
#[tokio::test]
async fn the_explorer_reads_the_catalog() {
    for server in servers().into_iter().filter(|s| s.name != "dremio") {
        let name = server.name;
        let conn = server.connect().await;
        let conn = conn.as_ref();
        let table = format!("explored_{name}_{}", std::process::id());
        run(
            conn,
            &format!("CREATE OR REPLACE TABLE {table} (id BIGINT NOT NULL, note VARCHAR)"),
        )
        .await
        .unwrap();
        run(
            conn,
            &format!("CREATE OR REPLACE VIEW {table}_v AS SELECT * FROM {table}"),
        )
        .await
        .unwrap();
        let catalogs = conn.list_databases().await.unwrap();
        assert!(!catalogs.is_empty(), "{name}");
        let schemas = conn.list_schemas().await.unwrap();
        assert!(
            schemas.iter().any(|s| s.name == "main"),
            "{name}: {schemas:?}"
        );
        let objects = conn.list_objects("main").await.unwrap();
        assert!(
            objects.iter().any(|o| o.name == table),
            "{name}: {objects:?}"
        );
        let described = conn.describe_table("main", &table).await.unwrap();
        let columns: Vec<(&str, bool)> = described
            .columns
            .iter()
            .map(|c| (c.name.as_str(), c.nullable))
            .collect();
        assert_eq!(columns, [("id", false), ("note", true)], "{name}");
        assert!(
            conn.list_columns("main")
                .await
                .unwrap()
                .iter()
                .any(|c| c.table == table)
        );
        // A pattern's `_` matches any character: only the exact name counts.
        assert!(conn.list_objects("m_in").await.unwrap().is_empty());
        run(conn, &format!("DROP VIEW {table}_v")).await.unwrap();
        run(conn, &format!("DROP TABLE {table}")).await.unwrap();
    }
}

#[tokio::test]
async fn dremio_browses_its_spaces_and_sources() {
    for server in servers().into_iter().filter(|s| s.name == "dremio") {
        let conn = server.connect().await;
        // A new Dremio lists a source's tables once it has read its metadata: a query first.
        let (_, rows) = query(conn.as_ref(), "SELECT count(*) AS n FROM sys.options").await;
        assert_eq!(rows.len(), 1);
        let schemas = conn.list_schemas().await.unwrap();
        assert!(
            schemas.iter().any(|s| s.name.eq_ignore_ascii_case("sys")),
            "{schemas:?}"
        );
        let objects = conn.list_objects("sys").await.unwrap();
        assert!(objects.iter().any(|o| o.name == "options"), "{objects:?}");
        assert!(!conn.server_version().await.unwrap().is_empty());
    }
}

#[tokio::test]
async fn a_wrong_password_is_a_connection_error_that_does_not_repeat_it() {
    for server in servers() {
        let mut p = server.params();
        p.password = Some(Secret::new("not-the-password-9c2e"));
        match server.driver.connect(&p).await {
            Err(DriverError::Connection(message)) => {
                assert!(
                    !message.contains("not-the-password-9c2e"),
                    "{}: {message}",
                    server.name
                );
            }
            Err(other) => panic!("{}: {other:?}", server.name),
            Ok(_) => panic!("{}: logged in with a wrong password", server.name),
        }
    }
}

#[tokio::test]
async fn connecting_to_the_address_given_keeps_the_host_name() {
    for server in servers() {
        let mut p = server.params();
        p.host = "flight.invalid".into();
        p.port = 1;
        p.connect_addr = Some(format!("127.0.0.1:{}", server.port).parse().unwrap());
        let conn = server
            .driver
            .connect(&p)
            .await
            .unwrap_or_else(|e| panic!("{}: {e}", server.name));
        let (_, rows) = query(conn.as_ref(), "SELECT 1 AS v").await;
        assert_eq!(rows.len(), 1);
    }
}
