//! InfluxDB 3 Core through the generic `flightsql` engine: it chooses the database by the
//! `database` header, which the plugin sends with every call when the connection names one.
//!
//! Needs `SPELAION_TEST_INFLUXDB=host:port` (`testing/compose.yaml`, 127.0.0.1:18181, no
//! authentication; its HTTP API and Flight SQL share the port). Without it the tests are skipped
//! with a notice, which does not count as tested.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

use futures_util::TryStreamExt;
use spelaion_driver_api::{
    ConnectParams, Connection, Driver, DriverError, ExecEvent, ExecOptions, ResultSchema, TlsMode,
    Value,
};
use spelaion_driver_flightsql::FlightSqlDriver;

/// The test's own database, written to before each test (InfluxDB makes it on the first write).
const DATABASE: &str = "spelaion_flight";

fn address() -> Option<(String, u16)> {
    let Ok(addr) = std::env::var("SPELAION_TEST_INFLUXDB") else {
        eprintln!("SPELAION_TEST_INFLUXDB not set: test skipped");
        return None;
    };
    let (host, port) = addr.rsplit_once(':').expect("host:port");
    Some((host.into(), port.parse().expect("a port")))
}

/// Writes line protocol to `database` through InfluxDB's HTTP API (the same port).
fn write_lines(host: &str, port: u16, database: &str, lines: &str) {
    let mut stream = TcpStream::connect((host, port)).expect("InfluxDB's HTTP API");
    stream
        .set_read_timeout(Some(Duration::from_secs(20)))
        .unwrap();
    let request = format!(
        "POST /api/v3/write_lp?db={database} HTTP/1.1\r\nHost: {host}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{lines}",
        lines.len()
    );
    stream.write_all(request.as_bytes()).unwrap();
    let mut answer = String::new();
    stream.read_to_string(&mut answer).unwrap();
    let status = answer.lines().next().unwrap_or_default();
    assert!(status.contains(" 20"), "the write was refused: {answer}");
}

fn params(host: &str, port: u16, database: &str) -> ConnectParams {
    ConnectParams {
        host: host.into(),
        port,
        database: database.into(),
        tls: TlsMode::Disable,
        connect_timeout: Some(Duration::from_secs(20)),
        ..Default::default()
    }
}

async fn connect(database: &str) -> Option<Box<dyn Connection>> {
    let (host, port) = address()?;
    write_lines(
        &host,
        port,
        DATABASE,
        "cpu,host=alpha usage=12.5 1700000000000000000\ncpu,host=beta usage=40 1700000001000000000\n",
    );
    let conn = FlightSqlDriver::generic()
        .connect(&params(&host, port, database))
        .await
        .expect("connecting");
    Some(conn)
}

async fn run(conn: &dyn Connection, sql: &str) -> Result<Vec<ExecEvent>, DriverError> {
    conn.execute(sql, &ExecOptions::default())
        .await?
        .try_collect()
        .await
}

async fn rows(conn: &dyn Connection, sql: &str) -> Vec<Vec<Value>> {
    let mut schema: Option<ResultSchema> = None;
    let mut out = Vec::new();
    for event in run(conn, sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
    {
        match event {
            ExecEvent::Columns(s) => schema = Some((*s).clone()),
            ExecEvent::Batch(batch) => {
                let columns = &schema.as_ref().expect("columns come first").columns;
                for row in 0..batch.num_rows() {
                    out.push(
                        (0..batch.num_columns())
                            .map(|c| {
                                Value::from_array(batch.column(c), row, columns[c].logical)
                                    .expect("a cell")
                            })
                            .collect(),
                    );
                }
            }
            ExecEvent::Notice(_) | ExecEvent::Done { .. } => {}
        }
    }
    out
}

#[tokio::test]
async fn a_statement_reads_the_connections_database() {
    let Some(conn) = connect(DATABASE).await else {
        return;
    };
    let rows = rows(
        conn.as_ref(),
        "SELECT host, usage FROM cpu WHERE time <= '2023-11-14T22:13:21Z' ORDER BY host",
    )
    .await;
    assert_eq!(
        rows,
        [
            vec![Value::Text("alpha".into()), Value::Float(12.5)],
            vec![Value::Text("beta".into()), Value::Float(40.0)],
        ]
    );
}

#[tokio::test]
async fn without_a_database_the_server_says_it_needs_one() {
    let Some((host, port)) = address() else {
        return;
    };
    let conn = FlightSqlDriver::generic()
        .connect(&params(&host, port, ""))
        .await
        .expect("connecting");
    let error = run(conn.as_ref(), "SELECT 1").await.expect_err("refused");
    assert!(error.to_string().contains("database"), "{error}");
}

#[tokio::test]
async fn the_explorer_reads_the_databases_tables() {
    let Some(conn) = connect(DATABASE).await else {
        return;
    };
    let schemas = conn.list_schemas().await.unwrap();
    assert!(schemas.iter().any(|s| s.name == "iox"), "{schemas:?}");
    let objects = conn.list_objects("iox").await.unwrap();
    assert!(objects.iter().any(|o| o.name == "cpu"), "{objects:?}");
    let described = conn.describe_table("iox", "cpu").await.unwrap();
    let names: Vec<&str> = described.columns.iter().map(|c| c.name.as_str()).collect();
    for column in ["host", "time", "usage"] {
        assert!(names.contains(&column), "{names:?}");
    }
}

#[tokio::test]
async fn a_cancel_reaches_the_server_with_the_database() {
    let Some(conn) = connect(DATABASE).await else {
        return;
    };
    let cancel = conn.cancel_handle();
    let started = Instant::now();
    let (result, ()) = tokio::join!(
        tokio::time::timeout(
            Duration::from_secs(30),
            run(
                conn.as_ref(),
                "SELECT count(*) FROM generate_series(1, 1000000000) a, generate_series(1, 1000000) b"
            )
        ),
        async {
            tokio::time::sleep(Duration::from_millis(1500)).await;
            cancel.cancel().await.unwrap();
        }
    );
    let result = result.expect("the cancel was obeyed");
    assert!(matches!(result, Err(DriverError::Cancelled)), "{result:?}");
    assert!(started.elapsed() < Duration::from_secs(25));
    // The connection still reads its database afterwards.
    let after = rows(conn.as_ref(), "SELECT count(*) FROM cpu").await;
    assert_eq!(after.len(), 1);
}
