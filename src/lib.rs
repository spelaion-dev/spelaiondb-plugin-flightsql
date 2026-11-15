//! Arrow Flight SQL for SpelaionDB: one plugin for every engine that speaks it (SpelaionDB's
//! ADR-041 and ADR-042). It serves a generic `flightsql` connection and named ones, `dremio` and
//! `gizmosql`, which differ only in what SpelaionDB's form offers for them.
//!
//! A statement goes to the server as a `CommandStatementQuery`; the answer names where its
//! results are (tickets), and each is read with `DoGet` as a stream of Arrow batches, converted
//! to SpelaionDB's logical types ([`types`]) without passing through any text. Results on another
//! server (an endpoint with a location of its own) are refused: everything goes through the
//! route SpelaionDB opened.
//!
//! A cancel stops reading (the gRPC call is ended, which the server sees) and asks the server
//! to cancel the statement (`CancelFlightInfo`) where it can.
//!
//! The catalog comes from Flight SQL's own metadata calls: catalogs (the explorer's databases),
//! schemas and tables, with each table's Arrow schema for its columns.
//!
//! Declared, so SpelaionDB hides it: transactions, editing, importing, binding values, plans.
//! SpelaionDB gives these engines no read-only mode and no agents (no barrier of ours).

mod channel;
pub mod tls;
pub mod types;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::{Array, RecordBatch, StringArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Schema};
use arrow_flight::error::FlightError;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::sql::{CommandGetDbSchemas, CommandGetTables};
use arrow_flight::{CancelFlightInfoRequest, FlightClient, FlightInfo};
use async_trait::async_trait;
use futures_util::StreamExt;
use spelaion_driver_api::{
    CancelHandle, Capabilities, ColumnInfo, ConnectParams, Connection, Driver, DriverError,
    EventStream, ExecEvent, ExecOptions, ObjectInfo, ObjectKind, ReferencingKey, Result,
    SchemaColumn, SchemaInfo, Secret, TableInfo,
};
use tokio::sync::{mpsc, oneshot, watch};
use tonic::Code;
use tonic::transport::Channel;

use crate::tls::ClientIdentity;

/// One engine name this plugin serves.
#[derive(Debug, Clone)]
pub struct FlightSqlDriver {
    engine: &'static str,
}

impl FlightSqlDriver {
    /// Any server that speaks Flight SQL.
    pub fn generic() -> Self {
        Self {
            engine: "flightsql",
        }
    }

    pub fn dremio() -> Self {
        Self { engine: "dremio" }
    }

    pub fn gizmosql() -> Self {
        Self { engine: "gizmosql" }
    }
}

#[async_trait]
impl Driver for FlightSqlDriver {
    fn engine(&self) -> &str {
        self.engine
    }

    async fn connect(&self, params: &ConnectParams) -> Result<Box<dyn Connection>> {
        let identity = match (&params.client_certificate, &params.client_key) {
            (Some(cert), Some(key)) => Some(ClientIdentity { cert, key }),
            (None, None) => None,
            _ => {
                return Err(DriverError::Connection(
                    "a client certificate needs its key, and a key its certificate".into(),
                ));
            }
        };
        let tls = tls::configure(params.tls, params.ca_certificate.as_deref(), identity)?;
        let timeout = params.connect_timeout();
        let channel =
            channel::open(&params.host, params.port, params.connect_addr, tls, timeout).await?;
        let scrub = Scrub(params.password.clone());
        let mut client = FlightSqlServiceClient::new(channel.clone());
        let password = params.password.as_ref().map(Secret::expose).unwrap_or("");
        if !params.user.is_empty() || !password.is_empty() {
            match tokio::time::timeout(timeout, client.handshake(&params.user, password)).await {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => {
                    return Err(DriverError::Connection(
                        scrub.text(&format!("the server refused the login: {e}")),
                    ));
                }
                Err(_) => {
                    return Err(DriverError::Connection(format!(
                        "the server did not answer within {} s",
                        timeout.as_secs()
                    )));
                }
            }
        }
        let token = client.token().cloned();
        let catalog = (!params.database.is_empty()).then(|| params.database.clone());
        let (cancel_tx, _) = watch::channel(0u64);
        Ok(Box::new(FlightSqlConnection {
            capabilities: Capabilities {
                transactions: false,
                cancel: true,
                explain: false,
                editable_results: false,
                schemas: true,
                insert_rows: false,
                bound_params: false,
            },
            channel,
            token,
            catalog: Mutex::new(catalog),
            cancel: Arc::new(Cancel {
                sent: AtomicU64::new(0),
                upto: cancel_tx,
            }),
            closed: Arc::new(AtomicBool::new(false)),
            scrub,
        }))
    }
}

/// Takes the password out of what the server or the transport says.
#[derive(Clone)]
struct Scrub(Option<Secret>);

impl Scrub {
    fn text(&self, text: &str) -> String {
        match &self.0 {
            Some(password) if !password.expose().is_empty() => {
                text.replace(password.expose(), "***")
            }
            _ => text.to_string(),
        }
    }
}

/// Cancels: remembers the last statement sent when it was asked, so that statement and every
/// one before it stop, and none sent after it is touched.
struct Cancel {
    sent: AtomicU64,
    upto: watch::Sender<u64>,
}

struct FlightSqlCancel(Arc<Cancel>);

#[async_trait]
impl CancelHandle for FlightSqlCancel {
    async fn cancel(&self) -> Result<()> {
        let sent = self.0.sent.load(Ordering::SeqCst);
        self.0.upto.send_if_modified(|upto| {
            if *upto < sent {
                *upto = sent;
                true
            } else {
                false
            }
        });
        Ok(())
    }
}

struct FlightSqlConnection {
    capabilities: Capabilities,
    channel: Channel,
    /// The bearer token the handshake gave, sent with every call.
    token: Option<String>,
    /// The catalog the explorer shows (Flight SQL has no session catalog to change).
    catalog: Mutex<Option<String>>,
    cancel: Arc<Cancel>,
    closed: Arc<AtomicBool>,
    scrub: Scrub,
}

/// What a call's failure means, by its gRPC status: the server or the route gone
/// (`Unavailable`) loses the connection, a cancel is a cancel, a login that is no longer good is
/// a connection error, and anything else is the server refusing the statement (its code kept).
fn classify(error: FlightError, scrub: &Scrub, closed: &AtomicBool) -> DriverError {
    let FlightError::Tonic(status) = error else {
        return DriverError::Database {
            code: None,
            message: scrub.text(&error.to_string()),
            detail: None,
            hint: None,
            position: None,
        };
    };
    let message = scrub.text(status.message());
    let message = if message.is_empty() {
        format!("{:?}", status.code())
    } else {
        message
    };
    match status.code() {
        Code::Unavailable => {
            closed.store(true, Ordering::SeqCst);
            DriverError::Connection(message)
        }
        Code::Cancelled => DriverError::Cancelled,
        Code::Unauthenticated => DriverError::Connection(message),
        code => DriverError::Database {
            code: Some(format!("{code:?}")),
            message,
            detail: None,
            hint: None,
            position: None,
        },
    }
}

impl FlightSqlConnection {
    fn client(&self) -> FlightSqlServiceClient<Channel> {
        let mut client = FlightSqlServiceClient::new(self.channel.clone());
        if let Some(token) = &self.token {
            client.set_token(token.clone());
        }
        client
    }

    fn current_catalog(&self) -> Option<String> {
        self.catalog.lock().expect("lock poisoned").clone()
    }

    fn error(&self, error: FlightError) -> DriverError {
        classify(error, &self.scrub, &self.closed)
    }

    /// Every batch of an answer, read whole: what the catalog calls read.
    async fn fetch(&self, info: FlightInfo) -> Result<Vec<RecordBatch>> {
        check_endpoints(&info)?;
        let mut client = self.client();
        let mut out = Vec::new();
        for endpoint in info.endpoint {
            let Some(ticket) = endpoint.ticket else {
                continue;
            };
            let mut stream = client.do_get(ticket).await.map_err(|e| self.error(e))?;
            while let Some(batch) = stream.next().await {
                out.push(batch.map_err(|e| self.error(e))?);
            }
        }
        Ok(out)
    }

    async fn tables(
        &self,
        schema: &str,
        table: Option<&str>,
        include_schema: bool,
    ) -> Result<Vec<TableRow>> {
        let info = self
            .client()
            .get_tables(CommandGetTables {
                catalog: self.current_catalog(),
                db_schema_filter_pattern: Some(schema.to_string()),
                table_name_filter_pattern: table.map(str::to_string),
                table_types: Vec::new(),
                include_schema,
            })
            .await
            .map_err(|e| self.error(e))?;
        let mut rows = Vec::new();
        for batch in self.fetch(info).await? {
            let schemas = strings(&batch, "db_schema_name")?;
            let names = strings(&batch, "table_name")?;
            let types = strings(&batch, "table_type")?;
            let ipc = if include_schema {
                batch.column_by_name("table_schema").and_then(|c| {
                    c.as_any()
                        .downcast_ref::<arrow::array::BinaryArray>()
                        .cloned()
                })
            } else {
                None
            };
            for i in 0..batch.num_rows() {
                // The filters are patterns (`_` is any character): only exact names count.
                if schemas[i].as_deref() != Some(schema) {
                    continue;
                }
                let name = names[i].clone().unwrap_or_default();
                if table.is_some_and(|t| t != name) {
                    continue;
                }
                let columns = match &ipc {
                    Some(array) if !array.is_null(i) => Some(
                        arrow::ipc::convert::try_schema_from_ipc_buffer(array.value(i)).map_err(
                            |e| {
                                DriverError::Other(format!(
                                    "the table's schema cannot be read: {e}"
                                ))
                            },
                        )?,
                    ),
                    _ => None,
                };
                rows.push(TableRow {
                    name,
                    kind: types[i].clone().unwrap_or_default(),
                    columns,
                });
            }
        }
        Ok(rows)
    }
}

struct TableRow {
    name: String,
    kind: String,
    columns: Option<Schema>,
}

fn kind(table_type: &str) -> ObjectKind {
    match table_type.to_ascii_uppercase().as_str() {
        "VIEW" | "SYSTEM VIEW" => ObjectKind::View,
        "MATERIALIZED VIEW" => ObjectKind::MaterializedView,
        _ => ObjectKind::Table,
    }
}

/// A column of a metadata answer as text, by name.
fn strings(batch: &RecordBatch, name: &str) -> Result<Vec<Option<String>>> {
    let Some(column) = batch.column_by_name(name) else {
        return Ok(vec![None; batch.num_rows()]);
    };
    let column = cast(column, &DataType::Utf8)
        .map_err(|e| DriverError::Other(format!("metadata column {name}: {e}")))?;
    let column = column
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DriverError::Other(format!("metadata column {name} is not text")))?;
    Ok((0..column.len())
        .map(|i| (!column.is_null(i)).then(|| column.value(i).to_string()))
        .collect())
}

/// Results on another server would be reached outside the route SpelaionDB opened: refused.
fn check_endpoints(info: &FlightInfo) -> Result<()> {
    let elsewhere = info.endpoint.iter().any(|endpoint| {
        endpoint
            .location
            .iter()
            .any(|l| !l.uri.is_empty() && !l.uri.starts_with("arrow-flight-reuse-connection:"))
    });
    if elsewhere {
        return Err(DriverError::Other(
            "the server put the results on another server, which this connection does not reach"
                .into(),
        ));
    }
    Ok(())
}

#[async_trait]
impl Connection for FlightSqlConnection {
    fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }

    async fn execute(&self, sql: &str, opts: &ExecOptions) -> Result<EventStream> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(DriverError::Connection("the connection is closed".into()));
        }
        let seq = self.cancel.sent.fetch_add(1, Ordering::SeqCst) + 1;
        let mut cancel = self.cancel.upto.subscribe();
        if *cancel.borrow() >= seq {
            return Err(DriverError::Cancelled);
        }
        let mut client = self.client();
        let info = {
            let execute = client.execute(sql.to_string(), None);
            tokio::pin!(execute);
            loop {
                tokio::select! {
                    info = &mut execute => break info.map_err(|e| self.error(e))?,
                    changed = cancel.changed() => {
                        if changed.is_err() || *cancel.borrow() >= seq {
                            return Err(DriverError::Cancelled);
                        }
                    }
                }
            }
        };
        check_endpoints(&info)?;
        let schema = info
            .clone()
            .try_decode_schema()
            .map_err(|e| DriverError::Other(format!("the result's schema cannot be read: {e}")))?;
        let result = (!schema.fields().is_empty()).then(|| types::result_schema(&schema));

        let (events, receiver) = mpsc::channel(4);
        let (started, answer) = oneshot::channel::<()>();
        let channel = self.channel.clone();
        let token = self.token.clone();
        let scrub = self.scrub.clone();
        let closed = self.closed.clone();
        let batch_size = opts.batch_size.max(1);
        tokio::spawn(async move {
            let _ = started.send(());
            let outcome: Result<()> = async {
                if let Some(result) = &result
                    && events
                        .send(Ok(ExecEvent::Columns(result.clone())))
                        .await
                        .is_err()
                {
                    return Ok(());
                }
                let mut client = FlightSqlServiceClient::new(channel.clone());
                if let Some(token) = &token {
                    client.set_token(token.clone());
                }
                for endpoint in &info.endpoint {
                    let Some(ticket) = endpoint.ticket.clone() else {
                        continue;
                    };
                    let mut stream = client
                        .do_get(ticket)
                        .await
                        .map_err(|e| classify(e, &scrub, &closed))?;
                    loop {
                        let next = tokio::select! {
                            next = stream.next() => next,
                            changed = cancel.changed() => {
                                if changed.is_err() || *cancel.borrow() >= seq {
                                    cancel_on_server(&channel, &token, &info).await;
                                    return Err(DriverError::Cancelled);
                                }
                                continue;
                            }
                        };
                        let Some(batch) = next else {
                            break;
                        };
                        let batch = batch.map_err(|e| classify(e, &scrub, &closed))?;
                        let Some(result) = &result else {
                            continue;
                        };
                        let converted = types::convert(&batch, result)?;
                        let mut offset = 0;
                        while offset < converted.num_rows() {
                            let length = batch_size.min(converted.num_rows() - offset);
                            if events
                                .send(Ok(ExecEvent::Batch(converted.slice(offset, length))))
                                .await
                                .is_err()
                            {
                                // Nobody reads any more: the statement stops.
                                cancel_on_server(&channel, &token, &info).await;
                                return Ok(());
                            }
                            offset += length;
                        }
                    }
                }
                let _ = events
                    .send(Ok(ExecEvent::Done {
                        rows_affected: None,
                    }))
                    .await;
                Ok(())
            }
            .await;
            if let Err(e) = outcome {
                let _ = events.send(Err(e)).await;
            }
        });
        let _ = answer.await;
        let mut receiver = receiver;
        Ok(futures_util::stream::poll_fn(move |cx| receiver.poll_recv(cx)).boxed())
    }

    fn cancel_handle(&self) -> Arc<dyn CancelHandle> {
        Arc::new(FlightSqlCancel(self.cancel.clone()))
    }

    /// What the server says of itself, where it says it (`FLIGHT_SQL_SERVER_NAME` and
    /// `…_VERSION`).
    async fn server_version(&self) -> Result<String> {
        use arrow_flight::sql::SqlInfo;
        let info = self
            .client()
            .get_sql_info(vec![
                SqlInfo::FlightSqlServerName,
                SqlInfo::FlightSqlServerVersion,
            ])
            .await
            .map_err(|e| self.error(e))?;
        let batches = self.fetch(info).await?;
        let mut parts = Vec::new();
        for batch in &batches {
            let Some(values) = batch.column_by_name("value") else {
                continue;
            };
            let options = arrow::util::display::FormatOptions::default();
            if let Ok(formatter) =
                arrow::util::display::ArrayFormatter::try_new(values.as_ref(), &options)
            {
                for i in 0..values.len() {
                    let text = formatter.value(i).to_string();
                    // A dense union is written `{tag=value}`.
                    let text = text
                        .split_once('=')
                        .map(|(_, v)| v.trim_end_matches('}').to_string())
                        .unwrap_or(text);
                    parts.push(text);
                }
            }
        }
        Ok(parts.join(" "))
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    async fn current_database(&self) -> Result<Option<String>> {
        Ok(self.current_catalog())
    }

    async fn select_database(&self, database: &str) -> Result<()> {
        *self.catalog.lock().expect("lock poisoned") = Some(database.to_string());
        Ok(())
    }

    async fn list_databases(&self) -> Result<Vec<String>> {
        let info = self
            .client()
            .get_catalogs()
            .await
            .map_err(|e| self.error(e))?;
        let mut out = Vec::new();
        for batch in self.fetch(info).await? {
            out.extend(strings(&batch, "catalog_name")?.into_iter().flatten());
        }
        Ok(out)
    }

    async fn list_schemas(&self) -> Result<Vec<SchemaInfo>> {
        let info = self
            .client()
            .get_db_schemas(CommandGetDbSchemas {
                catalog: self.current_catalog(),
                db_schema_filter_pattern: None,
            })
            .await
            .map_err(|e| self.error(e))?;
        let mut out = Vec::new();
        for batch in self.fetch(info).await? {
            for name in strings(&batch, "db_schema_name")?.into_iter().flatten() {
                let lower = name.to_ascii_lowercase();
                out.push(SchemaInfo {
                    system: matches!(
                        lower.as_str(),
                        "information_schema" | "pg_catalog" | "sys" | "$scratch"
                    ),
                    name,
                });
            }
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out.dedup_by(|a, b| a.name == b.name);
        Ok(out)
    }

    async fn list_objects(&self, schema: &str) -> Result<Vec<ObjectInfo>> {
        let mut rows = self.tables(schema, None, false).await?;
        rows.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(rows
            .into_iter()
            .map(|row| ObjectInfo {
                schema: schema.to_string(),
                kind: kind(&row.kind),
                name: row.name,
                estimated_rows: None,
                comment: None,
            })
            .collect())
    }

    async fn describe_table(&self, schema: &str, name: &str) -> Result<TableInfo> {
        let row = self
            .tables(schema, Some(name), true)
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| DriverError::Other(format!("there is no table {schema}.{name}")))?;
        let columns = row
            .columns
            .as_ref()
            .map(|s| {
                s.fields()
                    .iter()
                    .map(|field| ColumnInfo {
                        name: field.name().clone(),
                        native_type: field
                            .metadata()
                            .get("ARROW:FLIGHT:SQL:TYPE_NAME")
                            .cloned()
                            .unwrap_or_else(|| field.data_type().to_string()),
                        nullable: field.is_nullable(),
                        default: None,
                        comment: None,
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(TableInfo {
            object: ObjectInfo {
                schema: schema.to_string(),
                name: row.name,
                kind: kind(&row.kind),
                estimated_rows: None,
                comment: None,
            },
            columns,
            primary_key: Vec::new(),
            indexes: Vec::new(),
            foreign_keys: Vec::new(),
            constraints: Vec::new(),
        })
    }

    async fn list_columns(&self, schema: &str) -> Result<Vec<SchemaColumn>> {
        let mut out = Vec::new();
        for row in self.tables(schema, None, true).await? {
            if let Some(columns) = &row.columns {
                for field in columns.fields() {
                    out.push(SchemaColumn {
                        table: row.name.clone(),
                        name: field.name().clone(),
                        native_type: field
                            .metadata()
                            .get("ARROW:FLIGHT:SQL:TYPE_NAME")
                            .cloned()
                            .unwrap_or_else(|| field.data_type().to_string()),
                    });
                }
            }
        }
        Ok(out)
    }

    async fn list_referencing_keys(
        &self,
        _schema: &str,
        _name: &str,
    ) -> Result<Vec<ReferencingKey>> {
        Ok(Vec::new())
    }
}

/// Asks the server to cancel the statement `info` answered for, where it can; best effort.
async fn cancel_on_server(channel: &Channel, token: &Option<String>, info: &FlightInfo) {
    let mut client = FlightClient::new(channel.clone());
    if let Some(token) = token
        && client
            .add_header("authorization", &format!("Bearer {token}"))
            .is_err()
    {
        return;
    }
    let request = CancelFlightInfoRequest::new(info.clone());
    let _ = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        client.cancel_flight_info(request),
    )
    .await;
}
