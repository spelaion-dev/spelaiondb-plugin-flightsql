//! The plugin's program: the Flight SQL driver, served to SpelaionDB over its plugin protocol on
//! the program's standard input and output (SpelaionDB's ADR-041).
//!
//! It does nothing else. Standard output is the protocol: nothing here, and nothing in the
//! driver, ever prints to it.

use std::sync::Arc;

use spelaion_driver_flightsql::FlightSqlDriver;
use spelaion_plugin_sdk::{Plugin, run};

#[tokio::main]
async fn main() {
    // One process may have several rustls providers linked in: say which.
    let _ = rustls::crypto::ring::default_provider().install_default();
    run(Plugin::new(
        "spelaiondb-plugin-flightsql",
        env!("CARGO_PKG_VERSION"),
        vec![
            Arc::new(FlightSqlDriver::generic()),
            Arc::new(FlightSqlDriver::dremio()),
            Arc::new(FlightSqlDriver::gizmosql()),
        ],
    ))
    .await
}
