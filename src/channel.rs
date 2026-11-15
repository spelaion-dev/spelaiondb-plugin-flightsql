//! The gRPC channel to a Flight SQL server, over a connection this driver opens itself.
//!
//! tonic is handed our own connector: a TCP connection to the address SpelaionDB gave
//! (`connect_addr`, the local end of a tunnel it opened) or to `host:port`, never through a
//! proxy, then TLS as its mode asks (`tls.rs`), verified against `host`, which is also the name
//! sent in SNI and in HTTP/2's `:authority`.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

use hyper_util::rt::TokioIo;
use rustls::pki_types::ServerName;
use spelaion_driver_api::{DriverError, Result};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tonic::transport::{Channel, Endpoint, Uri};

use crate::tls::Tls;

pub trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

/// Where the server is and how to reach it.
#[derive(Clone)]
struct Route {
    host: String,
    port: u16,
    connect_addr: Option<SocketAddr>,
    tls: Tls,
    /// `prefer` once decided: 0 not yet, 1 TLS, 2 plain.
    preferred: Arc<AtomicU8>,
}

impl Route {
    async fn tcp(&self) -> std::io::Result<TcpStream> {
        let stream = match self.connect_addr {
            Some(addr) => TcpStream::connect(addr).await?,
            None => TcpStream::connect((self.host.as_str(), self.port)).await?,
        };
        let _ = stream.set_nodelay(true);
        Ok(stream)
    }

    async fn tls(
        &self,
        config: &Arc<rustls::ClientConfig>,
        tcp: TcpStream,
    ) -> std::io::Result<Box<dyn Io>> {
        let name = ServerName::try_from(self.host.clone())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
        Ok(Box::new(
            TlsConnector::from(config.clone())
                .connect(name, tcp)
                .await?,
        ))
    }

    async fn open(self) -> std::io::Result<TokioIo<Box<dyn Io>>> {
        let tcp = self.tcp().await?;
        let io: Box<dyn Io> = match &self.tls {
            Tls::Off => Box::new(tcp),
            Tls::Required(config) => self.tls(config, tcp).await?,
            Tls::Preferred(config) => match self.preferred.load(Ordering::SeqCst) {
                1 => self.tls(config, tcp).await?,
                2 => Box::new(tcp),
                _ => match self.tls(config, tcp).await {
                    Ok(io) => {
                        self.preferred.store(1, Ordering::SeqCst);
                        io
                    }
                    Err(_) => {
                        self.preferred.store(2, Ordering::SeqCst);
                        Box::new(self.tcp().await?)
                    }
                },
            },
        };
        Ok(TokioIo::new(io))
    }
}

/// A channel to the server, connected (so a server that is not there is an error now).
pub async fn open(
    host: &str,
    port: u16,
    connect_addr: Option<SocketAddr>,
    tls: Tls,
    connect_timeout: Duration,
) -> Result<Channel> {
    let authority = if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    };
    // Always `http`: TLS is ours, in the connector.
    let endpoint = Endpoint::from_shared(format!("http://{authority}"))
        .map_err(|e| DriverError::Connection(format!("{authority} is not an address: {e}")))?
        .connect_timeout(connect_timeout)
        .tcp_nodelay(true);
    let route = Route {
        host: host.to_string(),
        port,
        connect_addr,
        tls,
        preferred: Arc::new(AtomicU8::new(0)),
    };
    let connector = tower::service_fn(move |_: Uri| route.clone().open());
    match tokio::time::timeout(connect_timeout, endpoint.connect_with_connector(connector)).await {
        Ok(Ok(channel)) => Ok(channel),
        Ok(Err(e)) => Err(DriverError::Connection(format!(
            "could not reach {authority}: {}",
            error_chain(&e)
        ))),
        Err(_) => Err(DriverError::Connection(format!(
            "could not connect to {authority} within {} s",
            connect_timeout.as_secs()
        ))),
    }
}

/// An error and every cause under it, on one line.
pub fn error_chain(error: &dyn std::error::Error) -> String {
    let mut text = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        let line = cause.to_string();
        if !text.contains(&line) {
            text.push_str(": ");
            text.push_str(&line);
        }
        source = cause.source();
    }
    text
}
