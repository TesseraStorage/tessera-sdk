use crate::time::{Elapsed, Instant, timeout};

use bytes::Bytes;
use core::fmt::Debug;
use ed25519_dalek::{SignatureError, VerifyingKey};
use log::debug;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use thiserror::{self, Error};
use tokio::net::{TcpStream, lookup_host};
use tokio::sync::OnceCell;

use crate::rhp4::HostEndpoint;
use crate::time::Duration;

use super::{Error as TransportError, Transport};
use sia_core::rhp4::protocol::{RPCReadSector, RPCSettings, RPCWriteSector};
use sia_core::rhp4::{AccountToken, HostPrices};
use sia_core::signing::{PrivateKey, PublicKey};
use sia_core::types::Hash256;
use sia_core::types::v2::Protocol;
use sia_mux::{Mux, Stream};

#[derive(Debug, Error)]
pub enum ConnectError {
    #[error("connect error: {0}")]
    Io(#[from] std::io::Error),

    #[error("mux dial error: {0}")]
    Dial(#[from] sia_mux::DialError),

    #[error("mux error: {0}")]
    Mux(#[from] sia_mux::MuxError),

    #[error("timeout error: {0}")]
    Elapsed(#[from] Elapsed),

    #[error("invalid public key: {0}")]
    InvalidPublicKey(#[from] SignatureError),

    #[error("no endpoint")]
    NoEndpoint,
}

type ConnCell = Arc<OnceCell<Arc<Mux>>>;

#[derive(Clone)]
pub struct Client {
    open_conns: Arc<RwLock<HashMap<PublicKey, ConnCell>>>,
}

impl Default for Client {
    fn default() -> Self {
        Self::new()
    }
}

impl Client {
    pub fn new() -> Self {
        Self {
            open_conns: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    async fn new_conn(&self, host: &HostEndpoint) -> Result<Mux, ConnectError> {
        let host_bytes: [u8; 32] = host.public_key.into();
        let verifying_key = VerifyingKey::from_bytes(&host_bytes)?;

        for addr in &host.addresses {
            if addr.protocol != Protocol::SiaMux {
                continue;
            }
            // Pass the announced address through unchanged. IPv6 addresses are
            // bracketed (`[::1]:port`); splitting on the last colon left the
            // brackets attached to the host, so lookup failed for every IPv6 host.
            let resolved_addrs = lookup_host(addr.address.as_str())
                .await
                .inspect_err(|e| debug!("failed to resolve {}: {e}", addr.address))?;

            for socket in resolved_addrs {
                match TcpStream::connect(socket).await {
                    Ok(tcp) => match sia_mux::dial(tcp, &verifying_key).await {
                        Ok(mux_conn) => {
                            debug!(
                                "established siamux connection to {} via {socket}",
                                host.public_key
                            );
                            return Ok(mux_conn);
                        }
                        Err(e) => {
                            debug!(
                                "mux handshake failed to {} via {socket}: {e}",
                                host.public_key
                            );
                        }
                    },
                    Err(e) => {
                        debug!("TCP connect failed to {} ({socket}): {e}", addr.address);
                    }
                }
            }
        }
        Err(ConnectError::NoEndpoint)
    }

    async fn host_stream(&self, host: &HostEndpoint) -> Result<Stream, ConnectError> {
        let cell = if let Some(cell) = self.open_conns.read().unwrap().get(&host.public_key) {
            cell.clone()
        } else {
            self.open_conns
                .write()
                .unwrap()
                .entry(host.public_key)
                .or_insert_with(|| Arc::new(OnceCell::new()))
                .clone()
        };
        let conn = cell
            .get_or_try_init(|| async {
                let mux = timeout(Duration::from_secs(10), self.new_conn(host))
                    .await
                    .inspect_err(|e| {
                        debug!("siamux connection to {} timed out: {e}", host.public_key);
                    })??;
                debug!("created new siamux connection to {}", host.public_key);
                Ok::<_, ConnectError>(Arc::new(mux))
            })
            .await?
            .clone();

        let stream = conn.dial_stream().inspect_err(|_| {
            self.open_conns.write().unwrap().remove(&host.public_key);
        })?;
        Ok(stream)
    }
}

impl Transport for Client {
    async fn host_prices(
        &self,
        host: &HostEndpoint,
    ) -> Result<(HostPrices, Duration), TransportError> {
        let mut stream = self
            .host_stream(host)
            .await
            .map_err(|e| TransportError::Transport(e.to_string()))?;
        let start = Instant::now();
        let resp = RPCSettings::send_request(&mut stream)
            .await?
            .complete(&mut stream)
            .await?;
        Ok((resp.settings.prices, start.elapsed()))
    }

    async fn write_sector(
        &self,
        host: &HostEndpoint,
        prices: HostPrices,
        account_key: &PrivateKey,
        data: Bytes,
    ) -> Result<(Hash256, Duration), TransportError> {
        let token = AccountToken::new(account_key, host.public_key);
        let mut stream = self
            .host_stream(host)
            .await
            .map_err(|e| TransportError::Transport(e.to_string()))?;
        let start = Instant::now();
        let resp = RPCWriteSector::send_request(&mut stream, prices, token, data)
            .await?
            .complete(&mut stream)
            .await?;
        Ok((resp.root, start.elapsed()))
    }

    async fn read_sector(
        &self,
        host: &HostEndpoint,
        prices: HostPrices,
        account_key: &PrivateKey,
        root: Hash256,
        offset: usize,
        length: usize,
    ) -> Result<(Bytes, Duration), TransportError> {
        let token = AccountToken::new(account_key, host.public_key);
        let mut stream = self
            .host_stream(host)
            .await
            .map_err(|e| TransportError::Transport(e.to_string()))?;
        let start = Instant::now();
        let resp = RPCReadSector::send_request(&mut stream, prices, token, root, offset, length)
            .await?
            .complete(&mut stream)
            .await?;
        Ok((resp.data, start.elapsed()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sia_core::types::v2::NetAddress;
    use tokio::net::TcpListener;

    /// An announced IPv6 address is bracketed, and the brackets are not part
    /// of the address. Splitting on the last colon left them attached to the
    /// host, so every IPv6 host failed to resolve and no shard could reach it.
    ///
    /// The listener speaks no mux, so the connection is expected to fail. What
    /// matters is where: NoEndpoint means the address resolved and the socket
    /// connected, while a resolution failure surfaces as Io.
    #[tokio::test]
    async fn test_ipv6_address_resolves() {
        let listener = match TcpListener::bind("[::1]:0").await {
            Ok(l) => l,
            // no IPv6 loopback on this machine, nothing to assert
            Err(_) => return,
        };
        let port = listener.local_addr().unwrap().port();
        // Accept and drop, so the mux handshake fails on EOF. new_conn has no
        // timeout of its own, so a listener that never answers hangs forever.
        tokio::spawn(async move {
            let _ = listener.accept().await;
        });

        let host = HostEndpoint {
            public_key: PrivateKey::from_seed(&[1u8; 32]).public_key(),
            addresses: vec![NetAddress {
                protocol: Protocol::SiaMux,
                address: format!("[::1]:{port}"),
            }],
        };

        let client = Client::new();
        match client.new_conn(&host).await {
            Err(ConnectError::NoEndpoint) => {}
            Err(ConnectError::Io(e)) => panic!("address did not resolve: {e}"),
            Err(e) => panic!("expected NoEndpoint, got {e}"),
            Ok(_) => panic!("expected the mux handshake to fail"),
        }
    }

    /// IPv4 announcements stay `host:port`. The same lookup path used for
    /// bracketed IPv6 must still resolve them.
    #[tokio::test]
    async fn test_ipv4_address_resolves() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let _ = listener.accept().await;
        });

        let host = HostEndpoint {
            public_key: PrivateKey::from_seed(&[2u8; 32]).public_key(),
            addresses: vec![NetAddress {
                protocol: Protocol::SiaMux,
                address: format!("127.0.0.1:{port}"),
            }],
        };

        let client = Client::new();
        match client.new_conn(&host).await {
            Err(ConnectError::NoEndpoint) => {}
            Err(ConnectError::Io(e)) => panic!("address did not resolve: {e}"),
            Err(e) => panic!("expected NoEndpoint, got {e}"),
            Ok(_) => panic!("expected the mux handshake to fail"),
        }
    }
}
