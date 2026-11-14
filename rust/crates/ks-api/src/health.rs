//! `ks-api healthcheck`: the container's healthcheck, from inside the
//! container. The image is distroless — no shell, no curl — so the binary asks
//! its own `/readyz` over loopback and exits 0 or 1.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Where to dial a listener bound to `listen`: an unspecified address
/// (`0.0.0.0`, `::`) is every interface, loopback among them.
pub fn loopback(listen: SocketAddr) -> SocketAddr {
    match listen.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => (Ipv4Addr::LOCALHOST, listen.port()).into(),
        IpAddr::V6(ip) if ip.is_unspecified() => (Ipv6Addr::LOCALHOST, listen.port()).into(),
        _ => listen,
    }
}

/// `GET path` on `target`: whether the answer's status is 200.
pub async fn probe(target: SocketAddr, path: &str) -> std::io::Result<bool> {
    let mut stream = TcpStream::connect(target).await?;
    let request = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await?;
    let mut status_line = [0u8; 12];
    stream.read_exact(&mut status_line).await?;
    Ok(&status_line == b"HTTP/1.1 200")
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::{loopback, probe};

    #[test]
    fn an_unspecified_listener_is_dialled_on_loopback() {
        let any: SocketAddr = "0.0.0.0:8090".parse().unwrap();
        assert_eq!(loopback(any), "127.0.0.1:8090".parse().unwrap());
        let any6: SocketAddr = "[::]:8090".parse().unwrap();
        assert_eq!(loopback(any6), "[::1]:8090".parse().unwrap());
        let one: SocketAddr = "10.0.0.5:8090".parse().unwrap();
        assert_eq!(loopback(one), one);
    }

    async fn answering(status_line: &'static str) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 256];
            let _ = socket.read(&mut request).await;
            let reply = format!("{status_line}\r\ncontent-length: 0\r\n\r\n");
            socket.write_all(reply.as_bytes()).await.unwrap();
        });
        addr
    }

    #[tokio::test]
    async fn only_a_200_is_healthy() {
        assert!(
            probe(answering("HTTP/1.1 200 OK").await, "/readyz")
                .await
                .unwrap()
        );
        assert!(
            !probe(
                answering("HTTP/1.1 503 Service Unavailable").await,
                "/readyz"
            )
            .await
            .unwrap()
        );
    }

    #[tokio::test]
    async fn nobody_listening_is_an_error() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        assert!(probe(addr, "/readyz").await.is_err());
    }
}
