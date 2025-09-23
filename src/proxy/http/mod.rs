mod connector;
use self::connector::Connector;
use crate::common::net::{relay, relay_with_atomic_counter};
use crate::common::new_error;
use crate::config::{COUNTER_MAP, Router};
use crate::debug_log;
use crate::proxy::{Address, ChainStreamBuilder};
use bytes::Bytes;
use http::{StatusCode, header};
use http_body_util::{BodyExt, Empty, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::upgrade::Upgraded;
use hyper::{Method, Request, Response};
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioIo;
use log::info;
use std::collections::HashMap;
use std::io;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use tokio::net::TcpStream;

pub(crate) type BoxBody = http_body_util::combinators::BoxBody<Bytes, hyper::Error>;
pub(crate) type HttpClient = Client<Connector, BoxBody>;

#[derive(Clone)]
pub struct HttpInbound {
    inner_map: Arc<HashMap<String, ChainStreamBuilder>>,
    router: Arc<Router>,
    client: HttpClient,
    enable_api_server: bool,
    in_counter_up: Option<&'static AtomicU64>,
    in_counter_down: Option<&'static AtomicU64>,
    relay_buffer_size: usize,
}
impl HttpInbound {
    pub fn new(
        inner_map: Arc<HashMap<String, ChainStreamBuilder>>,
        router: Arc<Router>,
        enable_api_server: bool,
        in_counter_up: Option<&'static AtomicU64>,
        in_counter_down: Option<&'static AtomicU64>,
        relay_buffer_size: usize,
    ) -> Self {
        let client = Client::builder(hyper_util::rt::TokioExecutor::new())
            .http1_preserve_header_case(true)
            .build(Connector::new(inner_map.clone(), router.clone()));
        Self {
            client,
            router,
            enable_api_server,
            in_counter_up,
            in_counter_down,
            relay_buffer_size,
            inner_map,
        }
    }
    pub async fn serve_http_conn(&self, io: TcpStream) -> io::Result<()> {
        let io = TokioIo::new(io);

        let inner_map = self.inner_map.clone();
        let router = self.router.clone();
        let enable_api_server = self.enable_api_server;
        let in_counter_up = self.in_counter_up;
        let in_counter_down = self.in_counter_down;
        let relay_buffer_size = self.relay_buffer_size;
        let client = self.client.clone();

        let service = service_fn(move |req: Request<Incoming>| {
            let inner_map = inner_map.clone();
            let router = router.clone();
            let client = client.clone();
            async move {
                if Method::CONNECT == req.method() {
                    // Обработка CONNECT запроса
                    proxy_connect(
                        req,
                        inner_map,
                        router,
                        enable_api_server,
                        in_counter_up,
                        in_counter_down,
                        relay_buffer_size,
                    )
                    .await
                } else {
                    // Обычный HTTP прокси
                    proxy(req, client).await
                }
            }
        });

        let builder =
            hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new());

        let conn = builder.serve_connection_with_upgrades(io, service);

        if let Err(err) = conn.await {
            log::error!("Error serving connection: {:?}", err);
            return Err(new_error(err));
        }
        Ok(())
    }
}

#[inline]
fn empty_body() -> BoxBody {
    Empty::<Bytes>::new().map_err(|e| match e {}).boxed()
}

#[inline]
fn full_body<T: Into<Bytes>>(chunk: T) -> BoxBody {
    Full::new(chunk.into()).map_err(|e| match e {}).boxed()
}

#[inline]
fn internal_server_error() -> Response<BoxBody> {
    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .body(empty_body())
        .unwrap()
}

async fn proxy_connect(
    req: Request<Incoming>,
    inner_map: Arc<HashMap<String, ChainStreamBuilder>>,
    router: Arc<Router>,
    enable_api_server: bool,
    in_counter_up: Option<&'static AtomicU64>,
    in_counter_down: Option<&'static AtomicU64>,
    relay_buffer_size: usize,
) -> Result<Response<BoxBody>, hyper::Error> {
    if let Some(addr) = host_addr(req.uri()) {
        tokio::task::spawn(async move {
            let inner_map = inner_map;
            let router = router;
            match hyper::upgrade::on(req).await {
                Ok(upgraded) => {
                    if let Err(e) = tunnel(
                        upgraded,
                        addr,
                        inner_map,
                        router,
                        enable_api_server,
                        in_counter_up,
                        in_counter_down,
                        relay_buffer_size,
                    )
                    .await
                    {
                        log::error!("http tunnel error: {}", e);
                    };
                }
                Err(e) => log::error!("upgrade error: {}", e),
            }
        });

        Ok(Response::new(empty_body()))
    } else {
        log::error!("CONNECT host is not socket addr: {:?}", req.uri());
        let mut resp = Response::new(full_body("CONNECT must be to a socket address"));
        *resp.status_mut() = http::StatusCode::BAD_REQUEST;

        Ok(resp)
    }
}
async fn proxy(
    mut req: Request<Incoming>,
    client: HttpClient,
) -> Result<Response<BoxBody>, hyper::Error> {
    remove_proxy_headers(&mut req);
    debug_log!("http proxy server req: {:?}", req);

    let (parts, body) = req.into_parts();
    let new_body = body.boxed();

    let req = Request::from_parts(parts, new_body);

    let response = match client.request(req).await {
        Ok(resp) => {
            let (parts, body) = resp.into_parts();
            let new_body = body.boxed();
            Ok(Response::from_parts(parts, new_body))
        }
        Err(e) => {
            log::error!("Client request error: {}", e);
            Ok(internal_server_error())
        }
    };

    response
}

fn host_addr(uri: &http::Uri) -> Option<Address> {
    uri.authority()
        .and_then(|auth| Address::from_str(auth.as_str()).map(Some).unwrap_or(None))
}

// Create a TCP connection to host:port, build a tunnel between the connection and
// the upgraded connection
async fn tunnel(
    upgraded: Upgraded,
    addr: Address,
    inner_map: Arc<HashMap<String, ChainStreamBuilder>>,
    router: Arc<Router>,
    enable_api_server: bool,
    in_counter_up: Option<&'static AtomicU64>,
    in_counter_down: Option<&'static AtomicU64>,
    relay_buffer_size: usize,
) -> io::Result<()> {
    // Connect to remote server
    let ob = router.match_addr(&addr);
    let stream_builder = inner_map.get(ob).unwrap();
    info!("routing {} to outbound:{}", addr, ob);
    if stream_builder.is_blackhole() {
        return Ok(());
    }
    let upgraded = TokioIo::new(upgraded);
    let server = hyper_util::rt::tokio::WithHyperIo::new(stream_builder.build_tcp(addr).await?);
    if enable_api_server {
        let out_down = format!("outbound>>>{}>>>traffic>>>downlink", ob);
        let out_up = format!("outbound>>>{}>>>traffic>>>uplink", ob);
        let out_down = COUNTER_MAP.get().unwrap().get(out_down.as_str()).unwrap();
        let out_up = COUNTER_MAP.get().unwrap().get(out_up.as_str()).unwrap();
        relay_with_atomic_counter(
            upgraded,
            server,
            in_counter_up.unwrap(),
            in_counter_down.unwrap(),
            out_up,
            out_down,
            relay_buffer_size,
        )
        .await?;
    } else {
        relay(upgraded, server, relay_buffer_size).await?;
    }
    Ok(())
}

pub fn remove_proxy_headers<T>(req: &mut Request<T>) {
    // Remove headers that shouldn't be forwarded to upstream
    req.headers_mut().remove(header::ACCEPT_ENCODING);
    req.headers_mut().remove(header::CONNECTION);
    req.headers_mut().remove("proxy-connection");
    req.headers_mut().remove(header::PROXY_AUTHENTICATE);
    req.headers_mut().remove(header::PROXY_AUTHORIZATION);
}
