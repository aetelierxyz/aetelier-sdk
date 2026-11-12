use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aetelier_connect::config::workers::market_worker_config::GrpcSection;
use aetelier_connect::framework::grpc::{
    GrpcSettings, StreamFilter, TRANSACTIONS_FILTER, XToken, ping_request,
};
use aetelier_connect::framework::onchain::WatchedAddress;
use aetelier_types::config::markets::market_config::TransactionsConfig;
use aetelier_types::onchain::{ChainAddress, Commitment};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use clap::Parser;
use futures::channel::mpsc;
use tokio::time::{Instant, sleep_until};
use yellowstone_grpc_client::{GeyserGrpcBuilder, InterceptorXToken};
use yellowstone_grpc_proto::prelude::{
    SubscribeRequest, SubscribeUpdate, subscribe_update::UpdateOneof,
};
use yellowstone_grpc_proto::prost::Message as _;
use yellowstone_grpc_proto::prost::bytes::{Buf, Bytes};
use yellowstone_grpc_proto::tonic::client::Grpc;
use yellowstone_grpc_proto::tonic::codec::{
    Codec, CompressionEncoding, DecodeBuf, Decoder, EncodeBuf, Encoder,
};
use yellowstone_grpc_proto::tonic::codegen::http::uri::PathAndQuery;
use yellowstone_grpc_proto::tonic::service::interceptor::InterceptedService;
use yellowstone_grpc_proto::tonic::{Request, Status};

const SUBSCRIBE_PATH: &str = "/geyser.Geyser/Subscribe";
const REQUEST_BUFFER: usize = 16;

#[derive(Parser, Debug)]
#[command(name = "capture_grpc")]
struct Cli {
    #[arg(long, help = "Yellowstone endpoint, http:// or https://")]
    endpoint: String,
    #[arg(
        long,
        default_value = "GRPC_X_TOKEN",
        help = "variable holding the x-token"
    )]
    x_token_env: String,
    #[arg(long, default_value = "finalized", help = "confirmed or finalized")]
    commitment: String,
    #[arg(long, help = "capture length in seconds")]
    seconds: u64,
    #[arg(long, help = "resubscribe at tip once after this many seconds")]
    reconnect_after: Option<u64>,
    #[arg(long, help = "keep vote transactions")]
    votes: bool,
    #[arg(long, help = "output .jsonl")]
    out: PathBuf,
    #[arg(help = "watched base58 addresses; none captures every transaction")]
    addresses: Vec<String>,
}

struct PassThrough;

struct RequestEncoder;

struct RawDecoder;

impl Codec for PassThrough {
    type Encode = SubscribeRequest;
    type Decode = Bytes;
    type Encoder = RequestEncoder;
    type Decoder = RawDecoder;

    fn encoder(&mut self) -> RequestEncoder {
        RequestEncoder
    }

    fn decoder(&mut self) -> RawDecoder {
        RawDecoder
    }
}

impl Encoder for RequestEncoder {
    type Item = SubscribeRequest;
    type Error = Status;

    fn encode(
        &mut self,
        item: SubscribeRequest,
        dst: &mut EncodeBuf<'_>,
    ) -> Result<(), Status> {
        item.encode(dst).map_err(|error| {
            Status::internal(format!("the subscribe request did not encode: {error}"))
        })
    }
}

impl Decoder for RawDecoder {
    type Item = Bytes;
    type Error = Status;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Bytes>, Status> {
        Ok(Some(src.copy_to_bytes(src.remaining())))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let commitment = cli
        .commitment
        .parse::<Commitment>()
        .map_err(anyhow::Error::msg)?;
    let addresses = cli
        .addresses
        .iter()
        .map(|address| ChainAddress::solana(address))
        .collect::<Result<Vec<_>, _>>()?;
    let section = GrpcSection {
        endpoint: cli.endpoint,
        x_token_env: Some(cli.x_token_env),
        commitment,
        server_filter: !addresses.is_empty(),
        accept_zstd: true,
        max_decoding_message_size_mib: 64,
        ping_interval_secs: 15,
        max_pending_slots: 32,
        max_addresses_per_connection: 50,
        account_include_max: None,
        account_include_reject: Vec::new(),
    };
    let watches = addresses
        .into_iter()
        .map(|address| WatchedAddress::new(address, &TransactionsConfig::default()))
        .collect::<Vec<_>>();
    let mut request = StreamFilter {
        include_failed: true,
        ..StreamFilter::new(&watches, &section)
    }
    .request();
    if cli.votes
        && let Some(transactions) = request.transactions.get_mut(TRANSACTIONS_FILTER)
    {
        transactions.vote = None;
    }
    let settings = GrpcSettings::new(
        &section,
        XToken::from_env(&section, |name| std::env::var(name).ok()),
    );
    let started = Instant::now();
    let deadline = started + Duration::from_secs(cli.seconds);
    let resubscribe_at = cli
        .reconnect_after
        .map(|secs| started + Duration::from_secs(secs))
        .filter(|&at| at < deadline);
    eprintln!(
        "capturing {} for {}s into {}",
        settings.endpoint_host(),
        cli.seconds,
        cli.out.display()
    );
    let mut out = BufWriter::new(File::create(&cli.out)?);
    let recorded = record(&settings, &request, resubscribe_at, deadline, &mut out).await;
    out.flush()?;
    let updates = recorded?;
    eprintln!("captured {updates} updates -> {}", cli.out.display());
    Ok(())
}

async fn record(
    settings: &GrpcSettings,
    request: &SubscribeRequest,
    resubscribe_at: Option<Instant>,
    deadline: Instant,
    out: &mut impl Write,
) -> anyhow::Result<u64> {
    let mut updates = 0;
    if let Some(at) = resubscribe_at {
        updates += capture(settings, request.clone(), at, out).await?;
        writeln!(out, r#"{{"t_us":{},"reconnect":true}}"#, now_us())?;
    }
    Ok(updates + capture(settings, request.clone(), deadline, out).await?)
}

async fn capture(
    settings: &GrpcSettings,
    request: SubscribeRequest,
    until: Instant,
    out: &mut impl Write,
) -> anyhow::Result<u64> {
    let GeyserGrpcBuilder {
        endpoint, x_token, ..
    } = settings
        .builder()
        .map_err(|reason| anyhow::anyhow!("{reason}"))?;
    let channel = endpoint.connect().await?;
    let interceptor = InterceptorXToken {
        x_token,
        x_request_snapshot: false,
    };
    let mut grpc = Grpc::new(InterceptedService::new(channel, interceptor))
        .max_decoding_message_size(settings.max_decoding_message_size);
    if settings.accept_zstd {
        grpc = grpc.accept_compressed(CompressionEncoding::Zstd);
    }
    grpc.ready().await?;
    let (mut requests, outbound) = mpsc::channel(REQUEST_BUFFER);
    requests.try_send(request)?;
    let mut stream = grpc
        .streaming(
            Request::new(outbound),
            PathAndQuery::from_static(SUBSCRIBE_PATH),
            PassThrough,
        )
        .await?
        .into_inner();
    let mut ping_id = 0;
    requests.try_send(next_ping(&mut ping_id))?;
    let mut ping_at = Instant::now() + settings.ping_interval;
    let mut updates = 0;
    loop {
        tokio::select! {
            biased;
            () = sleep_until(until) => return Ok(updates),
            () = sleep_until(ping_at) => {
                requests.try_send(next_ping(&mut ping_id))?;
                ping_at = Instant::now() + settings.ping_interval;
            }
            received = stream.message() => {
                let t_us = now_us();
                let Some(raw) = received? else {
                    anyhow::bail!("the server ended the stream with status Ok");
                };
                let server_ping = is_server_ping(raw.clone());
                writeln!(out, r#"{{"t_us":{t_us},"upd":"{}"}}"#, STANDARD.encode(&raw))?;
                updates += 1;
                if server_ping {
                    requests.try_send(next_ping(&mut ping_id))?;
                }
            }
        }
    }
}

fn next_ping(last_id: &mut i32) -> SubscribeRequest {
    *last_id = last_id.checked_add(1).unwrap_or(1);
    ping_request(*last_id)
}

fn is_server_ping(raw: Bytes) -> bool {
    matches!(
        SubscribeUpdate::decode(raw),
        Ok(SubscribeUpdate {
            update_oneof: Some(UpdateOneof::Ping(_)),
            ..
        })
    )
}

fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_micros() as u64)
        .unwrap_or(0)
}
