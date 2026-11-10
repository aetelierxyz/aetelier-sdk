use std::fmt;
use std::time::Duration;

use yellowstone_grpc_proto::tonic::{Code, Status};

use super::PROTOCOL_REVISION;
use crate::clients::disconnect::DisconnectReason;

pub const RATE_LIMIT_COOL_DOWN: Duration = Duration::from_secs(10);
pub const RATE_LIMIT_COOL_DOWN_MAX: Duration = Duration::from_secs(300);

const TOO_MANY_REQUESTS: &str = "HTTP status code 429";
const RETRY_AFTER: &str = "retry-after";

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ServerVersion {
    pub package: Option<String>,
    pub version: Option<String>,
    pub proto: Option<String>,
}

impl ServerVersion {
    pub fn parse(raw: &str) -> Self {
        let Ok(json) = serde_json::from_str::<serde_json::Value>(raw) else {
            return Self::default();
        };
        let info = json
            .get("version")
            .filter(|nested| nested.is_object())
            .unwrap_or(&json);
        let field = |key: &str| {
            info.get(key)
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        };
        Self {
            package: field("package"),
            version: field("version"),
            proto: field("proto"),
        }
    }
}

impl fmt::Display for ServerVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (&self.package, &self.version) {
            (Some(package), Some(version)) => write!(f, "{package} {version}")?,
            (Some(name), None) | (None, Some(name)) => f.write_str(name)?,
            (None, None) => f.write_str("unknown server")?,
        }
        match &self.proto {
            Some(proto) => write!(f, " (proto {proto})"),
            None => Ok(()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PendingRequest {
    Subscribe,
    Update,
}

#[derive(Debug)]
pub enum GrpcExitReason {
    Setup(String),
    Connect(String),
    Status {
        status: Status,
        pending: Option<PendingRequest>,
    },
    Ended,
    Stale {
        silence: Duration,
    },
    RequestFailed(String),
}

impl fmt::Display for GrpcExitReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Setup(message) => write!(f, "gRPC setup failed: {message}"),
            Self::Connect(message) => write!(f, "gRPC connect failed: {message}"),
            Self::Status { status, pending } => {
                f.write_str(&status_text(status))?;
                match pending {
                    Some(PendingRequest::Subscribe) => {
                        f.write_str(" while the subscribe request was unacknowledged")
                    }
                    Some(PendingRequest::Update) => {
                        f.write_str(" while a filter update was unacknowledged")
                    }
                    None => Ok(()),
                }
            }
            Self::Ended => f.write_str("the server ended the stream with status Ok"),
            Self::Stale { silence } => {
                write!(f, "stale stream (no block meta for {silence:?})")
            }
            Self::RequestFailed(message) => write!(f, "request write failed: {message}"),
        }
    }
}

impl GrpcExitReason {
    pub fn classify(self, server: &ServerVersion) -> ExitClass {
        match self {
            Self::Status { status, pending } => match classify_status(&status, pending) {
                StatusClass::Retry => retry_transport(status_text(&status), None),
                StatusClass::CoolDown(wait) => {
                    retry_transport(status_text(&status), Some(wait))
                }
                StatusClass::Reject => ExitClass::Reject {
                    reason: reject_text(&status, server),
                },
                StatusClass::RejectUpdate => ExitClass::RejectUpdate {
                    reason: format!(
                        "filter update rejected: {}",
                        reject_text(&status, server)
                    ),
                    others: DisconnectReason::GoingAway {
                        reason: "filter update rejected; resubscribing with the last \
                                 accepted filter"
                            .to_string(),
                    },
                },
            },
            Self::Ended => ExitClass::Retry {
                reason: DisconnectReason::GoingAway {
                    reason: format!("server {server} ended the stream with status Ok"),
                },
                cool_down: None,
            },
            Self::Stale { silence } => ExitClass::Retry {
                reason: DisconnectReason::StaleConnection {
                    silence_duration: silence,
                },
                cool_down: None,
            },
            Self::RequestFailed(message) => ExitClass::Retry {
                reason: DisconnectReason::HeartbeatFailed {
                    source: message.into(),
                },
                cool_down: None,
            },
            Self::Connect(message) => retry_transport(message, None),
            Self::Setup(message) => ExitClass::Reject { reason: message },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusClass {
    Retry,
    CoolDown(Duration),
    Reject,
    RejectUpdate,
}

#[derive(Debug)]
pub enum ExitClass {
    Retry {
        reason: DisconnectReason,
        cool_down: Option<Duration>,
    },
    Reject {
        reason: String,
    },
    RejectUpdate {
        reason: String,
        others: DisconnectReason,
    },
}

pub fn classify_status(status: &Status, pending: Option<PendingRequest>) -> StatusClass {
    match status.code() {
        Code::Cancelled
        | Code::Unknown
        | Code::DeadlineExceeded
        | Code::Aborted
        | Code::Internal
        | Code::DataLoss => StatusClass::Retry,
        Code::Unavailable if !status.message().ends_with(TOO_MANY_REQUESTS) => {
            StatusClass::Retry
        }
        Code::ResourceExhausted | Code::Unavailable => {
            StatusClass::CoolDown(cool_down(status))
        }
        Code::InvalidArgument if pending == Some(PendingRequest::Update) => {
            StatusClass::RejectUpdate
        }
        Code::Unauthenticated
        | Code::PermissionDenied
        | Code::NotFound
        | Code::AlreadyExists
        | Code::FailedPrecondition
        | Code::OutOfRange
        | Code::Unimplemented
        | Code::InvalidArgument => StatusClass::Reject,
        _ => StatusClass::Retry,
    }
}

pub fn rejection(reason: &str) -> DisconnectReason {
    DisconnectReason::ProtocolRejection {
        code: 0,
        reason: reason.to_string(),
    }
}

fn cool_down(status: &Status) -> Duration {
    status
        .metadata()
        .get(RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map_or(RATE_LIMIT_COOL_DOWN, |secs| {
            Duration::from_secs(secs)
                .clamp(RATE_LIMIT_COOL_DOWN, RATE_LIMIT_COOL_DOWN_MAX)
        })
}

pub(crate) fn with_causes(
    mut text: String,
    mut source: Option<&(dyn std::error::Error + 'static)>,
) -> String {
    while let Some(cause) = source {
        let message = cause.to_string();
        if !text.ends_with(&message) {
            text = format!("{text}: {message}");
        }
        source = cause.source();
    }
    text
}

fn status_text(status: &Status) -> String {
    with_causes(
        format!("gRPC {:?}: {}", status.code(), status.message()),
        std::error::Error::source(status),
    )
}

fn reject_text(status: &Status, server: &ServerVersion) -> String {
    let hint = match status.code() {
        Code::OutOfRange => "; raise [collect.grpc] max_decoding_message_size_mib",
        Code::Unauthenticated | Code::PermissionDenied => {
            "; check the token named by [collect.grpc] x_token_env"
        }
        Code::InvalidArgument => "; the provider refused the subscription filter",
        _ => "",
    };
    format!(
        "{} ({PROTOCOL_REVISION}, server {server}){hint}",
        status_text(status)
    )
}

fn retry_transport(message: String, cool_down: Option<Duration>) -> ExitClass {
    ExitClass::Retry {
        reason: DisconnectReason::TransportError {
            source: message.into(),
        },
        cool_down,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::clients::reconnect::{ReconnectAction, ReconnectPolicy};
    use yellowstone_grpc_proto::tonic::metadata::MetadataValue;

    const HTTP_429: &str = "grpc-status header missing, mapped from HTTP status code 429";
    const HTTP_503: &str = "grpc-status header missing, mapped from HTTP status code 503";
    const MISSING_TRAILER: &str = "protocol error: missing grpc-status trailer, stream was \
                                   terminated without a final status (possible truncation \
                                   by a proxy or load balancer)";
    const OVERSIZED: &str = "Error, decoded message length too large: found 67108865 \
                             bytes, the limit is: 67108864 bytes";
    const SERVER: &str = "server yellowstone-grpc-geyser 10.0.0 (proto 9.0.0)";

    const RETRYABLE: [Code; 7] = [
        Code::Cancelled,
        Code::Unknown,
        Code::DeadlineExceeded,
        Code::Aborted,
        Code::Internal,
        Code::Unavailable,
        Code::DataLoss,
    ];

    const TERMINAL: [Code; 8] = [
        Code::Unauthenticated,
        Code::PermissionDenied,
        Code::NotFound,
        Code::AlreadyExists,
        Code::FailedPrecondition,
        Code::OutOfRange,
        Code::Unimplemented,
        Code::InvalidArgument,
    ];

    const PENDING: [Option<PendingRequest>; 3] = [
        None,
        Some(PendingRequest::Subscribe),
        Some(PendingRequest::Update),
    ];

    fn policy() -> ReconnectPolicy {
        ReconnectPolicy::builder()
            .initial_delay(Duration::from_millis(10))
            .max_delay(Duration::from_millis(100))
            .jitter_factor(0.0)
            .build()
    }

    fn after_data() -> ReconnectPolicy {
        let mut policy = policy();
        policy.on_connected();
        policy.on_message_received();
        policy
    }

    fn server() -> ServerVersion {
        ServerVersion {
            package: Some("yellowstone-grpc-geyser".to_string()),
            version: Some("10.0.0".to_string()),
            proto: Some("9.0.0".to_string()),
        }
    }

    fn exit(
        code: Code,
        message: &str,
        pending: Option<PendingRequest>,
    ) -> GrpcExitReason {
        GrpcExitReason::Status {
            status: Status::new(code, message),
            pending,
        }
    }

    fn with_retry_after(code: Code, message: &str, retry_after: &'static str) -> Status {
        let mut status = Status::new(code, message);
        status
            .metadata_mut()
            .insert(RETRY_AFTER, MetadataValue::from_static(retry_after));
        status
    }

    fn retried(class: ExitClass) -> (DisconnectReason, Option<Duration>) {
        match class {
            ExitClass::Retry { reason, cool_down } => (reason, cool_down),
            other => panic!("expected a retry, got {other:?}"),
        }
    }

    fn rejected(class: ExitClass) -> String {
        match class {
            ExitClass::Reject { reason } => reason,
            other => panic!("expected a rejection, got {other:?}"),
        }
    }

    fn backoff(policy: &mut ReconnectPolicy, reason: &DisconnectReason) -> Duration {
        match policy.next_action(reason) {
            ReconnectAction::RetryAfter(delay) => delay,
            other => panic!("expected a backoff for {reason}, got {other:?}"),
        }
    }

    fn gives_up(policy: &mut ReconnectPolicy, reason: &DisconnectReason) -> bool {
        matches!(policy.next_action(reason), ReconnectAction::GiveUp { .. })
    }

    #[derive(Debug)]
    struct Cause {
        text: &'static str,
        source: Option<Box<Cause>>,
    }

    impl Cause {
        fn chain(texts: &[&'static str]) -> Self {
            let (last, outer) = texts.split_last().expect("at least one cause");
            outer.iter().rev().fold(
                Self {
                    text: last,
                    source: None,
                },
                |source, text| Self {
                    text,
                    source: Some(Box::new(source)),
                },
            )
        }
    }

    impl fmt::Display for Cause {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(self.text)
        }
    }

    impl std::error::Error for Cause {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(self.source.as_deref()?)
        }
    }

    #[test]
    fn retryable_codes_reconnect_with_transport_backoff() {
        for code in RETRYABLE {
            let message = match code {
                Code::Unavailable => HTTP_503,
                Code::Unknown => MISSING_TRAILER,
                _ => "transient",
            };
            for pending in PENDING {
                let status = Status::new(code, message);
                assert_eq!(classify_status(&status, pending), StatusClass::Retry);
                let (reason, cool_down) =
                    retried(exit(code, message, pending).classify(&server()));
                assert_eq!(cool_down, None, "{code:?}");
                assert!(
                    matches!(reason, DisconnectReason::TransportError { .. }),
                    "{reason}"
                );
                assert_eq!(
                    reason.to_string(),
                    format!("transport error: gRPC {code:?}: {message}")
                );
                assert_eq!(backoff(&mut policy(), &reason), Duration::from_millis(20));
            }
        }
    }

    #[test]
    fn statuses_built_from_transport_failures_name_each_distinct_cause_once() {
        let subscribe = Status::from_error(Box::new(Cause::chain(&[
            "transport error",
            "connection error",
            "stream closed because of a broken pipe",
        ])));
        assert_eq!(
            (subscribe.code(), subscribe.message()),
            (Code::Unknown, "transport error")
        );
        let subscribing = GrpcExitReason::Status {
            status: subscribe,
            pending: Some(PendingRequest::Subscribe),
        };
        let causes = "gRPC Unknown: transport error: connection error: stream closed \
                      because of a broken pipe";
        assert_eq!(
            subscribing.to_string(),
            format!("{causes} while the subscribe request was unacknowledged")
        );
        let (reason, cool_down) = retried(subscribing.classify(&server()));
        assert_eq!(cool_down, None);
        assert_eq!(reason.to_string(), format!("transport error: {causes}"));

        let mut reset = Status::new(
            Code::Unknown,
            "h2 protocol error: error reading a body from connection",
        );
        reset.set_source(Arc::new(Cause::chain(&[
            "error reading a body from connection",
            "stream closed because of a broken pipe",
        ])));
        let (reason, _) = retried(
            GrpcExitReason::Status {
                status: reset,
                pending: None,
            }
            .classify(&server()),
        );
        assert_eq!(
            reason.to_string(),
            "transport error: gRPC Unknown: h2 protocol error: error reading a body from \
             connection: stream closed because of a broken pipe"
        );
        assert_eq!(backoff(&mut policy(), &reason), Duration::from_millis(20));

        let mut insecure =
            Status::new(Code::PermissionDenied, "h2 protocol error: http2 error");
        insecure.set_source(Arc::new(Cause::chain(&[
            "http2 error",
            "connection error received: inadequate security",
        ])));
        let reason = rejected(
            GrpcExitReason::Status {
                status: insecure,
                pending: None,
            }
            .classify(&server()),
        );
        assert_eq!(
            reason,
            format!(
                "gRPC PermissionDenied: h2 protocol error: http2 error: connection error \
                 received: inadequate security ({PROTOCOL_REVISION}, {SERVER}); check the \
                 token named by [collect.grpc] x_token_env"
            )
        );
    }

    #[test]
    fn resource_exhaustion_and_http_429_cool_down_for_at_least_ten_seconds() {
        assert_eq!(RATE_LIMIT_COOL_DOWN, Duration::from_secs(10));
        for (code, message) in [
            (Code::ResourceExhausted, "too many subscriptions"),
            (Code::Unavailable, HTTP_429),
        ] {
            for pending in PENDING {
                let status = Status::new(code, message);
                assert_eq!(
                    classify_status(&status, pending),
                    StatusClass::CoolDown(Duration::from_secs(10)),
                    "{code:?}"
                );
            }
            let (reason, cool_down) =
                retried(exit(code, message, None).classify(&server()));
            assert_eq!(cool_down, Some(Duration::from_secs(10)));
            assert_eq!(
                reason.to_string(),
                format!("transport error: gRPC {code:?}: {message}")
            );
            assert_eq!(backoff(&mut policy(), &reason), Duration::from_millis(20));
        }
    }

    #[test]
    fn retry_after_is_honoured_above_ten_seconds_and_capped() {
        assert_eq!(RATE_LIMIT_COOL_DOWN_MAX, Duration::from_secs(300));
        for (header, secs) in [("30", 30), ("3", 10), ("86400", 300), ("soon", 10)] {
            for (code, message) in [
                (Code::ResourceExhausted, "slow down"),
                (Code::Unavailable, HTTP_429),
            ] {
                let status = with_retry_after(code, message, header);
                let wait = Duration::from_secs(secs);
                assert_eq!(
                    classify_status(&status, None),
                    StatusClass::CoolDown(wait),
                    "{header}"
                );
                let exit = GrpcExitReason::Status {
                    status,
                    pending: None,
                };
                assert_eq!(retried(exit.classify(&server())).1, Some(wait), "{header}");
            }
        }
        let unavailable = with_retry_after(Code::Unavailable, HTTP_503, "30");
        assert_eq!(classify_status(&unavailable, None), StatusClass::Retry);
    }

    #[test]
    fn terminal_codes_reject_with_code_zero_naming_the_code_revision_and_server() {
        for code in TERMINAL {
            for pending in [None, Some(PendingRequest::Subscribe)] {
                let mut status = Status::new(code, "refused");
                status
                    .metadata_mut()
                    .insert("x-token", MetadataValue::from_static("secret-token"));
                assert_eq!(classify_status(&status, pending), StatusClass::Reject);
                let reason = rejected(
                    GrpcExitReason::Status { status, pending }.classify(&server()),
                );
                assert!(
                    reason.starts_with(&format!(
                        "gRPC {code:?}: refused ({PROTOCOL_REVISION}, {SERVER})"
                    )),
                    "{reason}"
                );
                assert!(!reason.contains("secret-token"), "{reason}");
                let disconnect = rejection(&reason);
                assert!(matches!(
                    &disconnect,
                    DisconnectReason::ProtocolRejection { code: 0, reason: r } if *r == reason
                ));
                assert!(!disconnect.is_retryable());
                assert!(gives_up(&mut policy(), &disconnect), "{code:?}");
            }
        }
        assert_eq!(PROTOCOL_REVISION, "yellowstone-grpc-proto@14.0.1");
        assert_eq!(
            rejected(
                exit(Code::PermissionDenied, "invalid x-token", None).classify(&server())
            ),
            "gRPC PermissionDenied: invalid x-token (yellowstone-grpc-proto@14.0.1, server \
             yellowstone-grpc-geyser 10.0.0 (proto 9.0.0)); check the token named by \
             [collect.grpc] x_token_env"
        );
        let unauthenticated =
            rejected(exit(Code::Unauthenticated, "no token", None).classify(&server()));
        assert!(
            unauthenticated
                .ends_with("; check the token named by [collect.grpc] x_token_env")
        );
        assert_eq!(
            rejected(
                exit(Code::Unimplemented, "unknown method", None)
                    .classify(&ServerVersion::default())
            ),
            "gRPC Unimplemented: unknown method (yellowstone-grpc-proto@14.0.1, server \
             unknown server)"
        );
    }

    #[test]
    fn terminal_statuses_stay_terminal_after_data() {
        for code in TERMINAL {
            let reason = rejected(exit(code, "refused", None).classify(&server()));
            let mut policy = after_data();
            assert!(policy.data_ever_received());
            assert!(gives_up(&mut policy, &rejection(&reason)), "{code:?}");
        }
    }

    #[test]
    fn retryable_statuses_retry_after_data() {
        for code in RETRYABLE {
            let (reason, _) = retried(exit(code, "transient", None).classify(&server()));
            assert_eq!(
                backoff(&mut after_data(), &reason),
                Duration::from_millis(20),
                "{code:?}"
            );
        }
        let (throttled, cool_down) =
            retried(exit(Code::ResourceExhausted, "slow down", None).classify(&server()));
        assert_eq!(cool_down, Some(RATE_LIMIT_COOL_DOWN));
        assert_eq!(
            backoff(&mut after_data(), &throttled),
            Duration::from_millis(20)
        );
    }

    #[test]
    fn a_nonzero_rejection_code_would_be_retried_after_data() {
        let coded = DisconnectReason::ProtocolRejection {
            code: 7,
            reason: "gRPC PermissionDenied: refused".to_string(),
        };
        assert!(gives_up(&mut policy(), &coded));
        assert!(matches!(
            after_data().next_action(&coded),
            ReconnectAction::RetryImmediately
        ));
        assert!(gives_up(
            &mut after_data(),
            &rejection("gRPC PermissionDenied: refused")
        ));
    }

    #[test]
    fn invalid_argument_while_an_update_is_pending_fails_only_the_attaching_set() {
        let message = "account_include exceeds 50 addresses";
        let status = Status::new(Code::InvalidArgument, message);
        assert_eq!(
            classify_status(&status, Some(PendingRequest::Update)),
            StatusClass::RejectUpdate
        );
        let ExitClass::RejectUpdate { reason, others } =
            exit(Code::InvalidArgument, message, Some(PendingRequest::Update))
                .classify(&server())
        else {
            panic!("expected a rejected update");
        };
        assert_eq!(
            reason,
            format!(
                "filter update rejected: gRPC InvalidArgument: {message} \
                 ({PROTOCOL_REVISION}, {SERVER}); the provider refused the subscription \
                 filter"
            )
        );
        assert!(matches!(
            &others,
            DisconnectReason::GoingAway { reason }
                if reason == "filter update rejected; resubscribing with the last accepted filter"
        ));
        assert!(others.is_retryable());
        assert_eq!(
            backoff(&mut after_data(), &others),
            Duration::from_millis(10)
        );
        assert!(gives_up(&mut after_data(), &rejection(&reason)));
    }

    #[test]
    fn invalid_argument_on_the_opening_request_is_terminal() {
        for pending in [None, Some(PendingRequest::Subscribe)] {
            let status = Status::new(Code::InvalidArgument, "bad filter");
            assert_eq!(classify_status(&status, pending), StatusClass::Reject);
            let reason = rejected(
                exit(Code::InvalidArgument, "bad filter", pending).classify(&server()),
            );
            assert_eq!(
                reason,
                format!(
                    "gRPC InvalidArgument: bad filter ({PROTOCOL_REVISION}, {SERVER}); the \
                     provider refused the subscription filter"
                )
            );
            assert!(gives_up(&mut after_data(), &rejection(&reason)));
        }
    }

    #[test]
    fn the_default_arm_retries_as_a_transport_error() {
        let ok = Status::new(Code::Ok, "");
        for pending in PENDING {
            assert_eq!(classify_status(&ok, pending), StatusClass::Retry);
        }
        let exit = GrpcExitReason::Status {
            status: ok,
            pending: None,
        };
        let (reason, cool_down) = retried(exit.classify(&server()));
        assert_eq!(cool_down, None);
        assert_eq!(reason.to_string(), "transport error: gRPC Ok: ");
        assert_eq!(backoff(&mut policy(), &reason), Duration::from_millis(20));
    }

    #[test]
    fn an_oversized_message_rejects_with_a_size_hint() {
        let status = Status::new(Code::OutOfRange, OVERSIZED);
        assert_eq!(classify_status(&status, None), StatusClass::Reject);
        let reason =
            rejected(exit(Code::OutOfRange, OVERSIZED, None).classify(&server()));
        assert_eq!(
            reason,
            format!(
                "gRPC OutOfRange: {OVERSIZED} ({PROTOCOL_REVISION}, {SERVER}); raise \
                 [collect.grpc] max_decoding_message_size_mib"
            )
        );
        assert!(gives_up(&mut after_data(), &rejection(&reason)));
    }

    #[test]
    fn stream_end_staleness_request_failures_and_connect_errors_map_to_their_rows() {
        let ended = GrpcExitReason::Ended;
        assert_eq!(
            ended.to_string(),
            "the server ended the stream with status Ok"
        );
        let (reason, cool_down) = retried(ended.classify(&server()));
        assert_eq!(cool_down, None);
        assert!(matches!(
            &reason,
            DisconnectReason::GoingAway { reason }
                if *reason == format!("{SERVER} ended the stream with status Ok")
        ));
        assert_eq!(backoff(&mut policy(), &reason), Duration::from_millis(10));

        let silence = Duration::from_secs(10);
        let stale = GrpcExitReason::Stale { silence };
        assert_eq!(stale.to_string(), "stale stream (no block meta for 10s)");
        let (reason, cool_down) = retried(stale.classify(&server()));
        assert_eq!(cool_down, None);
        assert!(matches!(
            reason,
            DisconnectReason::StaleConnection { silence_duration } if silence_duration == silence
        ));
        assert_eq!(backoff(&mut policy(), &reason), Duration::from_millis(10));

        let failed = GrpcExitReason::RequestFailed("request channel full".to_string());
        assert_eq!(
            failed.to_string(),
            "request write failed: request channel full"
        );
        let (reason, cool_down) = retried(failed.classify(&server()));
        assert_eq!(cool_down, None);
        assert_eq!(
            reason.to_string(),
            "heartbeat write failed: request channel full"
        );
        assert!(matches!(reason, DisconnectReason::HeartbeatFailed { .. }));
        assert_eq!(backoff(&mut policy(), &reason), Duration::from_millis(10));

        let refused = GrpcExitReason::Connect("dns error: no such host".to_string());
        assert_eq!(
            refused.to_string(),
            "gRPC connect failed: dns error: no such host"
        );
        let (reason, cool_down) = retried(refused.classify(&server()));
        assert_eq!(cool_down, None);
        assert_eq!(
            reason.to_string(),
            "transport error: dns error: no such host"
        );
        assert_eq!(backoff(&mut policy(), &reason), Duration::from_millis(20));
    }

    #[test]
    fn setup_errors_are_terminal() {
        let setup = GrpcExitReason::Setup("invalid uri character".to_string());
        assert_eq!(
            setup.to_string(),
            "gRPC setup failed: invalid uri character"
        );
        let reason = rejected(setup.classify(&server()));
        assert_eq!(reason, "invalid uri character");
        assert!(gives_up(&mut policy(), &rejection(&reason)));
        assert!(gives_up(&mut after_data(), &rejection(&reason)));
    }

    #[test]
    fn server_versions_parse_nested_and_flat_json() {
        let nested = ServerVersion::parse(
            r#"{"version":{"package":"yellowstone-grpc-geyser","version":"10.0.0","proto":"9.0.0","solana":"2.3.0"},"extra":{"hostname":"geyser-1"}}"#,
        );
        assert_eq!(nested, server());
        assert_eq!(
            nested.to_string(),
            "yellowstone-grpc-geyser 10.0.0 (proto 9.0.0)"
        );
        let flat = ServerVersion::parse(r#"{"package":"richat","version":"5.1.0"}"#);
        assert_eq!(
            flat,
            ServerVersion {
                package: Some("richat".to_string()),
                version: Some("5.1.0".to_string()),
                proto: None,
            }
        );
        assert_eq!(flat.to_string(), "richat 5.1.0");
        let proto_only = ServerVersion::parse(r#"{"proto":"9.0.0"}"#);
        assert_eq!(proto_only.to_string(), "unknown server (proto 9.0.0)");
        let package_only = ServerVersion::parse(r#"{"version":{"package":"richat"}}"#);
        assert_eq!(package_only.to_string(), "richat");
        for garbage in [
            "",
            "not json",
            "[]",
            "42",
            r#"{"version":3}"#,
            r#"{"version":{}}"#,
        ] {
            assert_eq!(
                ServerVersion::parse(garbage),
                ServerVersion::default(),
                "{garbage}"
            );
        }
        assert_eq!(ServerVersion::default().to_string(), "unknown server");
    }
}
