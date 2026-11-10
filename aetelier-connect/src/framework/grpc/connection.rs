use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use tokio::time::{Instant, MissedTickBehavior, interval_at, sleep_until, timeout};
use tracing::{error, info, warn};
use yellowstone_grpc_proto::prelude::{CommitmentLevel, SubscribeRequest};

use super::assemble::{SlotAssembler, bank_events};
use super::decode::{GrpcUpdate, decode_update};
use super::pool::{AttachId, ConnectionKey};
use super::request::{StreamFilter, commitment_level};
use super::session::{GeyserDial, GrpcSession, GrpcSettings, SessionEvent, XToken};
use super::status::{ExitClass, GrpcExitReason, ServerVersion, rejection};
use crate::clients::reconnect::{ReconnectAction, ReconnectPolicy};
use crate::config::workers::common::ReconnectSection;
use crate::config::workers::market_worker_config::GrpcSection;
use crate::framework::budget::{RateWindow, SourceMetrics};
use crate::framework::model::DomainEvent;
use crate::framework::onchain::{DecodedTransaction, RowContext, WatchedAddress};
use crate::framework::registry::TaskExit;
use crate::framework::slotbook::{
    CoverageStart, DEFAULT_REORDER_WINDOW_SLOTS, ReleasedBank, SlotBookConfig, SlotGap,
    SlotPosition,
};
use crate::workers::market_worker::framework_reconnect_policy;

pub const ATTACH_QUEUE_BANKS: usize = 256;
const OPEN_TIMEOUT: Duration = Duration::from_secs(15);
const TIP_POLL_TIMEOUT: Duration = Duration::from_secs(5);
const FANOUT_OVERFLOW: &str = "fan-out queue full";

pub(crate) struct NewAttach {
    pub(crate) id: AttachId,
    pub(crate) watch: WatchedAddress,
    pub(crate) metrics: SourceMetrics,
    pub(crate) events: mpsc::Sender<Vec<DomainEvent>>,
    pub(crate) exit: oneshot::Sender<TaskExit>,
}

pub(crate) enum Command {
    Attach(NewAttach),
    Detach(AttachId),
}

pub(crate) struct ConnectionContext {
    pub(crate) venue: &'static str,
    pub(crate) section: GrpcSection,
    pub(crate) x_token: Option<XToken>,
    pub(crate) reconnect: Option<ReconnectSection>,
    pub(crate) connect_rate: RateWindow,
}

#[derive(Clone, Copy)]
enum Coverage {
    None,
    Connection,
    ParentAfter(u64),
}

enum Offer {
    Delivered,
    Full,
    Gone,
}

struct AttachState {
    watch: WatchedAddress,
    metrics: SourceMetrics,
    events: mpsc::Sender<Vec<DomainEvent>>,
    exit: Option<oneshot::Sender<TaskExit>>,
    accepted: bool,
    carried: bool,
    coverage: Coverage,
    last_delivered: Option<SlotPosition>,
    outage: bool,
    owed_loss: Option<String>,
    owed_resume: bool,
    overflowing: bool,
    gone: bool,
}

impl AttachState {
    fn covers(&self, bank: &ReleasedBank<DecodedTransaction>) -> bool {
        match self.coverage {
            Coverage::None => false,
            Coverage::Connection => true,
            Coverage::ParentAfter(slot) => bank.parent_slot > slot,
        }
    }

    fn offer(&mut self, tail: Vec<DomainEvent>) -> Offer {
        let mut batch = Vec::with_capacity(tail.len() + 2);
        if let Some(reason) = &self.owed_loss {
            batch.push(DomainEvent::ConnectionLost {
                reason: reason.clone(),
            });
        }
        if self.owed_resume {
            batch.push(DomainEvent::ConnectionResumed);
        }
        batch.extend(tail);
        if batch.is_empty() {
            return Offer::Delivered;
        }
        match self.events.try_send(batch) {
            Ok(()) => {
                self.owed_loss = None;
                self.owed_resume = false;
                Offer::Delivered
            }
            Err(mpsc::error::TrySendError::Full(_)) => Offer::Full,
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.gone = true;
                Offer::Gone
            }
        }
    }
}

struct InFlight {
    ack: Option<i32>,
    attaching: Vec<AttachId>,
}

pub(crate) struct ConnectionCore {
    venue: &'static str,
    section: GrpcSection,
    assembler: SlotAssembler,
    attaches: BTreeMap<AttachId, AttachState>,
    accepted_any: bool,
    subscribed: bool,
    conn_epoch_us: u64,
    coverage_ack: Option<i32>,
    update: Option<InFlight>,
    dirty: bool,
    totals: SourceMetrics,
}

impl ConnectionCore {
    pub(crate) fn new(
        venue: &'static str,
        section: GrpcSection,
        totals: SourceMetrics,
    ) -> Self {
        let assembler = SlotAssembler::new(SlotBookConfig {
            max_pending_slots: section.max_pending_slots,
            reorder_window_slots: DEFAULT_REORDER_WINDOW_SLOTS,
        });
        Self {
            venue,
            section,
            assembler,
            attaches: BTreeMap::new(),
            accepted_any: false,
            subscribed: false,
            conn_epoch_us: 0,
            coverage_ack: None,
            update: None,
            dirty: false,
            totals,
        }
    }

    pub(crate) fn totals(&self) -> &SourceMetrics {
        &self.totals
    }

    pub(crate) fn add(&mut self, attach: NewAttach) {
        let NewAttach {
            id,
            watch,
            metrics,
            events,
            exit,
        } = attach;
        if self.subscribed {
            metrics.set_conn_epoch_us(self.conn_epoch_us);
        }
        self.attaches.insert(
            id,
            AttachState {
                watch,
                metrics,
                events,
                exit: Some(exit),
                accepted: false,
                carried: false,
                coverage: Coverage::None,
                last_delivered: None,
                outage: false,
                owed_loss: None,
                owed_resume: false,
                overflowing: false,
                gone: false,
            },
        );
    }

    pub(crate) fn remove(&mut self, id: AttachId) {
        let Some(attach) = self.attaches.remove(&id) else {
            return;
        };
        if attach.accepted || attach.carried {
            self.dirty = true;
        }
        if let Some(update) = &mut self.update {
            update.attaching.retain(|attaching| *attaching != id);
        }
        self.accepted_any = self.attaches.values().any(|attach| attach.accepted);
        info!(
            venue = self.venue,
            attach = ?id,
            address = %attach.watch.address,
            coverage_end_slot = self.assembler.book().highest_slot(),
            "framework.grpc.detached"
        );
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.attaches.is_empty()
    }

    pub(crate) fn opening_request(&mut self) -> Result<SubscribeRequest, String> {
        let everyone = !self.accepted_any;
        let filter = StreamFilter::new(
            self.attaches
                .values()
                .filter(|attach| everyone || attach.accepted)
                .map(|attach| &attach.watch),
            &self.section,
        );
        filter.admit(&self.section)?;
        for attach in self.attaches.values_mut() {
            attach.carried = everyone || attach.accepted;
        }
        self.dirty = false;
        Ok(filter.request())
    }

    pub(crate) fn on_subscribed(&mut self, conn_epoch_us: u64, opening_ack: i32) {
        self.subscribed = true;
        self.conn_epoch_us = conn_epoch_us;
        self.coverage_ack = Some(opening_ack);
        for attach in self.attaches.values_mut() {
            attach.metrics.set_conn_epoch_us(conn_epoch_us);
            if attach.outage && !attach.gone {
                attach.outage = false;
                attach.owed_resume = true;
                attach.offer(Vec::new());
            }
        }
    }

    pub(crate) fn on_update(&mut self, update: GrpcUpdate) {
        let watches: Vec<&WatchedAddress> =
            self.attaches.values().map(|attach| &attach.watch).collect();
        let counters = reporting(&self.attaches, &self.totals);
        let released = self.assembler.apply(update, &watches, &counters);
        self.fan_out(released);
    }

    pub(crate) fn on_acknowledged(&mut self, ack: i32, answered: bool) {
        let (acknowledged, coverage) = if self.coverage_ack == Some(ack) {
            self.coverage_ack = None;
            self.assembler.open(CoverageStart::AfterHighestSlot);
            let carried = self
                .attaches
                .iter()
                .filter(|(_, attach)| attach.carried)
                .map(|(id, _)| *id)
                .collect();
            (carried, Coverage::Connection)
        } else if let Some(update) = self.update.take_if(|update| update.ack == Some(ack))
        {
            let coverage = match self.assembler.book().highest_slot() {
                0 => Coverage::Connection,
                highest => Coverage::ParentAfter(highest),
            };
            (update.attaching, coverage)
        } else {
            return;
        };
        if !answered {
            warn!(
                venue = self.venue,
                acknowledgement = ack,
                "framework.grpc.coverage_without_pong"
            );
        }
        for id in acknowledged {
            if let Some(attach) = self.attaches.get_mut(&id) {
                attach.carried = false;
                attach.accepted = true;
                attach.coverage = coverage;
            }
        }
        self.accepted_any = self.attaches.values().any(|attach| attach.accepted);
    }

    pub(crate) fn next_update(&mut self) -> Option<SubscribeRequest> {
        while self.subscribed && self.coverage_ack.is_none() && self.update.is_none() {
            let attaching: Vec<AttachId> = self
                .attaches
                .iter()
                .filter(|(_, attach)| !attach.accepted && !attach.carried)
                .map(|(id, _)| *id)
                .collect();
            if attaching.is_empty() && !self.dirty {
                return None;
            }
            let filter = StreamFilter::new(
                self.attaches
                    .values()
                    .filter(|attach| attach.accepted || !attach.carried)
                    .map(|attach| &attach.watch),
                &self.section,
            );
            match filter.admit(&self.section) {
                Ok(()) => {
                    for id in &attaching {
                        if let Some(attach) = self.attaches.get_mut(id) {
                            attach.carried = true;
                        }
                    }
                    self.dirty = false;
                    self.update = Some(InFlight {
                        ack: None,
                        attaching,
                    });
                    return Some(filter.request());
                }
                Err(_) if attaching.is_empty() => {
                    self.dirty = false;
                    return None;
                }
                Err(reason) => {
                    for id in attaching {
                        self.resolve(id, &reason);
                    }
                }
            }
        }
        None
    }

    pub(crate) fn update_sent(&mut self, ack: i32) {
        if let Some(update) = &mut self.update {
            update.ack = Some(ack);
        }
    }

    pub(crate) fn set_tip(&mut self, tip_slot: u64) {
        let released = self.assembler.book().last_released();
        for attach in self.attaches.values().filter(|attach| !attach.gone) {
            if let Some(position) = attach.last_delivered.or(released) {
                attach
                    .metrics
                    .set_behind_tip_slots(tip_slot.saturating_sub(position.slot));
            }
        }
    }

    pub(crate) fn on_lost(&mut self, reason: &str) {
        self.flush();
        for attach in self.attaches.values_mut() {
            attach.coverage = Coverage::None;
            attach.carried = false;
            if attach.gone {
                continue;
            }
            attach.outage = true;
            attach.owed_resume = false;
            attach.owed_loss.get_or_insert_with(|| reason.to_string());
            attach.offer(Vec::new());
        }
        self.subscribed = false;
        self.coverage_ack = None;
        self.update = None;
    }

    pub(crate) fn reject_update(&mut self, reason: &str) {
        if let Some(update) = self.update.take() {
            for id in update.attaching {
                self.resolve(id, reason);
            }
        }
    }

    pub(crate) fn on_terminal(&mut self, reason: &str) {
        self.flush();
        let ids: Vec<AttachId> = self.attaches.keys().copied().collect();
        for id in ids {
            self.resolve(id, reason);
        }
        self.accepted_any = false;
        self.subscribed = false;
        self.coverage_ack = None;
        self.update = None;
        self.dirty = false;
    }

    fn apply(&mut self, command: Command) {
        match command {
            Command::Attach(attach) => self.add(attach),
            Command::Detach(id) => self.remove(id),
        }
    }

    fn resolve(&mut self, id: AttachId, reason: &str) {
        if let Some(exit) = self.attaches.remove(&id).and_then(|attach| attach.exit) {
            let _ = exit.send(TaskExit::Failed(rejection(reason)));
        }
    }

    fn flush(&mut self) {
        let counters = reporting(&self.attaches, &self.totals);
        let released = self.assembler.close(&counters);
        self.fan_out(released);
    }

    fn fan_out(&mut self, banks: Vec<ReleasedBank<DecodedTransaction>>) {
        let context = RowContext {
            exchange: self.venue,
            commitment: self.section.commitment,
        };
        for bank in &banks {
            let position = bank.position();
            for (id, attach) in &mut self.attaches {
                if attach.gone || !attach.covers(bank) {
                    continue;
                }
                let gap = attach
                    .last_delivered
                    .filter(|last| position.block_height > last.block_height + 1)
                    .map(|last| SlotGap::between(last, position));
                let watch = attach.watch;
                let events = bank_events(bank, gap, &[&watch], context);
                match attach.offer(events) {
                    Offer::Delivered => {
                        attach.last_delivered = Some(position);
                        attach.overflowing = false;
                    }
                    Offer::Full if !attach.overflowing => {
                        attach.overflowing = true;
                        attach.metrics.bump_fanout_overflow();
                        attach
                            .owed_loss
                            .get_or_insert_with(|| FANOUT_OVERFLOW.to_string());
                        attach.owed_resume = true;
                        warn!(
                            venue = self.venue,
                            attach = ?id,
                            address = %watch.address,
                            slot = bank.slot,
                            "framework.grpc.fanout_overflow"
                        );
                    }
                    Offer::Full | Offer::Gone => {}
                }
            }
        }
    }
}

fn reporting<'a>(
    attaches: &'a BTreeMap<AttachId, AttachState>,
    totals: &'a SourceMetrics,
) -> Vec<&'a SourceMetrics> {
    attaches
        .values()
        .find(|attach| !attach.gone)
        .map(|attach| &attach.metrics)
        .into_iter()
        .chain([totals])
        .collect()
}

pub(crate) struct ConnectLimiter {
    window: RateWindow,
    attempts: VecDeque<Instant>,
}

impl ConnectLimiter {
    pub(crate) fn new(window: RateWindow) -> Self {
        Self {
            window,
            attempts: VecDeque::new(),
        }
    }

    pub(crate) fn next_allowed(&mut self, now: Instant) -> Instant {
        self.forget(now);
        let max = self.window.max.max(1) as usize;
        match self.attempts.len().checked_sub(max) {
            Some(oldest) => self.attempts[oldest] + self.window.per,
            None => now,
        }
    }

    pub(crate) fn record(&mut self, at: Instant) {
        self.forget(at);
        self.attempts.push_back(at);
    }

    fn forget(&mut self, now: Instant) {
        while self
            .attempts
            .front()
            .is_some_and(|&attempt| attempt + self.window.per <= now)
        {
            self.attempts.pop_front();
        }
    }
}

#[derive(Default)]
pub(crate) struct ReconnectGate {
    held: Mutex<Option<HeldGate>>,
}

struct HeldGate {
    until: Instant,
    attempts: VecDeque<Instant>,
}

impl ReconnectGate {
    pub(crate) fn holds(&self, now: Instant) -> bool {
        self.held().as_ref().is_some_and(|held| held.until > now)
    }

    fn hold(&self, until: Instant, limiter: &ConnectLimiter) {
        *self.held() = Some(HeldGate {
            until,
            attempts: limiter.attempts.clone(),
        });
    }

    fn resume(&self, window: RateWindow, now: Instant) -> (Instant, ConnectLimiter) {
        let mut limiter = ConnectLimiter::new(window);
        match self.held().as_ref().filter(|held| held.until > now) {
            Some(held) => {
                limiter.attempts.clone_from(&held.attempts);
                (held.until, limiter)
            }
            None => (now, limiter),
        }
    }

    fn held(&self) -> MutexGuard<'_, Option<HeldGate>> {
        self.held.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

pub(crate) fn next_attempt_at(
    now: Instant,
    action: ReconnectAction,
    cool_down: Option<Duration>,
    limiter: &mut ConnectLimiter,
) -> Instant {
    let backoff = match action {
        ReconnectAction::RetryAfter(delay) => now + delay,
        ReconnectAction::RetryImmediately | ReconnectAction::GiveUp { .. } => now,
        ReconnectAction::CircuitOpen { until } => until,
    };
    backoff
        .max(now + cool_down.unwrap_or_default())
        .max(limiter.next_allowed(now))
}

pub(crate) struct SharedGrpcConnection {
    commands: mpsc::UnboundedSender<Command>,
}

impl SharedGrpcConnection {
    pub(crate) fn spawn(
        key: ConnectionKey,
        context: ConnectionContext,
        dial: Arc<dyn GeyserDial>,
        gate: Arc<ReconnectGate>,
        first: NewAttach,
    ) -> Self {
        let (commands, receiver) = mpsc::unbounded_channel();
        let mut core = ConnectionCore::new(
            context.venue,
            context.section.clone(),
            SourceMetrics::default(),
        );
        core.add(first);
        tokio::spawn(run(key, context, dial, gate, core, receiver));
        Self { commands }
    }

    pub(crate) fn send(&self, command: Command) -> Result<(), Command> {
        self.commands.send(command).map_err(|refused| refused.0)
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.commands.is_closed()
    }
}

async fn run(
    key: ConnectionKey,
    context: ConnectionContext,
    dial: Arc<dyn GeyserDial>,
    gate: Arc<ReconnectGate>,
    mut core: ConnectionCore,
    mut commands: mpsc::UnboundedReceiver<Command>,
) {
    let mut policy = framework_reconnect_policy(context.reconnect.as_ref());
    let (mut next_at, mut limiter) = gate.resume(context.connect_rate, Instant::now());
    let settings = GrpcSettings::new(&context.section, context.x_token.clone());
    let endpoint = settings.endpoint_host();
    let link = Link {
        key: &key,
        endpoint: &endpoint,
        settings: &settings,
        dial: dial.as_ref(),
        tip: commitment_level(context.section.commitment),
    };
    while wait_for_attempt(next_at, &mut core, &mut commands).await {
        limiter.record(Instant::now());
        let mut server = ServerVersion::default();
        let Some(exit) = link
            .attempt(&mut core, &mut commands, &mut policy, &mut server)
            .await
        else {
            continue;
        };
        let now = Instant::now();
        let (reason, action, cool_down) = match exit.classify(&server) {
            ExitClass::Retry { reason, cool_down } => {
                core.on_lost(&reason.to_string());
                let action = policy.next_action(&reason);
                (reason, action, cool_down)
            }
            ExitClass::RejectUpdate { reason, others } => {
                core.reject_update(&reason);
                core.on_lost(&others.to_string());
                let action = policy.next_action(&others);
                (others, action, None)
            }
            ExitClass::Reject { reason } => {
                error!(
                    endpoint = %endpoint,
                    shard = key.shard,
                    commitment = %key.commitment,
                    %reason,
                    "framework.grpc.terminal"
                );
                core.on_terminal(&reason);
                refuse_queued(&mut commands, &reason).await;
                break;
            }
        };
        next_at = next_attempt_at(now, action, cool_down, &mut limiter);
        gate.hold(next_at, &limiter);
        warn!(
            endpoint = %endpoint,
            shard = key.shard,
            %reason,
            delay_ms = next_at.saturating_duration_since(now).as_millis() as u64,
            "framework.grpc.reconnecting"
        );
    }
    info!(
        endpoint = %endpoint,
        shard = key.shard,
        commitment = %key.commitment,
        totals = ?core.totals().snapshot(),
        "framework.grpc.closed"
    );
}

struct Link<'a> {
    key: &'a ConnectionKey,
    endpoint: &'a str,
    settings: &'a GrpcSettings,
    dial: &'a dyn GeyserDial,
    tip: CommitmentLevel,
}

impl Link<'_> {
    async fn attempt(
        &self,
        core: &mut ConnectionCore,
        commands: &mut mpsc::UnboundedReceiver<Command>,
        policy: &mut ReconnectPolicy,
        server: &mut ServerVersion,
    ) -> Option<GrpcExitReason> {
        let channel = match timeout(OPEN_TIMEOUT, self.dial.dial(self.settings)).await {
            Ok(Ok(channel)) => channel,
            Ok(Err(exit)) => return Some(exit),
            Err(_) => return Some(open_timed_out()),
        };
        if !drain(core, commands) {
            return None;
        }
        let request = match core.opening_request() {
            Ok(request) => request,
            Err(reason) => return Some(GrpcExitReason::Setup(reason)),
        };
        let opened = timeout(
            OPEN_TIMEOUT,
            GrpcSession::open(channel, request, self.settings, core.totals()),
        )
        .await;
        let mut session = match opened {
            Ok(Ok(session)) => session,
            Ok(Err((exit, version))) => {
                *server = version;
                return Some(exit);
            }
            Err(_) => return Some(open_timed_out()),
        };
        server.clone_from(session.server());
        core.on_subscribed(session.conn_epoch_us(), session.opening_ack());
        info!(
            endpoint = self.endpoint,
            shard = self.key.shard,
            commitment = %self.key.commitment,
            token_env = self.key.token_env.as_deref(),
            conn_epoch_us = session.conn_epoch_us(),
            server = %session.server(),
            attaches = core.attaches.len(),
            "framework.grpc.subscribed"
        );
        self.serve(&mut session, core, commands, policy).await
    }

    async fn serve(
        &self,
        session: &mut GrpcSession,
        core: &mut ConnectionCore,
        commands: &mut mpsc::UnboundedReceiver<Command>,
        policy: &mut ReconnectPolicy,
    ) -> Option<GrpcExitReason> {
        let client = session.client().cloned();
        let every = self.settings.ping_interval;
        let mut tip_poll = interval_at(Instant::now() + every, every);
        tip_poll.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut tips: JoinSet<Option<u64>> = JoinSet::new();
        let mut connected = false;
        loop {
            if let Some(request) = core.next_update() {
                match session.update(request) {
                    Ok(ack) => core.update_sent(ack),
                    Err(exit) => return Some(exit),
                }
            }
            tokio::select! {
                biased;
                command = commands.recv() => core.apply(command?),
                event = session.next() => match event {
                    Ok(SessionEvent::Update { update, receipt }) => {
                        let update = decode_update(update, receipt);
                        let first_meta =
                            !connected && matches!(update, GrpcUpdate::BlockMeta(_));
                        core.on_update(update);
                        if first_meta {
                            connected = true;
                            policy.on_connected();
                            policy.on_message_received();
                        }
                    }
                    Ok(SessionEvent::Acknowledged { ack, answered, .. }) => {
                        core.on_acknowledged(ack, answered);
                    }
                    Err(exit) => return Some(exit),
                },
                Some(polled) = tips.join_next(), if !tips.is_empty() => {
                    if let Ok(Some(tip_slot)) = polled {
                        core.set_tip(tip_slot);
                    }
                }
                _ = tip_poll.tick(), if client.is_some() => {
                    if let Some(client) = client.as_ref().filter(|_| tips.is_empty()) {
                        let mut client = client.clone();
                        let level = self.tip;
                        tips.spawn(async move {
                            let polled =
                                timeout(TIP_POLL_TIMEOUT, client.get_slot(Some(level))).await;
                            polled.ok()?.ok().map(|response| response.slot)
                        });
                    }
                }
            }
        }
    }
}

async fn wait_for_attempt(
    at: Instant,
    core: &mut ConnectionCore,
    commands: &mut mpsc::UnboundedReceiver<Command>,
) -> bool {
    loop {
        tokio::select! {
            biased;
            command = commands.recv() => match command {
                Some(command) => core.apply(command),
                None => return false,
            },
            () = sleep_until(at), if !core.is_empty() => return true,
        }
    }
}

fn drain(
    core: &mut ConnectionCore,
    commands: &mut mpsc::UnboundedReceiver<Command>,
) -> bool {
    while let Ok(command) = commands.try_recv() {
        core.apply(command);
    }
    !core.is_empty()
}

async fn refuse_queued(commands: &mut mpsc::UnboundedReceiver<Command>, reason: &str) {
    commands.close();
    while let Some(command) = commands.recv().await {
        if let Command::Attach(attach) = command {
            let _ = attach.exit.send(TaskExit::Failed(rejection(reason)));
        }
    }
}

fn open_timed_out() -> GrpcExitReason {
    GrpcExitReason::Connect(format!("open timed out after {}s", OPEN_TIMEOUT.as_secs()))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;
    use crate::clients::disconnect::DisconnectReason;
    use crate::framework::grpc::decode::TransactionUpdate;
    use crate::framework::grpc::{
        GeyserChannel, RATE_LIMIT_COOL_DOWN, TRANSACTIONS_FILTER, classify_status,
    };
    use crate::framework::onchain::decoded::fixtures::{decoded, key};
    use crate::framework::slotbook::BlockMeta;
    use aetelier_types::onchain::{Commitment, MatchRole};
    use tokio::sync::mpsc::error::TryRecvError;
    use yellowstone_grpc_proto::tonic::Status;

    const T0: i64 = 1_791_321_067;
    const EPOCH: u64 = 1_791_321_000_000_000;

    struct Probe {
        events: mpsc::Receiver<Vec<DomainEvent>>,
        exit: oneshot::Receiver<TaskExit>,
        metrics: SourceMetrics,
    }

    impl Probe {
        fn batches(&mut self) -> Vec<Vec<String>> {
            let mut batches = Vec::new();
            while let Ok(batch) = self.events.try_recv() {
                batches.push(kinds(&batch));
            }
            batches
        }
    }

    fn section() -> GrpcSection {
        GrpcSection {
            endpoint: "http://mock.geyser".to_string(),
            x_token_env: None,
            commitment: Commitment::Finalized,
            server_filter: true,
            accept_zstd: true,
            max_decoding_message_size_mib: 64,
            ping_interval_secs: 15,
            max_pending_slots: 32,
            max_addresses_per_connection: 50,
            account_include_max: None,
            account_include_reject: Vec::new(),
        }
    }

    fn watching(seed: u8) -> WatchedAddress {
        WatchedAddress {
            address: key(seed),
            required_role: MatchRole::AccountKey,
            include_failed: true,
            keep_logs: false,
        }
    }

    fn core() -> ConnectionCore {
        ConnectionCore::new("solana", section(), SourceMetrics::default())
    }

    fn new_attach(id: u64, seed: u8, queue: usize) -> (NewAttach, Probe) {
        let (events, receiver) = mpsc::channel(queue);
        let (exit, outcome) = oneshot::channel();
        let metrics = SourceMetrics::default();
        let attach = NewAttach {
            id: AttachId(id),
            watch: watching(seed),
            metrics: metrics.clone(),
            events,
            exit,
        };
        let probe = Probe {
            events: receiver,
            exit: outcome,
            metrics,
        };
        (attach, probe)
    }

    fn join(core: &mut ConnectionCore, id: u64, seed: u8, queue: usize) -> Probe {
        let (attach, probe) = new_attach(id, seed, queue);
        core.add(attach);
        probe
    }

    fn block(slot: u64) -> BlockMeta {
        BlockMeta {
            slot,
            bank_id: 0,
            parent_slot: slot - 1,
            block_height: Some(slot - 50),
            block_time_secs: Some(T0 + slot as i64),
            blockhash: format!("hash{slot}"),
        }
    }

    fn meta(slot: u64) -> GrpcUpdate {
        GrpcUpdate::BlockMeta(block(slot))
    }

    fn tx(slot: u64, tx_index: u64, seeds: &[u8]) -> GrpcUpdate {
        let mut tx = decoded(tx_index);
        tx.static_keys = seeds.iter().map(|&seed| key(seed)).collect();
        GrpcUpdate::Transaction(TransactionUpdate {
            slot,
            bank_id: 0,
            decoded: Ok(tx),
        })
    }

    fn bank(core: &mut ConnectionCore, slot: u64, rows: &[(u64, &[u8])]) {
        for &(tx_index, seeds) in rows {
            core.on_update(tx(slot, tx_index, seeds));
        }
        core.on_update(meta(slot));
    }

    fn subscribe(core: &mut ConnectionCore, conn_epoch_us: u64) -> SubscribeRequest {
        let request = core
            .opening_request()
            .expect("the opening request is admitted");
        core.on_subscribed(conn_epoch_us, 1);
        request
    }

    fn covered(core: &mut ConnectionCore) {
        subscribe(core, EPOCH);
        core.on_acknowledged(1, true);
        core.on_update(meta(100));
        core.on_update(meta(101));
    }

    fn addresses(request: &SubscribeRequest) -> Vec<String> {
        request.transactions[TRANSACTIONS_FILTER]
            .account_include
            .clone()
    }

    fn base58(seeds: &[u8]) -> Vec<String> {
        seeds.iter().map(|&seed| key(seed).to_base58()).collect()
    }

    fn kinds(batch: &[DomainEvent]) -> Vec<String> {
        batch
            .iter()
            .map(|event| match event {
                DomainEvent::ConnectionLost { .. } => "lost".to_string(),
                DomainEvent::ConnectionResumed => "resumed".to_string(),
                DomainEvent::SlotGap(gap) => format!(
                    "gap {}->{} lost {}",
                    gap.prev_slot,
                    gap.next_slot,
                    gap.blocks_lost()
                ),
                DomainEvent::Transaction(row) => {
                    format!("row {}:{}", row.slot, row.tx_index)
                }
                DomainEvent::SlotTick(tick) => format!("tick {}", tick.slot),
                other => format!("{other:?}"),
            })
            .collect()
    }

    fn rejected_with(exit: Result<TaskExit, oneshot::error::TryRecvError>) -> String {
        match exit {
            Ok(TaskExit::Failed(DisconnectReason::ProtocolRejection {
                code: 0,
                reason,
            })) => reason,
            other => panic!("expected a code 0 rejection, got {other:?}"),
        }
    }

    #[test]
    fn the_opening_request_carries_every_attach_until_one_is_accepted() {
        let mut core = core();
        let _a = join(&mut core, 1, 2, 8);
        let _b = join(&mut core, 2, 1, 8);
        let first = core.opening_request().expect("admitted");
        assert_eq!(addresses(&first), base58(&[1, 2]));
        assert_eq!((first.from_slot, first.ping), (None, None));
        core.on_lost("transport error: connection refused");
        let _c = join(&mut core, 3, 3, 8);
        let redial = core.opening_request().expect("admitted");
        assert_eq!(addresses(&redial), base58(&[1, 2, 3]));
        core.on_subscribed(EPOCH, 1);
        core.on_acknowledged(1, true);
        assert_eq!(core.next_update(), None);
        let _d = join(&mut core, 4, 4, 8);
        core.on_lost("transport error: connection reset");
        let reopened = core.opening_request().expect("admitted");
        assert_eq!(addresses(&reopened), base58(&[1, 2, 3]));
        core.on_subscribed(EPOCH + 1, 1);
        assert_eq!(core.next_update(), None);
        core.on_acknowledged(1, true);
        let update = core.next_update().expect("the new attach joins by update");
        assert_eq!(addresses(&update), base58(&[1, 2, 3, 4]));
    }

    #[test]
    fn the_coverage_acknowledgement_opens_the_book_two_slots_after_the_highest_slot() {
        let mut core = core();
        let mut a = join(&mut core, 1, 1, 8);
        subscribe(&mut core, EPOCH);
        core.on_update(meta(100));
        core.on_update(meta(101));
        assert_eq!(core.assembler.book().boundary_slot(), None);
        core.on_acknowledged(1, true);
        assert_eq!(core.assembler.book().boundary_slot(), Some(103));
        bank(&mut core, 102, &[(0, &[1])]);
        bank(&mut core, 103, &[(0, &[1])]);
        assert_eq!(a.batches(), vec![vec!["row 103:0", "tick 103"]]);

        let mut fresh = self::core();
        let mut b = join(&mut fresh, 1, 1, 8);
        subscribe(&mut fresh, EPOCH);
        fresh.on_acknowledged(1, true);
        for slot in 200..=202 {
            bank(&mut fresh, slot, &[(0, &[1])]);
        }
        assert_eq!(fresh.assembler.book().boundary_slot(), Some(202));
        assert_eq!(b.batches(), vec![vec!["row 202:0", "tick 202"]]);
    }

    #[test]
    fn a_released_bank_reaches_each_covered_attach_as_gap_rows_then_tick() {
        let mut core = core();
        let mut a = join(&mut core, 1, 1, 8);
        let mut b = join(&mut core, 2, 2, 8);
        covered(&mut core);
        bank(&mut core, 102, &[(0, &[1]), (1, &[2]), (2, &[1, 2])]);
        assert_eq!(
            a.batches(),
            vec![vec!["row 102:0", "row 102:2", "tick 102"]]
        );
        assert_eq!(
            b.batches(),
            vec![vec!["row 102:1", "row 102:2", "tick 102"]]
        );
        core.on_update(GrpcUpdate::BlockMeta(BlockMeta {
            block_time_secs: None,
            ..block(103)
        }));
        for slot in 104..=108 {
            bank(&mut core, slot, &[(0, &[1, 2])]);
        }
        let expected: Vec<Vec<String>> = std::iter::once(vec![
            "gap 102->104 lost 1".to_string(),
            "row 104:0".to_string(),
            "tick 104".to_string(),
        ])
        .chain(
            (105..=108).map(|slot| vec![format!("row {slot}:0"), format!("tick {slot}")]),
        )
        .collect();
        assert_eq!(a.batches(), expected);
        assert_eq!(b.batches(), expected);
    }

    #[test]
    fn a_live_attach_is_covered_from_two_slots_after_its_update_acknowledgement() {
        let mut core = core();
        let mut a = join(&mut core, 1, 1, 8);
        covered(&mut core);
        bank(&mut core, 102, &[(0, &[1])]);
        a.batches();
        let mut c = join(&mut core, 2, 3, 8);
        let update = core.next_update().expect("the attach joins by update");
        assert_eq!(addresses(&update), base58(&[1, 3]));
        assert_eq!((update.from_slot, update.ping), (None, None));
        core.update_sent(2);
        core.on_update(tx(103, 0, &[1, 3]));
        core.on_acknowledged(2, true);
        core.on_update(tx(103, 1, &[3]));
        core.on_update(meta(103));
        bank(&mut core, 104, &[(0, &[1, 3])]);
        bank(&mut core, 105, &[(0, &[1, 3])]);
        assert_eq!(
            a.batches(),
            vec![
                vec!["row 103:0", "tick 103"],
                vec!["row 104:0", "tick 104"],
                vec!["row 105:0", "tick 105"],
            ]
        );
        assert_eq!(c.batches(), vec![vec!["row 105:0", "tick 105"]]);
    }

    #[test]
    fn a_live_attach_is_covered_from_the_second_bank_after_its_acknowledgement() {
        let mut core = core();
        let mut a = join(&mut core, 1, 1, 8);
        covered(&mut core);
        bank(&mut core, 102, &[(0, &[1])]);
        a.batches();
        let mut c = join(&mut core, 2, 3, 8);
        core.next_update().expect("the attach joins by update");
        core.update_sent(2);
        core.on_acknowledged(2, true);
        for (slot, parent_slot, height) in
            [(104, 102, 53), (108, 104, 54), (109, 108, 55)]
        {
            core.on_update(tx(slot, 0, &[1, 3]));
            core.on_update(GrpcUpdate::BlockMeta(BlockMeta {
                parent_slot,
                block_height: Some(height),
                ..block(slot)
            }));
        }
        assert_eq!(
            a.batches(),
            vec![
                vec!["row 104:0", "tick 104"],
                vec!["row 108:0", "tick 108"],
                vec!["row 109:0", "tick 109"],
            ]
        );
        assert_eq!(
            c.batches(),
            vec![vec!["row 108:0", "tick 108"], vec!["row 109:0", "tick 109"]]
        );
    }

    #[test]
    fn one_update_is_in_flight_and_later_changes_coalesce_into_the_next() {
        let mut core = core();
        let _a = join(&mut core, 1, 1, 8);
        covered(&mut core);
        let _c = join(&mut core, 2, 3, 8);
        let first = core.next_update().expect("the first update");
        assert_eq!(addresses(&first), base58(&[1, 3]));
        assert_eq!(core.next_update(), None);
        core.update_sent(2);
        let _d = join(&mut core, 3, 4, 8);
        core.remove(AttachId(1));
        assert_eq!(core.next_update(), None);
        core.on_acknowledged(2, true);
        let second = core.next_update().expect("the coalesced update");
        assert_eq!(addresses(&second), base58(&[3, 4]));
        core.update_sent(3);
        assert_eq!(core.next_update(), None);
        core.on_acknowledged(3, true);
        assert_eq!(core.next_update(), None);
    }

    #[test]
    fn a_detach_narrows_the_filter_with_one_full_update() {
        let mut core = core();
        let _a = join(&mut core, 1, 1, 8);
        let _b = join(&mut core, 2, 2, 8);
        covered(&mut core);
        assert_eq!(core.next_update(), None);
        core.remove(AttachId(2));
        let narrowed = core.next_update().expect("the narrowing update");
        assert_eq!(
            narrowed,
            StreamFilter::new([&watching(1)], &section()).request()
        );
        core.update_sent(2);
        assert_eq!(core.next_update(), None);
        core.on_acknowledged(2, true);
        assert_eq!(core.next_update(), None);
        core.remove(AttachId(9));
        assert_eq!(core.next_update(), None);
        let _e = join(&mut core, 5, 5, 8);
        core.remove(AttachId(5));
        assert_eq!(core.next_update(), None);
        assert!(!core.is_empty());
    }

    #[test]
    fn a_rejected_update_resolves_only_the_attaches_it_carried() {
        let mut core = core();
        let mut a = join(&mut core, 1, 1, 8);
        covered(&mut core);
        let mut c = join(&mut core, 2, 3, 8);
        core.next_update().expect("the update");
        core.update_sent(2);
        let mut d = join(&mut core, 3, 4, 8);
        let reason = "filter update rejected: gRPC InvalidArgument: too many addresses";
        core.reject_update(reason);
        assert_eq!(rejected_with(c.exit.try_recv()), reason);
        assert_eq!(c.events.try_recv().err(), Some(TryRecvError::Disconnected));
        assert!(d.exit.try_recv().is_err());
        core.on_lost("server going away (1001): filter update rejected");
        assert_eq!(a.batches(), vec![vec!["lost"]]);
        assert_eq!(d.batches(), vec![vec!["lost"]]);
        let reopened = core.opening_request().expect("admitted");
        assert_eq!(addresses(&reopened), base58(&[1]));
    }

    #[test]
    fn a_loss_flushes_held_banks_before_telling_every_attach() {
        let mut core = core();
        let mut a = join(&mut core, 1, 1, 8);
        let mut b = join(&mut core, 2, 2, 8);
        covered(&mut core);
        bank(&mut core, 102, &[(0, &[1])]);
        a.batches();
        b.batches();
        bank(&mut core, 104, &[(0, &[1])]);
        assert!(a.batches().is_empty());
        core.on_lost("transport error: gRPC Unknown: h2 protocol error");
        assert_eq!(
            a.batches(),
            vec![
                vec!["gap 102->104 lost 1", "row 104:0", "tick 104"],
                vec!["lost"],
            ]
        );
        assert_eq!(
            b.batches(),
            vec![vec!["gap 102->104 lost 1", "tick 104"], vec!["lost"]]
        );
    }

    #[test]
    fn a_resubscribe_resumes_each_told_attach_before_its_next_tick() {
        let mut core = core();
        let mut a = join(&mut core, 1, 1, 8);
        let mut b = join(&mut core, 2, 2, 1);
        covered(&mut core);
        bank(&mut core, 102, &[(0, &[1, 2])]);
        a.batches();
        b.batches();
        core.on_lost("transport error: connection reset");
        core.opening_request().expect("admitted");
        core.on_subscribed(EPOCH + 1, 1);
        assert_eq!(a.batches(), vec![vec!["lost"], vec!["resumed"]]);
        assert_eq!(b.batches(), vec![vec!["lost"]]);
        core.on_acknowledged(1, true);
        core.on_update(meta(110));
        core.on_update(meta(111));
        bank(&mut core, 112, &[(0, &[1, 2])]);
        assert_eq!(
            a.batches(),
            vec![vec!["gap 102->112 lost 9", "row 112:0", "tick 112"]]
        );
        assert_eq!(
            b.batches(),
            vec![vec![
                "resumed",
                "gap 102->112 lost 9",
                "row 112:0",
                "tick 112"
            ]]
        );
    }

    #[test]
    fn a_reopened_connection_carries_only_accepted_attaches_and_updates_the_rest() {
        let mut core = core();
        let _a = join(&mut core, 1, 1, 8);
        covered(&mut core);
        let _c = join(&mut core, 2, 3, 8);
        core.next_update().expect("the update");
        core.update_sent(2);
        core.on_lost("transport error: connection reset");
        let reopened = core.opening_request().expect("admitted");
        assert_eq!(addresses(&reopened), base58(&[1]));
        core.on_subscribed(EPOCH + 1, 1);
        assert_eq!(core.next_update(), None);
        core.on_acknowledged(1, true);
        let update = core.next_update().expect("the unaccepted attach rejoins");
        assert_eq!(addresses(&update), base58(&[1, 3]));
    }

    #[test]
    fn a_full_queue_drops_whole_banks_and_the_next_batch_carries_loss_resume_and_gap() {
        let mut core = core();
        let mut a = join(&mut core, 1, 1, 1);
        covered(&mut core);
        for slot in 102..=104 {
            bank(&mut core, slot, &[(0, &[1])]);
        }
        assert_eq!(a.metrics.snapshot().fanout_overflow, 1);
        assert_eq!(a.batches(), vec![vec!["row 102:0", "tick 102"]]);
        bank(&mut core, 105, &[(0, &[1])]);
        let batch = a.events.try_recv().expect("the next batch");
        assert_eq!(
            kinds(&batch),
            vec![
                "lost",
                "resumed",
                "gap 102->105 lost 2",
                "row 105:0",
                "tick 105"
            ]
        );
        assert!(matches!(
            &batch[0],
            DomainEvent::ConnectionLost { reason } if reason == "fan-out queue full"
        ));
        bank(&mut core, 106, &[(0, &[1])]);
        assert_eq!(a.batches(), vec![vec!["row 106:0", "tick 106"]]);
        assert_eq!(a.metrics.snapshot().fanout_overflow, 1);
    }

    #[test]
    fn connection_counters_reach_only_the_oldest_attach_and_pass_on_at_detach() {
        let mut core = core();
        let a = join(&mut core, 1, 1, 8);
        let b = join(&mut core, 2, 2, 8);
        covered(&mut core);
        core.on_update(tx(102, 0, &[9]));
        let mut vote = decoded(1);
        vote.is_vote = true;
        vote.static_keys = vec![key(1)];
        core.on_update(GrpcUpdate::Transaction(TransactionUpdate {
            slot: 102,
            bank_id: 0,
            decoded: Ok(vote),
        }));
        core.on_update(GrpcUpdate::Unrequested("slot"));
        let oldest = a.metrics.snapshot();
        assert_eq!(
            (
                oldest.filtered_out,
                oldest.votes_excluded,
                oldest.dropped_frames
            ),
            (1, 1, 1)
        );
        assert_eq!(oldest.msgs, 4);
        assert_eq!(oldest, core.totals().snapshot());
        assert_eq!(b.metrics.snapshot(), Default::default());
        core.remove(AttachId(1));
        core.on_update(tx(103, 0, &[9]));
        assert_eq!(b.metrics.snapshot().filtered_out, 1);
        assert_eq!(a.metrics.snapshot().filtered_out, 1);
        assert_eq!(core.totals().snapshot().filtered_out, 2);
    }

    #[test]
    fn every_attach_takes_the_physical_connection_epoch() {
        let mut core = core();
        let a = join(&mut core, 1, 1, 8);
        let b = join(&mut core, 2, 2, 8);
        subscribe(&mut core, EPOCH);
        assert_eq!(a.metrics.conn_epoch_us(), EPOCH);
        assert_eq!(b.metrics.conn_epoch_us(), EPOCH);
        let c = join(&mut core, 3, 3, 8);
        assert_eq!(c.metrics.conn_epoch_us(), EPOCH);
        core.on_lost("transport error: connection reset");
        let d = join(&mut core, 4, 4, 8);
        assert_eq!(d.metrics.conn_epoch_us(), 0);
        subscribe(&mut core, EPOCH + 5);
        for probe in [&a, &b, &c, &d] {
            assert_eq!(probe.metrics.conn_epoch_us(), EPOCH + 5);
        }
    }

    #[test]
    fn a_terminal_failure_resolves_every_attach_with_a_code_zero_rejection_and_no_resume()
    {
        let mut core = core();
        let mut a = join(&mut core, 1, 1, 8);
        let mut b = join(&mut core, 2, 2, 8);
        covered(&mut core);
        bank(&mut core, 102, &[(0, &[1])]);
        a.batches();
        b.batches();
        bank(&mut core, 104, &[(0, &[1])]);
        let reason = "gRPC Unauthenticated: invalid x-token (yellowstone-grpc-proto@14.0.1, \
                      server unknown server)";
        core.on_terminal(reason);
        assert_eq!(
            a.batches(),
            vec![vec!["gap 102->104 lost 1", "row 104:0", "tick 104"]]
        );
        assert_eq!(b.batches(), vec![vec!["gap 102->104 lost 1", "tick 104"]]);
        for probe in [&mut a, &mut b] {
            assert_eq!(
                probe.events.try_recv().err(),
                Some(TryRecvError::Disconnected)
            );
            assert_eq!(rejected_with(probe.exit.try_recv()), reason);
        }
        assert!(core.is_empty());
        assert_eq!(core.next_update(), None);
        assert!(core.opening_request().is_err());
    }

    #[test]
    fn an_unanswered_acknowledgement_still_opens_coverage() {
        let mut core = core();
        let mut a = join(&mut core, 1, 1, 8);
        subscribe(&mut core, EPOCH);
        core.on_update(meta(100));
        core.on_acknowledged(1, false);
        assert_eq!(core.assembler.book().boundary_slot(), Some(102));
        core.on_update(meta(101));
        bank(&mut core, 102, &[(0, &[1])]);
        assert_eq!(a.batches(), vec![vec!["row 102:0", "tick 102"]]);
        let mut c = join(&mut core, 2, 3, 8);
        core.next_update().expect("the update");
        core.update_sent(2);
        core.on_update(tx(103, 0, &[1, 3]));
        core.on_acknowledged(2, false);
        assert_eq!(core.next_update(), None);
        core.on_update(meta(103));
        bank(&mut core, 104, &[(0, &[3])]);
        bank(&mut core, 105, &[(0, &[3])]);
        assert_eq!(c.batches(), vec![vec!["row 105:0", "tick 105"]]);
    }

    #[test]
    fn a_quiet_attach_receives_ticks_without_rows() {
        let mut core = core();
        let mut a = join(&mut core, 1, 1, 8);
        let mut b = join(&mut core, 2, 2, 8);
        covered(&mut core);
        bank(&mut core, 102, &[(0, &[1])]);
        bank(&mut core, 103, &[]);
        assert_eq!(
            a.batches(),
            vec![vec!["row 102:0", "tick 102"], vec!["tick 103"]]
        );
        assert_eq!(b.batches(), vec![vec!["tick 102"], vec!["tick 103"]]);
    }

    #[test]
    fn behind_tip_is_measured_from_each_attach_last_delivered_slot() {
        let mut core = core();
        let mut a = join(&mut core, 1, 1, 8);
        let b = join(&mut core, 2, 2, 1);
        covered(&mut core);
        for slot in 102..=104 {
            bank(&mut core, slot, &[(0, &[1, 2])]);
        }
        core.set_tip(110);
        assert_eq!(a.metrics.snapshot().behind_tip_slots, 6);
        assert_eq!(b.metrics.snapshot().behind_tip_slots, 8);
        let c = join(&mut core, 3, 3, 8);
        core.set_tip(112);
        assert_eq!(a.metrics.snapshot().behind_tip_slots, 8);
        assert_eq!(b.metrics.snapshot().behind_tip_slots, 10);
        assert_eq!(c.metrics.snapshot().behind_tip_slots, 8);
        assert_eq!(a.batches().len(), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn the_connect_limiter_admits_ten_attempts_per_minute() {
        let mut limiter = ConnectLimiter::new(RateWindow {
            max: 10,
            per: Duration::from_secs(60),
        });
        let start = Instant::now();
        for attempt in 0..10 {
            let now = Instant::now();
            assert_eq!(limiter.next_allowed(now), now, "{attempt}");
            limiter.record(now);
            tokio::time::advance(Duration::from_secs(1)).await;
        }
        let now = Instant::now();
        assert_eq!(limiter.next_allowed(now), start + Duration::from_secs(60));
        tokio::time::sleep_until(start + Duration::from_secs(60)).await;
        let now = Instant::now();
        assert_eq!(limiter.next_allowed(now), now);
        limiter.record(now);
        assert_eq!(limiter.next_allowed(now), start + Duration::from_secs(61));
    }

    #[tokio::test(start_paused = true)]
    async fn a_cool_down_defers_the_next_attempt_past_the_backoff() {
        let mut limiter = ConnectLimiter::new(RateWindow {
            max: 1_000,
            per: Duration::from_secs(60),
        });
        let now = Instant::now();
        let secs = Duration::from_secs;
        assert_eq!(
            next_attempt_at(
                now,
                ReconnectAction::RetryAfter(secs(2)),
                Some(RATE_LIMIT_COOL_DOWN),
                &mut limiter
            ),
            now + secs(10)
        );
        assert_eq!(
            next_attempt_at(
                now,
                ReconnectAction::RetryAfter(secs(30)),
                Some(secs(10)),
                &mut limiter
            ),
            now + secs(30)
        );
        assert_eq!(
            next_attempt_at(
                now,
                ReconnectAction::RetryImmediately,
                Some(secs(30)),
                &mut limiter
            ),
            now + secs(30)
        );
        assert_eq!(
            next_attempt_at(
                now,
                ReconnectAction::RetryAfter(secs(2)),
                None,
                &mut limiter
            ),
            now + secs(2)
        );
        let exhausted = Status::resource_exhausted("too many streams");
        assert_eq!(
            classify_status(&exhausted, None),
            crate::framework::grpc::StatusClass::CoolDown(RATE_LIMIT_COOL_DOWN)
        );
        let ExitClass::Retry { reason, cool_down } = GrpcExitReason::Status {
            status: exhausted,
            pending: None,
        }
        .classify(&ServerVersion::default()) else {
            panic!("resource exhaustion is retried");
        };
        let mut policy = framework_reconnect_policy(Some(&ReconnectSection {
            initial_delay_ms: Some(1_000),
            max_delay_ms: Some(30_000),
            max_attempts: None,
            jitter_factor: Some(0.0),
        }));
        let action = policy.next_action(&reason);
        assert!(matches!(action, ReconnectAction::RetryAfter(delay) if delay == secs(2)));
        assert_eq!(
            next_attempt_at(now, action, cool_down, &mut limiter),
            now + secs(10)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_circuit_open_action_waits_until_its_instant() {
        let mut limiter = ConnectLimiter::new(RateWindow {
            max: 1_000,
            per: Duration::from_secs(60),
        });
        let now = Instant::now();
        let until = now + Duration::from_secs(8);
        assert_eq!(
            next_attempt_at(
                now,
                ReconnectAction::CircuitOpen { until },
                None,
                &mut limiter
            ),
            until
        );
        assert_eq!(
            next_attempt_at(
                now,
                ReconnectAction::GiveUp {
                    reason: "never reached".to_string()
                },
                None,
                &mut limiter
            ),
            now
        );
        let mut policy = framework_reconnect_policy(Some(&ReconnectSection {
            initial_delay_ms: Some(1_000),
            max_delay_ms: Some(2_000),
            max_attempts: Some(1),
            jitter_factor: Some(0.0),
        }));
        let action = policy.next_action(&DisconnectReason::TransportError {
            source: "connection refused".into(),
        });
        assert!(matches!(action, ReconnectAction::CircuitOpen { .. }));
        assert_eq!(
            next_attempt_at(now, action, None, &mut limiter),
            now + Duration::from_secs(8)
        );
    }

    #[derive(Default)]
    struct StalledDial {
        dials: std::sync::Mutex<Vec<Instant>>,
    }

    #[async_trait::async_trait]
    impl GeyserDial for StalledDial {
        async fn dial(
            &self,
            _settings: &GrpcSettings,
        ) -> Result<GeyserChannel, GrpcExitReason> {
            self.dials.lock().expect("dials").push(Instant::now());
            std::future::pending().await
        }
    }

    fn task_key() -> ConnectionKey {
        ConnectionKey {
            endpoint: section().endpoint,
            commitment: Commitment::Finalized,
            token_env: None,
            shard: 0,
        }
    }

    fn task_context() -> ConnectionContext {
        ConnectionContext {
            venue: "solana",
            section: section(),
            x_token: None,
            reconnect: Some(ReconnectSection {
                initial_delay_ms: Some(1_000),
                max_delay_ms: Some(30_000),
                max_attempts: None,
                jitter_factor: Some(0.0),
            }),
            connect_rate: RateWindow {
                max: 10,
                per: Duration::from_secs(60),
            },
        }
    }

    async fn next_loss(probe: &mut Probe, within: Duration) -> String {
        let batch = timeout(within, probe.events.recv())
            .await
            .expect("the loss arrives")
            .expect("the attach is open");
        match batch.as_slice() {
            [DomainEvent::ConnectionLost { reason }] => reason.clone(),
            other => panic!("expected one loss, got {other:?}"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_dial_that_never_completes_times_out_and_retries() {
        let stalled = Arc::new(StalledDial::default());
        let dial: Arc<dyn GeyserDial> = stalled.clone();
        let (first, mut probe) = new_attach(1, 1, 8);
        let started = Instant::now();
        let connection = SharedGrpcConnection::spawn(
            task_key(),
            task_context(),
            dial,
            Arc::default(),
            first,
        );
        let reason = next_loss(&mut probe, Duration::from_secs(20)).await;
        assert_eq!(started.elapsed(), OPEN_TIMEOUT);
        assert!(reason.contains("open timed out after 15s"), "{reason}");
        sleep_until(started + Duration::from_secs(18)).await;
        assert_eq!(
            *stalled.dials.lock().expect("dials"),
            vec![started, started + Duration::from_secs(17)]
        );
        assert!(!connection.is_closed());
        assert!(connection.send(Command::Detach(AttachId(1))).is_ok());
        next_loss(&mut probe, Duration::from_secs(20)).await;
        assert_eq!(started.elapsed(), Duration::from_secs(17) + OPEN_TIMEOUT);
        assert!(probe.events.recv().await.is_none());
        sleep_until(started + Duration::from_secs(120)).await;
        assert_eq!(stalled.dials.lock().expect("dials").len(), 2);
        assert!(!connection.is_closed());
        drop(connection);
        sleep_until(started + Duration::from_secs(121)).await;
        assert_eq!(Arc::strong_count(&stalled), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn an_attach_queued_behind_the_last_detach_is_dialed_for_and_never_dropped() {
        let stalled = Arc::new(StalledDial::default());
        let dial: Arc<dyn GeyserDial> = stalled.clone();
        let (first, mut leaving) = new_attach(1, 1, 8);
        let started = Instant::now();
        let connection = SharedGrpcConnection::spawn(
            task_key(),
            task_context(),
            dial,
            Arc::default(),
            first,
        );
        next_loss(&mut leaving, Duration::from_secs(20)).await;
        assert_eq!(started.elapsed(), OPEN_TIMEOUT);
        let (second, mut joining) = new_attach(2, 2, 8);
        assert!(connection.send(Command::Detach(AttachId(1))).is_ok());
        assert!(connection.send(Command::Attach(second)).is_ok());
        assert!(leaving.events.recv().await.is_none());
        let reason = next_loss(&mut joining, Duration::from_secs(40)).await;
        assert!(reason.contains("open timed out after 15s"), "{reason}");
        assert_eq!(started.elapsed(), Duration::from_secs(17) + OPEN_TIMEOUT);
        assert_eq!(
            *stalled.dials.lock().expect("dials"),
            vec![started, started + Duration::from_secs(17)]
        );
        assert_eq!(
            joining.exit.try_recv().err(),
            Some(oneshot::error::TryRecvError::Empty)
        );
        assert!(!connection.is_closed());
    }

    #[derive(Default)]
    struct TerminalDial {
        dialing: AtomicBool,
        released: AtomicBool,
    }

    #[async_trait::async_trait]
    impl GeyserDial for TerminalDial {
        async fn dial(
            &self,
            _settings: &GrpcSettings,
        ) -> Result<GeyserChannel, GrpcExitReason> {
            self.dialing.store(true, Ordering::Release);
            while !self.released.load(Ordering::Acquire) {
                std::thread::yield_now();
            }
            Err(GrpcExitReason::Setup("invalid uri character".to_string()))
        }
    }

    #[test]
    fn an_attach_accepted_while_a_terminal_exit_closes_the_connection_is_rejected() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
            .expect("a runtime");
        let handle = runtime.handle().clone();
        let (stop, stopped) = oneshot::channel::<()>();
        let driver = std::thread::spawn(move || runtime.block_on(stopped));
        let entered = handle.enter();
        let mut spins = 0u64;
        let (mut accepted, mut lost) = (0, Vec::new());
        for _ in 0..10_000 {
            let terminal = Arc::new(TerminalDial::default());
            let dial: Arc<dyn GeyserDial> = terminal.clone();
            let (first, _first) = new_attach(1, 1, 8);
            let (late, probe) = new_attach(2, 2, 8);
            let connection = SharedGrpcConnection::spawn(
                task_key(),
                task_context(),
                dial,
                Arc::default(),
                first,
            );
            while !terminal.dialing.load(Ordering::Acquire) {
                std::thread::yield_now();
            }
            terminal.released.store(true, Ordering::Release);
            for _ in 0..spins {
                std::hint::spin_loop();
            }
            let sent = connection.send(Command::Attach(late)).is_ok();
            drop(connection);
            if !sent {
                spins = spins.saturating_sub(8);
                continue;
            }
            spins += 8;
            accepted += 1;
            match probe.exit.blocking_recv() {
                Ok(TaskExit::Failed(DisconnectReason::ProtocolRejection {
                    code: 0,
                    reason,
                })) if reason == "invalid uri character" => {}
                other => lost.push(format!("{other:?}")),
            }
        }
        drop(entered);
        stop.send(()).expect("the runtime is running");
        driver
            .join()
            .expect("the runtime thread ends")
            .expect("the runtime stops on request");
        assert!(accepted > 0);
        assert!(
            lost.is_empty(),
            "{} of {accepted} accepted attaches ended without the rejection: {:?}",
            lost.len(),
            lost.first()
        );
    }
}
