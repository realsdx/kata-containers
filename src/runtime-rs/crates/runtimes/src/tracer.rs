// Copyright (c) 2019-2022 Alibaba Cloud
// Copyright (c) 2019-2022 Ant Group
//
// SPDX-License-Identifier: Apache-2.0
//

use std::cmp::min;
use std::sync::Arc;

use anyhow::Result;
use opentelemetry::global;
use opentelemetry::runtime::Tokio;
use tokio::sync::Notify;
use tracing::{span, subscriber::NoSubscriber, Span, Subscriber};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::prelude::*;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Registry;

const DEFAULT_JAEGER_URL: &str = "http://localhost:14268/api/traces";

// Wait for the root span to close before shutting down the exporter;
// otherwise the final root span can be lost. Dropping our handle is not
// enough: active child spans also hold references to the root.
struct RootSpanCloseLayer {
    closed: Arc<Notify>,
}

impl<SubscriberType> Layer<SubscriberType> for RootSpanCloseLayer
where
    SubscriberType: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_close(&self, id: span::Id, ctx: Context<'_, SubscriberType>) {
        if let Some(span) = ctx.span(&id) {
            if span.metadata().target() == module_path!() && span.name() == "root-span" {
                self.closed.notify_one();
            }
        }
    }
}

/// The tracer wrapper for kata-containers
/// The fields and member methods should ALWAYS be PRIVATE and be exposed in a safe
/// way to other modules
unsafe impl Send for KataTracer {}
unsafe impl Sync for KataTracer {}
pub struct KataTracer {
    subscriber: Arc<dyn Subscriber + Send + Sync>,
    root_span: Option<Span>,
    root_closed: Arc<Notify>,
    enabled: bool,
}

impl Default for KataTracer {
    fn default() -> Self {
        Self::new()
    }
}

impl KataTracer {
    /// Constructor of KataTracer, this is a dummy implementation for static initialization
    pub fn new() -> Self {
        Self {
            subscriber: Arc::new(NoSubscriber::default()),
            root_span: None,
            root_closed: Arc::new(Notify::new()),
            enabled: false,
        }
    }

    /// Set the tracing enabled flag
    fn enable(&mut self) {
        self.enabled = true;
    }

    /// Return whether the tracing is enabled, enabled by [`trace_setup`]
    fn enabled(&self) -> bool {
        self.enabled
    }

    /// Call when the tracing is enabled (set in toml configuration file)
    /// This setup the subscriber, which maintains the span's information, to global and
    /// inside KATA_TRACER.
    ///
    /// Note that the span will be noop(not collected) if a invalid subscriber is set
    pub fn trace_setup(
        &mut self,
        sid: &str,
        jaeger_endpoint: &str,
        jaeger_username: &str,
        jaeger_password: &str,
    ) -> Result<()> {
        if self.enabled() {
            return Ok(());
        }

        // If varify jaeger config returns an error, it means that the tracing should not be enabled
        let endpoint = verify_jaeger_config(jaeger_endpoint, jaeger_username, jaeger_password)?;

        // derive a subscriber to collect span info
        let tracer = opentelemetry_jaeger::new_collector_pipeline()
            .with_service_name(format!("kata-sb-{}", &sid[0..min(8, sid.len())]))
            .with_endpoint(endpoint)
            .with_username(jaeger_username)
            .with_password(jaeger_password)
            .with_hyper()
            .install_batch(Tokio)?;

        let layer = tracing_opentelemetry::layer().with_tracer(tracer);

        // Keep this layer outermost so OpenTelemetry queues the root before we notify shutdown.
        let sub = Registry::default().with(layer).with(RootSpanCloseLayer {
            closed: self.root_closed.clone(),
        });

        // we use Arc to let global subscriber and katatracer to SHARE the SAME subscriber
        // this is for record the global subscriber into a global variable KATA_TRACER for more usages
        let subscriber = Arc::new(sub);
        tracing::subscriber::set_global_default(subscriber.clone())?;
        self.subscriber = subscriber;

        self.root_span = Some(span!(parent: None, tracing::Level::TRACE, "root-span"));

        // modity the enable state, note that we have successfully enable tracing
        self.enable();

        info!(sl!(), "Tracing enabled successfully");
        Ok(())
    }

    pub fn begin_shutdown(&mut self) -> Option<Arc<Notify>> {
        drop(self.root_span.take()?);
        // Notify retains a permit if the root closed synchronously before the waiter starts.
        Some(self.root_closed.clone())
    }

    pub fn shutdown_provider() {
        global::shutdown_tracer_provider();
    }

    pub fn root_span(&self) -> Option<Span> {
        self.root_span.clone()
    }
}

/// Verifying the configuration of jaeger and setup the default value
fn verify_jaeger_config(endpoint: &str, username: &str, passwd: &str) -> Result<String> {
    if username.is_empty() && !passwd.is_empty() {
        warn!(
            sl!(),
            "Jaeger password with empty username is not allowed, tracing is NOT enabled"
        );
        return Err(anyhow::anyhow!("Empty username with non-empty password"));
    }

    // set the default endpoint address, this expects a jaeger-collector running on localhost:14268
    let endpt = if endpoint.is_empty() {
        DEFAULT_JAEGER_URL
    } else {
        endpoint
    }
    .to_owned();

    Ok(endpt)
}
