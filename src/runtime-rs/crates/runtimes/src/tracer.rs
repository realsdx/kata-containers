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
use tracing::{span, subscriber::NoSubscriber, Span, Subscriber};
use tracing_subscriber::prelude::*;
use tracing_subscriber::Registry;

const DEFAULT_JAEGER_URL: &str = "http://localhost:14268/api/traces";

/// The tracer wrapper for kata-containers
/// The fields and member methods should ALWAYS be PRIVATE and be exposed in a safe
/// way to other modules
unsafe impl Send for KataTracer {}
unsafe impl Sync for KataTracer {}
pub struct KataTracer {
    subscriber: Arc<dyn Subscriber + Send + Sync>,
    root_span: Option<Span>,
    enabled: bool,
    finish_requested: bool,
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
            enabled: false,
            finish_requested: false,
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
        // If varify jaeger config returns an error, it means that the tracing should not be enabled
        let endpoint = verify_jaeger_config(jaeger_endpoint, jaeger_username, jaeger_password)?;

        // derive a subscriber to collect span info
        let tracer = opentelemetry_jaeger::new_collector_pipeline()
            .with_service_name(format!("kata-sb-{}", &sid[0..min(8, sid.len())]))
            .with_endpoint(endpoint)
            .with_username(jaeger_username)
            .with_password(jaeger_password)
            .with_reqwest()
            .install_batch(Tokio)?;

        let layer = tracing_opentelemetry::layer().with_tracer(tracer);

        let sub = Registry::default().with(layer);

        // we use Arc to let global subscriber and katatracer to SHARE the SAME subscriber
        // this is for record the global subscriber into a global variable KATA_TRACER for more usages
        let subscriber = Arc::new(sub);
        tracing::subscriber::set_global_default(subscriber.clone())?;
        self.subscriber = subscriber;

        // Keep one owned handle so all shim requests can share a sandbox-lifetime parent.
        self.root_span = Some(span!(tracing::Level::TRACE, "root-span", sandbox_id = %sid));

        // modity the enable state, note that we have successfully enable tracing
        self.enable();

        info!(sl!(), "Tracing enabled successfully");
        Ok(())
    }

    pub fn root_span(&self) -> Option<Span> {
        self.root_span.clone()
    }

    pub fn request_finish(&mut self) {
        self.finish_requested = true;
    }

    /// Finish tracing after the final request span has closed.
    pub fn finish_if_requested(&mut self) -> bool {
        if !self.finish_requested {
            return false;
        }
        self.finish_requested = false;

        if self.enabled() {
            // Dropping the final owned handle closes the root before provider shutdown.
            self.root_span.take();
            self.enabled = false;
        }

        true
    }

    pub fn shutdown_provider() {
        global::shutdown_tracer_provider();
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
