// Copyright (c) 2019-2025 Alibaba Cloud
// Copyright (c) 2019-2025 Ant Group
//
// SPDX-License-Identifier: Apache-2.0
//

use std::{
    convert::{TryFrom, TryInto},
    sync::Arc,
};

use async_trait::async_trait;
use common::types::{SandboxRequest, SandboxResponse};
use containerd_shim_protos::{sandbox_api, sandbox_async};
use runtimes::RuntimeHandlerManager;
use tracing::{info_span, Instrument, Span};
use ttrpc::{self, r#async::TtrpcContext};

pub(crate) struct SandboxService {
    handler: Arc<RuntimeHandlerManager>,
}

impl SandboxService {
    pub(crate) fn new(handler: Arc<RuntimeHandlerManager>) -> Self {
        Self { handler }
    }

    async fn handler_message<TtrpcReq, TtrpcResp>(
        &self,
        ctx: &TtrpcContext,
        req: TtrpcReq,
    ) -> ttrpc::Result<TtrpcResp>
    where
        SandboxRequest: TryFrom<TtrpcReq>,
        <SandboxRequest as TryFrom<TtrpcReq>>::Error: std::fmt::Debug,
        TtrpcResp: TryFrom<SandboxResponse>,
        <TtrpcResp as TryFrom<SandboxResponse>>::Error: std::fmt::Debug,
    {
        let r = req.try_into().map_err(|err| {
            ttrpc::Error::Others(format!("failed to translate from shim {err:?}"))
        })?;
        let logger = sl!().new(o!("stream id" =>  ctx.mh.stream_id));
        debug!(logger, "====> sandbox service {:?}", &r);
        let resp = self
            .handler
            .handler_sandbox_message(r)
            .await
            .map_err(|err| {
                ttrpc::Error::Others(format!("failed to handle sandbox message {err:?}"))
            })?;
        debug!(logger, "<==== sandbox service {:?}", &resp);
        resp.try_into()
            .map_err(|err| ttrpc::Error::Others(format!("failed to translate to shim {err:?}")))
    }
}

macro_rules! impl_service {
    ($($name: tt | $span: literal | $finish: literal | $req: ty | $resp: ty),*) => {
        #[async_trait]
        impl sandbox_async::Sandbox for SandboxService {
            $(async fn $name(&self, ctx: &TtrpcContext, req: $req) -> ttrpc::Result<$resp> {
                let parent = self.handler.trace_parent().await;
                let request_span = parent.as_ref().map_or_else(Span::none, |parent| {
                    info_span!(parent: parent, $span)
                });
                let result = self.handler_message(ctx, req).instrument(request_span).await;
                drop(parent);
                if $finish {
                    self.handler.finish_tracing_if_requested().await;
                }
                result
            })*
        }
    };
}

impl_service!(
    create_sandbox
        | "ttrpc.sandbox.CreateSandbox"
        | false
        | sandbox_api::CreateSandboxRequest
        | sandbox_api::CreateSandboxResponse,
    start_sandbox
        | "ttrpc.sandbox.StartSandbox"
        | false
        | sandbox_api::StartSandboxRequest
        | sandbox_api::StartSandboxResponse,
    platform
        | "ttrpc.sandbox.Platform"
        | false
        | sandbox_api::PlatformRequest
        | sandbox_api::PlatformResponse,
    stop_sandbox
        | "ttrpc.sandbox.StopSandbox"
        | false
        | sandbox_api::StopSandboxRequest
        | sandbox_api::StopSandboxResponse,
    wait_sandbox
        | "ttrpc.sandbox.WaitSandbox"
        | false
        | sandbox_api::WaitSandboxRequest
        | sandbox_api::WaitSandboxResponse,
    sandbox_status
        | "ttrpc.sandbox.SandboxStatus"
        | false
        | sandbox_api::SandboxStatusRequest
        | sandbox_api::SandboxStatusResponse,
    ping_sandbox
        | "ttrpc.sandbox.PingSandbox"
        | false
        | sandbox_api::PingRequest
        | sandbox_api::PingResponse,
    shutdown_sandbox
        | "ttrpc.sandbox.ShutdownSandbox"
        | true
        | sandbox_api::ShutdownSandboxRequest
        | sandbox_api::ShutdownSandboxResponse
);
