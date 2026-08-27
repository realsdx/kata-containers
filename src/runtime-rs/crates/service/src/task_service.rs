// Copyright (c) 2019-2022 Alibaba Cloud
// Copyright (c) 2019-2022 Ant Group
//
// SPDX-License-Identifier: Apache-2.0
//

use std::{
    convert::{TryFrom, TryInto},
    sync::Arc,
};

use async_trait::async_trait;
use common::types::{TaskRequest, TaskResponse};
use containerd_shim_protos::{api, shim_async};
use ttrpc::{self, r#async::TtrpcContext};

use runtimes::RuntimeHandlerManager;
use tracing::{info_span, Instrument, Span};

pub(crate) struct TaskService {
    handler: Arc<RuntimeHandlerManager>,
}

impl TaskService {
    pub(crate) fn new(handler: Arc<RuntimeHandlerManager>) -> Self {
        Self { handler }
    }

    async fn handler_message<TtrpcReq, TtrpcResp>(
        &self,
        ctx: &TtrpcContext,
        req: TtrpcReq,
    ) -> ttrpc::Result<TtrpcResp>
    where
        TaskRequest: TryFrom<TtrpcReq>,
        <TaskRequest as TryFrom<TtrpcReq>>::Error: std::fmt::Debug,
        TtrpcResp: TryFrom<TaskResponse>,
        <TtrpcResp as TryFrom<TaskResponse>>::Error: std::fmt::Debug,
    {
        let r = req.try_into().map_err(|err| {
            ttrpc::Error::Others(format!("failed to translate from shim {err:?}"))
        })?;
        let logger = sl!().new(o!("stream id" =>  ctx.mh.stream_id));
        debug!(logger, "====> task service {:?}", &r);
        let resp = self
            .handler
            .handler_task_message(r)
            .await
            .map_err(|err| ttrpc::Error::Others(format!("failed to handle message {err:?}")))?;
        debug!(logger, "<==== task service {:?}", &resp);
        resp.try_into()
            .map_err(|err| ttrpc::Error::Others(format!("failed to translate to shim {err:?}")))
    }
}

macro_rules! impl_service {
    ($($name: tt | $span: literal | $finish: literal | $req: ty | $resp: ty),*) => {
        #[async_trait]
        impl shim_async::Task for TaskService {
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
    state | "ttrpc.task.State" | false | api::StateRequest | api::StateResponse,
    create | "ttrpc.task.Create" | false | api::CreateTaskRequest | api::CreateTaskResponse,
    start | "ttrpc.task.Start" | false | api::StartRequest | api::StartResponse,
    delete | "ttrpc.task.Delete" | false | api::DeleteRequest | api::DeleteResponse,
    pids | "ttrpc.task.Pids" | false | api::PidsRequest | api::PidsResponse,
    pause | "ttrpc.task.Pause" | false | api::PauseRequest | api::Empty,
    resume | "ttrpc.task.Resume" | false | api::ResumeRequest | api::Empty,
    kill | "ttrpc.task.Kill" | false | api::KillRequest | api::Empty,
    exec | "ttrpc.task.Exec" | false | api::ExecProcessRequest | api::Empty,
    resize_pty | "ttrpc.task.ResizePty" | false | api::ResizePtyRequest | api::Empty,
    update | "ttrpc.task.Update" | false | api::UpdateTaskRequest | api::Empty,
    wait | "ttrpc.task.Wait" | false | api::WaitRequest | api::WaitResponse,
    stats | "ttrpc.task.Stats" | false | api::StatsRequest | api::StatsResponse,
    connect | "ttrpc.task.Connect" | false | api::ConnectRequest | api::ConnectResponse,
    shutdown | "ttrpc.task.Shutdown" | true | api::ShutdownRequest | api::Empty,
    close_io | "ttrpc.task.CloseIO" | false | api::CloseIORequest | api::Empty
);
