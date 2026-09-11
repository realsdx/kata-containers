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
    ($($name: tt | $span: literal $(, $field:ident)* | $req: ty | $resp: ty),*) => {
        #[async_trait]
        impl shim_async::Task for TaskService {
            $(async fn $name(&self, ctx: &TtrpcContext, req: $req) -> ttrpc::Result<$resp> {
                let request_span = self.handler.trace_parent().await.map_or_else(Span::none, |parent| {
                    info_span!(parent: &parent, $span, container_id = %req.id,
                        $($field = %req.$field,)* success = tracing::field::Empty)
                });
                let result = self.handler_message(ctx, req).instrument(request_span.clone()).await;
                request_span.record("success", result.is_ok());
                result
            })*
        }
    };
}

impl_service!(
    state | "ttrpc.task.State" | api::StateRequest | api::StateResponse,
    create | "ttrpc.task.Create" | api::CreateTaskRequest | api::CreateTaskResponse,
    start | "ttrpc.task.Start",
    exec_id | api::StartRequest | api::StartResponse,
    delete | "ttrpc.task.Delete",
    exec_id | api::DeleteRequest | api::DeleteResponse,
    pids | "ttrpc.task.Pids" | api::PidsRequest | api::PidsResponse,
    pause | "ttrpc.task.Pause" | api::PauseRequest | api::Empty,
    resume | "ttrpc.task.Resume" | api::ResumeRequest | api::Empty,
    kill | "ttrpc.task.Kill",
    exec_id | api::KillRequest | api::Empty,
    exec | "ttrpc.task.Exec",
    exec_id | api::ExecProcessRequest | api::Empty,
    resize_pty | "ttrpc.task.ResizePty" | api::ResizePtyRequest | api::Empty,
    update | "ttrpc.task.Update" | api::UpdateTaskRequest | api::Empty,
    wait | "ttrpc.task.Wait",
    exec_id | api::WaitRequest | api::WaitResponse,
    stats | "ttrpc.task.Stats" | api::StatsRequest | api::StatsResponse,
    connect | "ttrpc.task.Connect" | api::ConnectRequest | api::ConnectResponse,
    shutdown | "ttrpc.task.Shutdown" | api::ShutdownRequest | api::Empty,
    close_io | "ttrpc.task.CloseIO" | api::CloseIORequest | api::Empty
);
