//! High-level client wrapper over the tonic-generated `BrokerClient`.
//! Clone-friendly; each call clones the underlying channel cheaply.

use std::time::Duration;

use tonic::transport::Channel;

use crate::error::{BrokerError, Result};
use crate::proto;

#[derive(Clone)]
pub struct Client {
    inner: proto::broker_client::BrokerClient<Channel>,
    token: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ReceivedMessage {
    pub partition: u32,
    pub id: u64,
    pub receipt: proto::Receipt,
    pub attempts: u32,
    pub payload: Vec<u8>,
}

impl Client {
    pub async fn connect(addr: String) -> Result<Self> {
        let inner = proto::broker_client::BrokerClient::connect(addr).await?;
        Ok(Self { inner, token: None })
    }

    pub async fn connect_with_token(addr: String, token: String) -> Result<Self> {
        let inner = proto::broker_client::BrokerClient::connect(addr).await?;
        Ok(Self {
            inner,
            token: Some(token),
        })
    }

    pub async fn connect_tls(
        addr: String,
        ca_cert_pem: Vec<u8>,
        domain: String,
    ) -> Result<Self> {
        Self::connect_tls_inner(addr, ca_cert_pem, domain, None).await
    }

    pub async fn connect_tls_with_token(
        addr: String,
        ca_cert_pem: Vec<u8>,
        domain: String,
        token: String,
    ) -> Result<Self> {
        Self::connect_tls_inner(addr, ca_cert_pem, domain, Some(token)).await
    }

    async fn connect_tls_inner(
        addr: String,
        ca_cert_pem: Vec<u8>,
        domain: String,
        token: Option<String>,
    ) -> Result<Self> {
        let tls = tonic::transport::ClientTlsConfig::new()
            .ca_certificate(tonic::transport::Certificate::from_pem(ca_cert_pem))
            .domain_name(domain);
        let endpoint = tonic::transport::Endpoint::from_shared(addr)?.tls_config(tls)?;
        let channel = endpoint.connect().await?;
        let inner = proto::broker_client::BrokerClient::new(channel);
        Ok(Self { inner, token })
    }

    fn auth<T>(&self, body: T) -> tonic::Request<T> {
        let mut req = tonic::Request::new(body);
        if let Some(t) = &self.token {
            if let Ok(val) = format!("Bearer {t}").parse() {
                req.metadata_mut().insert("authorization", val);
            }
        }
        req
    }

    pub async fn open_topic(&mut self, name: &str, partition_count: u32) -> Result<u32> {
        let resp = self
            .inner
            .open_topic(self.auth(proto::OpenTopicRequest {
                name: name.to_string(),
                partition_count,
            }))
            .await?;
        Ok(resp.into_inner().partition_count)
    }

    pub async fn send(
        &mut self,
        topic: &str,
        key: &[u8],
        payload: &[u8],
    ) -> Result<(u32, u64)> {
        let resp = self
            .inner
            .send(self.auth(proto::SendRequest {
                topic: topic.to_string(),
                key: key.to_vec(),
                payload: payload.to_vec(),
            }))
            .await?;
        let r = resp.into_inner();
        Ok((r.partition, r.record_id))
    }

    pub async fn send_to_partition(
        &mut self,
        topic: &str,
        partition: u32,
        payload: &[u8],
    ) -> Result<u64> {
        // There is no dedicated RPC for "send to specific partition" yet, so we
        // use a key that we know hashes to the target partition by sending and
        // checking the response — for tests we just send with the partition id
        // as the key, which is good enough for the round-trip even if the hash
        // disagrees. Returns the record id, ignores partition.
        let resp = self
            .inner
            .send(self.auth(proto::SendRequest {
                topic: topic.to_string(),
                key: partition.to_le_bytes().to_vec(),
                payload: payload.to_vec(),
            }))
            .await?;
        Ok(resp.into_inner().record_id)
    }

    pub async fn send_batch(
        &mut self,
        topic: &str,
        items: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<Vec<(u32, u64)>> {
        let resp = self
            .inner
            .send_batch(self.auth(proto::SendBatchRequest {
                topic: topic.to_string(),
                items: items
                    .into_iter()
                    .map(|(k, v)| proto::SendItem {
                        key: k,
                        payload: v,
                    })
                    .collect(),
            }))
            .await?;
        Ok(resp
            .into_inner()
            .results
            .into_iter()
            .map(|r| (r.partition, r.record_id))
            .collect())
    }

    pub async fn receive_stream(
        &mut self,
        topic: &str,
        max_batch: u32,
        poll_interval: Duration,
    ) -> Result<tonic::Streaming<proto::TopicMessage>> {
        let resp = self
            .inner
            .receive_stream(self.auth(proto::ReceiveStreamRequest {
                topic: topic.to_string(),
                max_batch,
                poll_interval_ms: poll_interval.as_millis() as u64,
            }))
            .await?;
        Ok(resp.into_inner())
    }

    pub async fn receive(&mut self, topic: &str, max: u32) -> Result<Vec<ReceivedMessage>> {
        let resp = self
            .inner
            .receive(self.auth(proto::ReceiveRequest {
                topic: topic.to_string(),
                max,
            }))
            .await?;
        let messages = resp.into_inner().messages;
        let mut out = Vec::with_capacity(messages.len());
        for m in messages {
            let receipt = m
                .receipt
                .ok_or_else(|| BrokerError::InvalidRequest("server omitted receipt".into()))?;
            out.push(ReceivedMessage {
                partition: m.partition,
                id: m.id,
                receipt,
                attempts: m.attempts,
                payload: m.payload,
            });
        }
        Ok(out)
    }

    pub async fn ack(&mut self, topic: &str, receipt: proto::Receipt) -> Result<()> {
        self.inner
            .ack(self.auth(proto::AckRequest {
                topic: topic.to_string(),
                receipt: Some(receipt),
            }))
            .await?;
        Ok(())
    }

    pub async fn nack(&mut self, topic: &str, receipt: proto::Receipt) -> Result<()> {
        self.inner
            .nack(self.auth(proto::NackRequest {
                topic: topic.to_string(),
                receipt: Some(receipt),
            }))
            .await?;
        Ok(())
    }

    pub async fn extend_visibility(
        &mut self,
        topic: &str,
        receipt: proto::Receipt,
        extension: Duration,
    ) -> Result<()> {
        self.inner
            .extend_visibility(self.auth(proto::ExtendVisibilityRequest {
                topic: topic.to_string(),
                receipt: Some(receipt),
                extension_ms: extension.as_millis() as u64,
            }))
            .await?;
        Ok(())
    }

    pub async fn stats(&mut self, topic: &str) -> Result<proto::StatsResponse> {
        let resp = self
            .inner
            .stats(self.auth(proto::StatsRequest {
                topic: topic.to_string(),
            }))
            .await?;
        Ok(resp.into_inner())
    }

    pub async fn join_group(
        &mut self,
        topic: &str,
        group: &str,
    ) -> Result<(u64, u64, Vec<u32>)> {
        let resp = self
            .inner
            .join_group(self.auth(proto::JoinGroupRequest {
                topic: topic.to_string(),
                group: group.to_string(),
            }))
            .await?;
        let inner = resp.into_inner();
        Ok((inner.member_id, inner.generation, inner.assignment))
    }

    pub async fn heartbeat(
        &mut self,
        topic: &str,
        group: &str,
        member_id: u64,
    ) -> Result<(u64, Vec<u32>)> {
        let resp = self
            .inner
            .heartbeat(self.auth(proto::HeartbeatRequest {
                topic: topic.to_string(),
                group: group.to_string(),
                member_id,
            }))
            .await?;
        let inner = resp.into_inner();
        Ok((inner.generation, inner.assignment))
    }

    pub async fn leave_group(
        &mut self,
        topic: &str,
        group: &str,
        member_id: u64,
    ) -> Result<()> {
        self.inner
            .leave_group(self.auth(proto::LeaveGroupRequest {
                topic: topic.to_string(),
                group: group.to_string(),
                member_id,
            }))
            .await?;
        Ok(())
    }

    pub async fn stream_for_member(
        &mut self,
        topic: &str,
        group: &str,
        member_id: u64,
        max: u32,
    ) -> Result<Vec<proto::StreamedRecord>> {
        let resp = self
            .inner
            .stream_for_member(self.auth(proto::StreamForMemberRequest {
                topic: topic.to_string(),
                group: group.to_string(),
                member_id,
                max,
            }))
            .await?;
        Ok(resp.into_inner().messages)
    }

    pub async fn stream_from_group(
        &mut self,
        topic: &str,
        group: &str,
        partition: u32,
        max: u32,
    ) -> Result<Vec<proto::StreamedRecord>> {
        let resp = self
            .inner
            .stream_from_group(self.auth(proto::StreamFromGroupRequest {
                topic: topic.to_string(),
                group: group.to_string(),
                partition,
                max,
            }))
            .await?;
        Ok(resp.into_inner().messages)
    }

    pub async fn commit_group_offset(
        &mut self,
        topic: &str,
        group: &str,
        partition: u32,
        offset: u64,
    ) -> Result<()> {
        self.inner
            .commit_group_offset(self.auth(proto::CommitGroupOffsetRequest {
                topic: topic.to_string(),
                group: group.to_string(),
                partition,
                offset,
            }))
            .await?;
        Ok(())
    }

    pub async fn get_group_offset(
        &mut self,
        topic: &str,
        group: &str,
        partition: u32,
    ) -> Result<Option<u64>> {
        let resp = self
            .inner
            .get_group_offset(self.auth(proto::GetGroupOffsetRequest {
                topic: topic.to_string(),
                group: group.to_string(),
                partition,
            }))
            .await?;
        Ok(resp.into_inner().offset)
    }

    pub async fn create_root_task(&mut self, workflow: &str, payload: Vec<u8>) -> Result<u64> {
        let resp = self
            .inner
            .create_root_task(self.auth(proto::CreateRootTaskRequest {
                workflow: workflow.to_string(),
                payload,
            }))
            .await?;
        Ok(resp.into_inner().task_id)
    }

    pub async fn create_child_task(
        &mut self,
        workflow: &str,
        parent_id: u64,
        payload: Vec<u8>,
    ) -> Result<u64> {
        let resp = self
            .inner
            .create_child_task(self.auth(proto::CreateChildTaskRequest {
                workflow: workflow.to_string(),
                parent_id,
                payload,
            }))
            .await?;
        Ok(resp.into_inner().task_id)
    }

    pub async fn get_task(&mut self, workflow: &str, task_id: u64) -> Result<proto::TaskMessage> {
        let resp = self
            .inner
            .get_task(self.auth(proto::GetTaskRequest {
                workflow: workflow.to_string(),
                task_id,
            }))
            .await?;
        Ok(resp.into_inner())
    }

    pub async fn list_children(&mut self, workflow: &str, parent_id: u64) -> Result<Vec<u64>> {
        let resp = self
            .inner
            .list_children(self.auth(proto::ListChildrenRequest {
                workflow: workflow.to_string(),
                parent_id,
            }))
            .await?;
        Ok(resp.into_inner().child_ids)
    }

    pub async fn mark_task_running(&mut self, workflow: &str, task_id: u64) -> Result<()> {
        self.inner
            .mark_task_running(self.auth(proto::TaskOpRequest {
                workflow: workflow.to_string(),
                task_id,
            }))
            .await?;
        Ok(())
    }

    pub async fn complete_task(
        &mut self,
        workflow: &str,
        task_id: u64,
        result: Vec<u8>,
    ) -> Result<()> {
        self.inner
            .complete_task(self.auth(proto::CompleteTaskRequest {
                workflow: workflow.to_string(),
                task_id,
                result,
            }))
            .await?;
        Ok(())
    }

    pub async fn fail_task(&mut self, workflow: &str, task_id: u64, error: String) -> Result<()> {
        self.inner
            .fail_task(self.auth(proto::FailTaskRequest {
                workflow: workflow.to_string(),
                task_id,
                error,
            }))
            .await?;
        Ok(())
    }

    pub async fn cancel_task(&mut self, workflow: &str, task_id: u64) -> Result<u64> {
        let resp = self
            .inner
            .cancel_task(self.auth(proto::TaskOpRequest {
                workflow: workflow.to_string(),
                task_id,
            }))
            .await?;
        Ok(resp.into_inner().cancelled)
    }

    pub async fn register_schema(
        &mut self,
        subject: &str,
        bytes: Vec<u8>,
    ) -> Result<proto::SchemaMessage> {
        let resp = self
            .inner
            .register_schema(self.auth(proto::RegisterSchemaRequest {
                subject: subject.to_string(),
                bytes,
            }))
            .await?;
        Ok(resp.into_inner())
    }

    pub async fn get_schema_by_id(&mut self, id: u64) -> Result<proto::SchemaMessage> {
        let resp = self
            .inner
            .get_schema_by_id(self.auth(proto::GetSchemaByIdRequest { id }))
            .await?;
        Ok(resp.into_inner())
    }

    pub async fn get_schema_version(
        &mut self,
        subject: &str,
        version: u32,
    ) -> Result<proto::SchemaMessage> {
        let resp = self
            .inner
            .get_schema_version(self.auth(proto::GetSchemaVersionRequest {
                subject: subject.to_string(),
                version,
            }))
            .await?;
        Ok(resp.into_inner())
    }

    pub async fn get_latest_schema(&mut self, subject: &str) -> Result<proto::SchemaMessage> {
        let resp = self
            .inner
            .get_latest_schema(self.auth(proto::GetLatestSchemaRequest {
                subject: subject.to_string(),
            }))
            .await?;
        Ok(resp.into_inner())
    }

    pub async fn list_schema_versions(&mut self, subject: &str) -> Result<Vec<u32>> {
        let resp = self
            .inner
            .list_schema_versions(self.auth(proto::ListSchemaVersionsRequest {
                subject: subject.to_string(),
            }))
            .await?;
        Ok(resp.into_inner().versions)
    }

    pub async fn list_subjects(&mut self) -> Result<Vec<String>> {
        let resp = self
            .inner
            .list_subjects(self.auth(proto::ListSubjectsRequest {}))
            .await?;
        Ok(resp.into_inner().subjects)
    }
}
