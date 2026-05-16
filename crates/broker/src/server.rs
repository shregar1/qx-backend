//! gRPC server: `Broker` holds a topic map, `BrokerSvc` implements the
//! tonic-generated trait by spawning `Topic` calls on the blocking pool
//! (since `Topic`'s API is sync and does fsync).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use qx_queue::{QueueConfig, ReceiptHandle};
use qx_storage::PartitionConfig;
use qx_topic::{Topic, TopicConfig, TopicError, TopicReceipt};
use qx_schema_registry::{Registry, RegistryError, Schema};
use qx_workflow::{Engine, Task, TaskStatus, WorkflowError};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use crate::error::Result;
use crate::proto;
use crate::proto::broker_server::{Broker as BrokerSvcTrait, BrokerServer};

#[derive(Debug, Default, Clone)]
pub struct BrokerConfig {
    pub auth_token: Option<String>,
    pub tls: Option<TlsConfig>,
}

#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// PEM-encoded server certificate chain.
    pub cert_pem: Vec<u8>,
    /// PEM-encoded server private key.
    pub key_pem: Vec<u8>,
}

type GroupKey = (String, String);
type SharedGroup = Arc<Mutex<crate::groups::ConsumerGroup>>;

pub struct Broker {
    dir: PathBuf,
    topics: Mutex<HashMap<String, Arc<Mutex<Topic>>>>,
    workflows: Mutex<HashMap<String, Arc<Mutex<Engine>>>>,
    groups: Mutex<HashMap<GroupKey, SharedGroup>>,
    schemas: Mutex<Option<Arc<Registry>>>,
    config: BrokerConfig,
}

impl Broker {
    pub fn open(dir: impl AsRef<Path>) -> Arc<Self> {
        Self::open_with_config(dir, BrokerConfig::default())
    }

    pub fn open_with_config(dir: impl AsRef<Path>, config: BrokerConfig) -> Arc<Self> {
        Arc::new(Self {
            dir: dir.as_ref().to_path_buf(),
            topics: Mutex::new(HashMap::new()),
            workflows: Mutex::new(HashMap::new()),
            groups: Mutex::new(HashMap::new()),
            schemas: Mutex::new(None),
            config,
        })
    }

    fn get_or_init_schemas(&self) -> Result<Arc<Registry>> {
        let mut schemas = self.schemas.lock().unwrap();
        if let Some(r) = schemas.as_ref() {
            return Ok(r.clone());
        }
        let dir = self.dir.join("schema-registry");
        std::fs::create_dir_all(&dir)?;
        let r = Arc::new(Registry::open(dir.join("schemas.redb"))?);
        *schemas = Some(r.clone());
        Ok(r)
    }

    fn get_or_create_group(
        &self,
        topic_name: &str,
        group_name: &str,
    ) -> Result<SharedGroup> {
        let key = (topic_name.to_string(), group_name.to_string());
        let mut groups = self.groups.lock().unwrap();
        if let Some(g) = groups.get(&key) {
            return Ok(g.clone());
        }
        // Need partition count from the topic
        let topic = self
            .lookup(topic_name)
            .ok_or_else(|| crate::error::BrokerError::TopicNotFound(topic_name.to_string()))?;
        let partition_count = topic.lock().unwrap().partition_count();
        let group = Arc::new(Mutex::new(crate::groups::ConsumerGroup::new(
            partition_count,
        )));
        groups.insert(key, group.clone());
        Ok(group)
    }

    fn lookup_group(&self, topic_name: &str, group_name: &str) -> Option<SharedGroup> {
        self.groups
            .lock()
            .unwrap()
            .get(&(topic_name.to_string(), group_name.to_string()))
            .cloned()
    }

    fn get_or_open_workflow(&self, name: &str) -> Result<Arc<Mutex<Engine>>> {
        let mut wfs = self.workflows.lock().unwrap();
        if let Some(arc) = wfs.get(name) {
            return Ok(arc.clone());
        }
        let dir = self.dir.join("workflows");
        std::fs::create_dir_all(&dir)?;
        let path = dir.join(format!("{name}.redb"));
        let engine = Engine::open(path)?;
        let arc = Arc::new(Mutex::new(engine));
        wfs.insert(name.to_string(), arc.clone());
        Ok(arc)
    }

    pub async fn serve(self: Arc<Self>, addr: SocketAddr) -> Result<()> {
        let listener = TcpListener::bind(addr).await?;
        self.serve_with_listener(listener).await
    }

    pub async fn serve_with_listener(
        self: Arc<Self>,
        listener: TcpListener,
    ) -> Result<()> {
        let svc = BrokerSvc {
            broker: self.clone(),
        };
        let expected_token = self.config.auth_token.clone();
        #[allow(clippy::result_large_err)]
        let interceptor = move |req: tonic::Request<()>| -> std::result::Result<tonic::Request<()>, Status> {
            check_auth(&expected_token, &req)?;
            Ok(req)
        };
        let stream = TcpListenerStream::new(listener);
        let mut builder = Server::builder();
        if let Some(tls) = &self.config.tls {
            let identity =
                tonic::transport::Identity::from_pem(&tls.cert_pem, &tls.key_pem);
            builder = builder
                .tls_config(tonic::transport::ServerTlsConfig::new().identity(identity))?;
        }
        builder
            .add_service(BrokerServer::with_interceptor(svc, interceptor))
            .serve_with_incoming(stream)
            .await?;
        Ok(())
    }

    pub async fn serve_metrics(self: Arc<Self>, addr: SocketAddr) -> Result<()> {
        let listener = TcpListener::bind(addr).await?;
        self.serve_metrics_with_listener(listener).await
    }

    pub async fn serve_metrics_with_listener(
        self: Arc<Self>,
        listener: TcpListener,
    ) -> Result<()> {
        loop {
            let (stream, _) = listener.accept().await?;
            let broker = self.clone();
            tokio::spawn(async move {
                let _ = handle_metrics_connection(broker, stream).await;
            });
        }
    }

    pub fn all_stats(&self) -> Result<Vec<(String, qx_topic::TopicStats)>> {
        let snapshots: Vec<_> = {
            let topics = self.topics.lock().unwrap();
            topics.iter().map(|(n, a)| (n.clone(), a.clone())).collect()
        };
        let mut out = Vec::with_capacity(snapshots.len());
        for (name, arc) in snapshots {
            let stats = arc.lock().unwrap().stats()?;
            out.push((name, stats));
        }
        Ok(out)
    }

    fn get_or_open(&self, name: &str, partition_count: u32) -> Result<u32> {
        let mut topics = self.topics.lock().unwrap();
        if let Some(arc) = topics.get(name) {
            let t = arc.lock().unwrap();
            return Ok(t.partition_count());
        }
        let dir = self.dir.join(name);
        let cfg = TopicConfig {
            partition_count,
            queue: QueueConfig {
                partition: PartitionConfig::default(),
                visibility_timeout: Duration::from_secs(30),
                max_attempts: 5,
                durable_send: false,
            },
        };
        let topic = Topic::open(&dir, cfg)?;
        let count = topic.partition_count();
        topics.insert(name.to_string(), Arc::new(Mutex::new(topic)));
        Ok(count)
    }

    fn lookup(&self, name: &str) -> Option<Arc<Mutex<Topic>>> {
        self.topics.lock().unwrap().get(name).cloned()
    }
}

struct BrokerSvc {
    broker: Arc<Broker>,
}

impl BrokerSvc {
    #[allow(clippy::result_large_err)]
    fn lookup_or_not_found(&self, topic: &str) -> std::result::Result<Arc<Mutex<Topic>>, Status> {
        self.broker
            .lookup(topic)
            .ok_or_else(|| Status::not_found(format!("topic '{topic}' not found")))
    }
}

#[tonic::async_trait]
impl BrokerSvcTrait for BrokerSvc {
    async fn open_topic(
        &self,
        req: Request<proto::OpenTopicRequest>,
    ) -> std::result::Result<Response<proto::OpenTopicResponse>, Status> {
        let req = req.into_inner();
        let broker = self.broker.clone();
        let count = tokio::task::spawn_blocking(move || {
            broker.get_or_open(&req.name, req.partition_count)
        })
        .await
        .map_err(join_err)?
        .map_err(|e| match e {
            crate::error::BrokerError::Topic(te) => topic_err_to_status(te),
            other => Status::internal(other.to_string()),
        })?;
        Ok(Response::new(proto::OpenTopicResponse {
            partition_count: count,
        }))
    }

    async fn send(
        &self,
        req: Request<proto::SendRequest>,
    ) -> std::result::Result<Response<proto::SendResponse>, Status> {
        let req = req.into_inner();
        let topic_mu = self.lookup_or_not_found(&req.topic)?;
        let result = tokio::task::spawn_blocking(move || {
            let mut t = topic_mu.lock().unwrap();
            t.send(&req.key, &req.payload)
        })
        .await
        .map_err(join_err)?
        .map_err(topic_err_to_status)?;
        Ok(Response::new(proto::SendResponse {
            partition: result.0,
            record_id: result.1,
        }))
    }

    async fn send_batch(
        &self,
        req: Request<proto::SendBatchRequest>,
    ) -> std::result::Result<Response<proto::SendBatchResponse>, Status> {
        let req = req.into_inner();
        let topic_mu = self.lookup_or_not_found(&req.topic)?;
        let items: Vec<(Vec<u8>, Vec<u8>)> = req
            .items
            .into_iter()
            .map(|i| (i.key, i.payload))
            .collect();
        let results = tokio::task::spawn_blocking(move || {
            let mut t = topic_mu.lock().unwrap();
            let pairs: Vec<(&[u8], &[u8])> =
                items.iter().map(|(k, v)| (k.as_slice(), v.as_slice())).collect();
            t.send_batch(&pairs)
        })
        .await
        .map_err(join_err)?
        .map_err(topic_err_to_status)?;
        Ok(Response::new(proto::SendBatchResponse {
            results: results
                .into_iter()
                .map(|(p, id)| proto::SendResponse {
                    partition: p,
                    record_id: id,
                })
                .collect(),
        }))
    }

    type ReceiveStreamStream = std::pin::Pin<
        Box<dyn tokio_stream::Stream<Item = std::result::Result<proto::TopicMessage, Status>> + Send + 'static>,
    >;

    async fn receive_stream(
        &self,
        req: Request<proto::ReceiveStreamRequest>,
    ) -> std::result::Result<Response<Self::ReceiveStreamStream>, Status> {
        let req = req.into_inner();
        let topic_mu = self.lookup_or_not_found(&req.topic)?;
        let max_batch = (req.max_batch.max(1) as usize).min(1024);
        let poll = if req.poll_interval_ms == 0 {
            Duration::from_millis(50)
        } else {
            Duration::from_millis(req.poll_interval_ms)
        };

        let (tx, rx) = tokio::sync::mpsc::channel::<std::result::Result<proto::TopicMessage, Status>>(64);
        tokio::spawn(async move {
            loop {
                if tx.is_closed() {
                    return;
                }
                let topic_mu = topic_mu.clone();
                let result = tokio::task::spawn_blocking(move || {
                    let mut t = topic_mu.lock().unwrap();
                    t.receive(max_batch)
                })
                .await;
                match result {
                    Ok(Ok(messages)) if messages.is_empty() => {
                        tokio::time::sleep(poll).await;
                    }
                    Ok(Ok(messages)) => {
                        for msg in messages {
                            if tx.send(Ok(proto_message(msg))).await.is_err() {
                                return;
                            }
                        }
                    }
                    Ok(Err(e)) => {
                        let _ = tx.send(Err(topic_err_to_status(e))).await;
                        return;
                    }
                    Err(e) => {
                        let _ = tx.send(Err(Status::internal(format!("blocking: {e}")))).await;
                        return;
                    }
                }
            }
        });
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(stream) as Self::ReceiveStreamStream))
    }

    async fn receive(
        &self,
        req: Request<proto::ReceiveRequest>,
    ) -> std::result::Result<Response<proto::ReceiveResponse>, Status> {
        let req = req.into_inner();
        let topic_mu = self.lookup_or_not_found(&req.topic)?;
        let max = req.max as usize;
        let messages = tokio::task::spawn_blocking(move || {
            let mut t = topic_mu.lock().unwrap();
            t.receive(max)
        })
        .await
        .map_err(join_err)?
        .map_err(topic_err_to_status)?;
        Ok(Response::new(proto::ReceiveResponse {
            messages: messages.into_iter().map(proto_message).collect(),
        }))
    }

    async fn ack(
        &self,
        req: Request<proto::AckRequest>,
    ) -> std::result::Result<Response<proto::AckResponse>, Status> {
        let req = req.into_inner();
        let topic_mu = self.lookup_or_not_found(&req.topic)?;
        let receipt = req
            .receipt
            .ok_or_else(|| Status::invalid_argument("missing receipt"))?;
        let r = from_proto_receipt(receipt);
        tokio::task::spawn_blocking(move || {
            let mut t = topic_mu.lock().unwrap();
            t.ack(r)
        })
        .await
        .map_err(join_err)?
        .map_err(topic_err_to_status)?;
        Ok(Response::new(proto::AckResponse {}))
    }

    async fn nack(
        &self,
        req: Request<proto::NackRequest>,
    ) -> std::result::Result<Response<proto::NackResponse>, Status> {
        let req = req.into_inner();
        let topic_mu = self.lookup_or_not_found(&req.topic)?;
        let receipt = req
            .receipt
            .ok_or_else(|| Status::invalid_argument("missing receipt"))?;
        let r = from_proto_receipt(receipt);
        tokio::task::spawn_blocking(move || {
            let mut t = topic_mu.lock().unwrap();
            t.nack(r)
        })
        .await
        .map_err(join_err)?
        .map_err(topic_err_to_status)?;
        Ok(Response::new(proto::NackResponse {}))
    }

    async fn extend_visibility(
        &self,
        req: Request<proto::ExtendVisibilityRequest>,
    ) -> std::result::Result<Response<proto::ExtendVisibilityResponse>, Status> {
        let req = req.into_inner();
        let topic_mu = self.lookup_or_not_found(&req.topic)?;
        let receipt = req
            .receipt
            .ok_or_else(|| Status::invalid_argument("missing receipt"))?;
        let r = from_proto_receipt(receipt);
        let ext = Duration::from_millis(req.extension_ms);
        tokio::task::spawn_blocking(move || {
            let mut t = topic_mu.lock().unwrap();
            t.extend_visibility(r, ext)
        })
        .await
        .map_err(join_err)?
        .map_err(topic_err_to_status)?;
        Ok(Response::new(proto::ExtendVisibilityResponse {}))
    }

    async fn join_group(
        &self,
        req: Request<proto::JoinGroupRequest>,
    ) -> std::result::Result<Response<proto::JoinGroupResponse>, Status> {
        let req = req.into_inner();
        let broker = self.broker.clone();
        let (member_id, generation, assignment) = tokio::task::spawn_blocking(move || {
            let g = broker.get_or_create_group(&req.topic, &req.group)?;
            let (id, generation, assignment) = g.lock().unwrap().join();
            Ok::<_, crate::error::BrokerError>((id, generation, assignment))
        })
        .await
        .map_err(join_err)?
        .map_err(broker_err_to_status)?;
        Ok(Response::new(proto::JoinGroupResponse {
            member_id,
            generation,
            assignment,
        }))
    }

    async fn heartbeat(
        &self,
        req: Request<proto::HeartbeatRequest>,
    ) -> std::result::Result<Response<proto::HeartbeatResponse>, Status> {
        let req = req.into_inner();
        let group = self.broker.lookup_group(&req.topic, &req.group).ok_or_else(|| {
            Status::not_found(format!("group '{}' for topic '{}'", req.group, req.topic))
        })?;
        let (generation, assignment) = tokio::task::spawn_blocking(move || {
            let mut g = group.lock().unwrap();
            g.heartbeat(req.member_id)
        })
        .await
        .map_err(join_err)?
        .ok_or_else(|| Status::not_found("unknown member id"))?;
        Ok(Response::new(proto::HeartbeatResponse {
            generation,
            assignment,
        }))
    }

    async fn leave_group(
        &self,
        req: Request<proto::LeaveGroupRequest>,
    ) -> std::result::Result<Response<proto::LeaveGroupResponse>, Status> {
        let req = req.into_inner();
        if let Some(group) = self.broker.lookup_group(&req.topic, &req.group) {
            tokio::task::spawn_blocking(move || {
                group.lock().unwrap().leave(req.member_id);
            })
            .await
            .map_err(join_err)?;
        }
        Ok(Response::new(proto::LeaveGroupResponse {}))
    }

    async fn stream_for_member(
        &self,
        req: Request<proto::StreamForMemberRequest>,
    ) -> std::result::Result<Response<proto::StreamFromGroupResponse>, Status> {
        let req = req.into_inner();
        let topic_mu = self.lookup_or_not_found(&req.topic)?;
        let group = self
            .broker
            .lookup_group(&req.topic, &req.group)
            .ok_or_else(|| Status::not_found("group not joined"))?;
        let assignment = group
            .lock()
            .unwrap()
            .assignment(req.member_id)
            .ok_or_else(|| Status::not_found("unknown member id"))?;
        let max = req.max as usize;
        let group_name = req.group.clone();
        let messages = tokio::task::spawn_blocking(move || {
            let mut t = topic_mu.lock().unwrap();
            let mut out = Vec::new();
            // Fair across assigned partitions: ceil(max / n).
            let per_partition = if assignment.is_empty() {
                0
            } else {
                max.div_ceil(assignment.len())
            };
            for partition in &assignment {
                if out.len() >= max {
                    break;
                }
                let take = (max - out.len()).min(per_partition);
                let msgs = t.stream(&group_name, *partition, take)?;
                for m in msgs {
                    out.push(m);
                }
            }
            Ok::<_, qx_topic::TopicError>(out)
        })
        .await
        .map_err(join_err)?
        .map_err(topic_err_to_status)?;
        Ok(Response::new(proto::StreamFromGroupResponse {
            messages: messages
                .into_iter()
                .map(|m| proto::StreamedRecord {
                    partition: m.partition,
                    id: m.id,
                    payload: m.payload,
                })
                .collect(),
        }))
    }

    async fn stream_from_group(
        &self,
        req: Request<proto::StreamFromGroupRequest>,
    ) -> std::result::Result<Response<proto::StreamFromGroupResponse>, Status> {
        let req = req.into_inner();
        let topic_mu = self.lookup_or_not_found(&req.topic)?;
        let partition = req.partition;
        let max = req.max as usize;
        let group = req.group;
        let messages = tokio::task::spawn_blocking(move || {
            let mut t = topic_mu.lock().unwrap();
            t.stream(&group, partition, max)
        })
        .await
        .map_err(join_err)?
        .map_err(topic_err_to_status)?;
        Ok(Response::new(proto::StreamFromGroupResponse {
            messages: messages
                .into_iter()
                .map(|m| proto::StreamedRecord {
                    partition: m.partition,
                    id: m.id,
                    payload: m.payload,
                })
                .collect(),
        }))
    }

    async fn commit_group_offset(
        &self,
        req: Request<proto::CommitGroupOffsetRequest>,
    ) -> std::result::Result<Response<proto::CommitGroupOffsetResponse>, Status> {
        let req = req.into_inner();
        let topic_mu = self.lookup_or_not_found(&req.topic)?;
        let partition = req.partition;
        let offset = req.offset;
        let group = req.group;
        tokio::task::spawn_blocking(move || {
            let mut t = topic_mu.lock().unwrap();
            t.commit_offset(&group, partition, offset)
        })
        .await
        .map_err(join_err)?
        .map_err(topic_err_to_status)?;
        Ok(Response::new(proto::CommitGroupOffsetResponse {}))
    }

    async fn get_group_offset(
        &self,
        req: Request<proto::GetGroupOffsetRequest>,
    ) -> std::result::Result<Response<proto::GetGroupOffsetResponse>, Status> {
        let req = req.into_inner();
        let topic_mu = self.lookup_or_not_found(&req.topic)?;
        let partition = req.partition;
        let group = req.group;
        let offset = tokio::task::spawn_blocking(move || {
            let t = topic_mu.lock().unwrap();
            t.group_offset(&group, partition)
        })
        .await
        .map_err(join_err)?
        .map_err(topic_err_to_status)?;
        Ok(Response::new(proto::GetGroupOffsetResponse { offset }))
    }

    async fn create_root_task(
        &self,
        req: Request<proto::CreateRootTaskRequest>,
    ) -> std::result::Result<Response<proto::CreateTaskResponse>, Status> {
        let req = req.into_inner();
        let broker = self.broker.clone();
        let task_id = tokio::task::spawn_blocking(move || {
            let wf = broker.get_or_open_workflow(&req.workflow)?;
            let mut e = wf.lock().unwrap();
            Ok::<u64, crate::error::BrokerError>(e.create_root(&req.payload)?)
        })
        .await
        .map_err(join_err)?
        .map_err(broker_err_to_status)?;
        Ok(Response::new(proto::CreateTaskResponse { task_id }))
    }

    async fn create_child_task(
        &self,
        req: Request<proto::CreateChildTaskRequest>,
    ) -> std::result::Result<Response<proto::CreateTaskResponse>, Status> {
        let req = req.into_inner();
        let broker = self.broker.clone();
        let task_id = tokio::task::spawn_blocking(move || {
            let wf = broker.get_or_open_workflow(&req.workflow)?;
            let mut e = wf.lock().unwrap();
            Ok::<u64, crate::error::BrokerError>(e.create_child(req.parent_id, &req.payload)?)
        })
        .await
        .map_err(join_err)?
        .map_err(broker_err_to_status)?;
        Ok(Response::new(proto::CreateTaskResponse { task_id }))
    }

    async fn get_task(
        &self,
        req: Request<proto::GetTaskRequest>,
    ) -> std::result::Result<Response<proto::TaskMessage>, Status> {
        let req = req.into_inner();
        let broker = self.broker.clone();
        let task = tokio::task::spawn_blocking(move || {
            let wf = broker.get_or_open_workflow(&req.workflow)?;
            let e = wf.lock().unwrap();
            Ok::<Option<Task>, crate::error::BrokerError>(e.get(req.task_id)?)
        })
        .await
        .map_err(join_err)?
        .map_err(broker_err_to_status)?
        .ok_or_else(|| Status::not_found(format!("task {}", req.task_id)))?;
        Ok(Response::new(task_to_proto(task)))
    }

    async fn list_children(
        &self,
        req: Request<proto::ListChildrenRequest>,
    ) -> std::result::Result<Response<proto::ListChildrenResponse>, Status> {
        let req = req.into_inner();
        let broker = self.broker.clone();
        let kids = tokio::task::spawn_blocking(move || {
            let wf = broker.get_or_open_workflow(&req.workflow)?;
            let e = wf.lock().unwrap();
            Ok::<Vec<u64>, crate::error::BrokerError>(e.children(req.parent_id)?)
        })
        .await
        .map_err(join_err)?
        .map_err(broker_err_to_status)?;
        Ok(Response::new(proto::ListChildrenResponse { child_ids: kids }))
    }

    async fn mark_task_running(
        &self,
        req: Request<proto::TaskOpRequest>,
    ) -> std::result::Result<Response<proto::TaskOpResponse>, Status> {
        let req = req.into_inner();
        let broker = self.broker.clone();
        tokio::task::spawn_blocking(move || {
            let wf = broker.get_or_open_workflow(&req.workflow)?;
            let mut e = wf.lock().unwrap();
            e.mark_running(req.task_id)?;
            Ok::<(), crate::error::BrokerError>(())
        })
        .await
        .map_err(join_err)?
        .map_err(broker_err_to_status)?;
        Ok(Response::new(proto::TaskOpResponse {}))
    }

    async fn complete_task(
        &self,
        req: Request<proto::CompleteTaskRequest>,
    ) -> std::result::Result<Response<proto::TaskOpResponse>, Status> {
        let req = req.into_inner();
        let broker = self.broker.clone();
        tokio::task::spawn_blocking(move || {
            let wf = broker.get_or_open_workflow(&req.workflow)?;
            let mut e = wf.lock().unwrap();
            e.complete(req.task_id, req.result)?;
            Ok::<(), crate::error::BrokerError>(())
        })
        .await
        .map_err(join_err)?
        .map_err(broker_err_to_status)?;
        Ok(Response::new(proto::TaskOpResponse {}))
    }

    async fn fail_task(
        &self,
        req: Request<proto::FailTaskRequest>,
    ) -> std::result::Result<Response<proto::TaskOpResponse>, Status> {
        let req = req.into_inner();
        let broker = self.broker.clone();
        tokio::task::spawn_blocking(move || {
            let wf = broker.get_or_open_workflow(&req.workflow)?;
            let mut e = wf.lock().unwrap();
            e.fail(req.task_id, req.error)?;
            Ok::<(), crate::error::BrokerError>(())
        })
        .await
        .map_err(join_err)?
        .map_err(broker_err_to_status)?;
        Ok(Response::new(proto::TaskOpResponse {}))
    }

    async fn cancel_task(
        &self,
        req: Request<proto::TaskOpRequest>,
    ) -> std::result::Result<Response<proto::CancelTaskResponse>, Status> {
        let req = req.into_inner();
        let broker = self.broker.clone();
        let cancelled = tokio::task::spawn_blocking(move || {
            let wf = broker.get_or_open_workflow(&req.workflow)?;
            let mut e = wf.lock().unwrap();
            Ok::<usize, crate::error::BrokerError>(e.cancel(req.task_id)?)
        })
        .await
        .map_err(join_err)?
        .map_err(broker_err_to_status)?;
        Ok(Response::new(proto::CancelTaskResponse {
            cancelled: cancelled as u64,
        }))
    }

    async fn register_schema(
        &self,
        req: Request<proto::RegisterSchemaRequest>,
    ) -> std::result::Result<Response<proto::SchemaMessage>, Status> {
        let req = req.into_inner();
        let broker = self.broker.clone();
        let schema = tokio::task::spawn_blocking(move || {
            let reg = broker.get_or_init_schemas()?;
            Ok::<_, crate::error::BrokerError>(reg.register(&req.subject, &req.bytes)?)
        })
        .await
        .map_err(join_err)?
        .map_err(broker_err_to_status)?;
        Ok(Response::new(schema_to_proto(schema)))
    }

    async fn get_schema_by_id(
        &self,
        req: Request<proto::GetSchemaByIdRequest>,
    ) -> std::result::Result<Response<proto::SchemaMessage>, Status> {
        let req = req.into_inner();
        let broker = self.broker.clone();
        let schema = tokio::task::spawn_blocking(move || {
            let reg = broker.get_or_init_schemas()?;
            Ok::<_, crate::error::BrokerError>(reg.get_by_id(req.id)?)
        })
        .await
        .map_err(join_err)?
        .map_err(broker_err_to_status)?;
        Ok(Response::new(schema_to_proto(schema)))
    }

    async fn get_schema_version(
        &self,
        req: Request<proto::GetSchemaVersionRequest>,
    ) -> std::result::Result<Response<proto::SchemaMessage>, Status> {
        let req = req.into_inner();
        let broker = self.broker.clone();
        let schema = tokio::task::spawn_blocking(move || {
            let reg = broker.get_or_init_schemas()?;
            Ok::<_, crate::error::BrokerError>(reg.get_version(&req.subject, req.version)?)
        })
        .await
        .map_err(join_err)?
        .map_err(broker_err_to_status)?;
        Ok(Response::new(schema_to_proto(schema)))
    }

    async fn get_latest_schema(
        &self,
        req: Request<proto::GetLatestSchemaRequest>,
    ) -> std::result::Result<Response<proto::SchemaMessage>, Status> {
        let req = req.into_inner();
        let broker = self.broker.clone();
        let schema = tokio::task::spawn_blocking(move || {
            let reg = broker.get_or_init_schemas()?;
            Ok::<_, crate::error::BrokerError>(reg.get_latest(&req.subject)?)
        })
        .await
        .map_err(join_err)?
        .map_err(broker_err_to_status)?;
        Ok(Response::new(schema_to_proto(schema)))
    }

    async fn list_schema_versions(
        &self,
        req: Request<proto::ListSchemaVersionsRequest>,
    ) -> std::result::Result<Response<proto::ListSchemaVersionsResponse>, Status> {
        let req = req.into_inner();
        let broker = self.broker.clone();
        let versions = tokio::task::spawn_blocking(move || {
            let reg = broker.get_or_init_schemas()?;
            Ok::<_, crate::error::BrokerError>(reg.list_versions(&req.subject)?)
        })
        .await
        .map_err(join_err)?
        .map_err(broker_err_to_status)?;
        Ok(Response::new(proto::ListSchemaVersionsResponse { versions }))
    }

    async fn list_subjects(
        &self,
        _req: Request<proto::ListSubjectsRequest>,
    ) -> std::result::Result<Response<proto::ListSubjectsResponse>, Status> {
        let broker = self.broker.clone();
        let subjects = tokio::task::spawn_blocking(move || {
            let reg = broker.get_or_init_schemas()?;
            Ok::<_, crate::error::BrokerError>(reg.list_subjects()?)
        })
        .await
        .map_err(join_err)?
        .map_err(broker_err_to_status)?;
        Ok(Response::new(proto::ListSubjectsResponse { subjects }))
    }

    async fn stats(
        &self,
        req: Request<proto::StatsRequest>,
    ) -> std::result::Result<Response<proto::StatsResponse>, Status> {
        let req = req.into_inner();
        let topic_mu = self.lookup_or_not_found(&req.topic)?;
        let stats = tokio::task::spawn_blocking(move || {
            let t = topic_mu.lock().unwrap();
            t.stats()
        })
        .await
        .map_err(join_err)?
        .map_err(topic_err_to_status)?;
        Ok(Response::new(proto::StatsResponse {
            partition_count: stats.partition_count,
            pending: stats.pending,
            inflight: stats.inflight,
            dead: stats.dead,
        }))
    }
}

fn proto_message(m: qx_topic::TopicMessage) -> proto::TopicMessage {
    proto::TopicMessage {
        partition: m.partition,
        id: m.id,
        receipt: Some(proto::Receipt {
            partition: m.receipt.partition,
            record_id: m.receipt.inner.record_id,
            attempt: m.receipt.inner.attempt,
        }),
        attempts: m.attempts,
        payload: m.payload,
    }
}

fn from_proto_receipt(r: proto::Receipt) -> TopicReceipt {
    TopicReceipt {
        partition: r.partition,
        inner: ReceiptHandle {
            record_id: r.record_id,
            attempt: r.attempt,
        },
    }
}

fn topic_err_to_status(e: TopicError) -> Status {
    use TopicError::*;
    match &e {
        InvalidPartition { .. } | ZeroPartitions | PartitionCountMismatch { .. } => {
            Status::invalid_argument(e.to_string())
        }
        _ => Status::internal(e.to_string()),
    }
}

fn broker_err_to_status(e: crate::error::BrokerError) -> Status {
    use crate::error::BrokerError;
    match e {
        BrokerError::Topic(te) => topic_err_to_status(te),
        BrokerError::Workflow(WorkflowError::TaskNotFound { id }) => {
            Status::not_found(format!("task {id}"))
        }
        BrokerError::Workflow(WorkflowError::TerminalTask { id }) => {
            Status::failed_precondition(format!("task {id} is terminal"))
        }
        BrokerError::SchemaRegistry(RegistryError::SubjectNotFound(s)) => {
            Status::not_found(format!("subject {s}"))
        }
        BrokerError::SchemaRegistry(RegistryError::SchemaIdNotFound(id)) => {
            Status::not_found(format!("schema id {id}"))
        }
        BrokerError::SchemaRegistry(RegistryError::VersionNotFound { subject, version }) => {
            Status::not_found(format!("subject {subject} version {version}"))
        }
        other => Status::internal(other.to_string()),
    }
}

fn schema_to_proto(s: Schema) -> proto::SchemaMessage {
    proto::SchemaMessage {
        id: s.id,
        subject: s.subject,
        version: s.version,
        bytes: s.bytes,
    }
}

fn task_to_proto(t: Task) -> proto::TaskMessage {
    proto::TaskMessage {
        id: t.id,
        parent_id: t.parent_id,
        status: match t.status {
            TaskStatus::Pending => 0,
            TaskStatus::Running => 1,
            TaskStatus::Done => 2,
            TaskStatus::Failed => 3,
            TaskStatus::Cancelled => 4,
        },
        created_nanos: t.created_nanos,
        payload: t.payload,
        result: t.result.unwrap_or_default(),
        error: t.error.unwrap_or_default(),
    }
}

fn join_err(e: tokio::task::JoinError) -> Status {
    Status::internal(format!("blocking task join: {e}"))
}

#[allow(clippy::result_large_err)]
fn check_auth<T>(
    expected: &Option<String>,
    req: &tonic::Request<T>,
) -> std::result::Result<(), Status> {
    let Some(expected) = expected else {
        return Ok(());
    };
    let header = req
        .metadata()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| Status::unauthenticated("missing authorization header"))?;
    let provided = header.strip_prefix("Bearer ").unwrap_or(header);
    if provided != expected {
        return Err(Status::unauthenticated("invalid token"));
    }
    Ok(())
}

async fn handle_metrics_connection(
    broker: Arc<Broker>,
    mut stream: tokio::net::TcpStream,
) -> std::io::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut buf = vec![0u8; 4096];
    let _ = stream.read(&mut buf).await?;

    let body = match broker.all_stats() {
        Ok(stats) => format_metrics(&stats),
        Err(e) => format!("# error: {e}\n"),
    };

    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    stream.write_all(response.as_bytes()).await?;
    stream.shutdown().await?;
    Ok(())
}

pub(crate) fn format_metrics(stats: &[(String, qx_topic::TopicStats)]) -> String {
    let mut out = String::new();
    out.push_str("# HELP qx_topic_pending Messages awaiting delivery.\n");
    out.push_str("# TYPE qx_topic_pending gauge\n");
    for (name, s) in stats {
        out.push_str(&format!(
            "qx_topic_pending{{topic={}}} {}\n",
            escape_label(name),
            s.pending
        ));
    }
    out.push_str("# HELP qx_topic_inflight Messages claimed by a consumer, not yet acked.\n");
    out.push_str("# TYPE qx_topic_inflight gauge\n");
    for (name, s) in stats {
        out.push_str(&format!(
            "qx_topic_inflight{{topic={}}} {}\n",
            escape_label(name),
            s.inflight
        ));
    }
    out.push_str("# HELP qx_topic_dead Messages in the dead-letter state.\n");
    out.push_str("# TYPE qx_topic_dead gauge\n");
    for (name, s) in stats {
        out.push_str(&format!(
            "qx_topic_dead{{topic={}}} {}\n",
            escape_label(name),
            s.dead
        ));
    }
    out.push_str("# HELP qx_topic_partitions Configured partition count.\n");
    out.push_str("# TYPE qx_topic_partitions gauge\n");
    for (name, s) in stats {
        out.push_str(&format!(
            "qx_topic_partitions{{topic={}}} {}\n",
            escape_label(name),
            s.partition_count
        ));
    }
    out
}

fn escape_label(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' | '\\' => {
                out.push('\\');
                out.push(c);
            }
            '\n' => out.push_str("\\n"),
            _ => out.push(c),
        }
    }
    out.push('"');
    out
}
