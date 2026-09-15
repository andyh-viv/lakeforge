//! Kubernetes backend: one driver Deployment + ClusterIP Service and one
//! executor Deployment per cluster, all labelled `lakeforge.io/cluster=<id>`.

use async_trait::async_trait;
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::Service;
use kube::api::{DeleteParams, Patch, PatchParams, PostParams};
use kube::{Api, Client};
use serde_json::json;

use crate::{BackendState, BackendStatus, ClusterBackend, ClusterError, ClusterHandle, LaunchSpec, Result};

pub struct KubernetesBackend {
    client: Client,
    namespace: String,
    default_image: String,
    service_account: Option<String>,
    pod_labels: Vec<(String, String)>,
    node_selector: Vec<(String, String)>,
    extra_env: Vec<(String, String)>,
}

/// Parses `k1=v1,k2=v2` (surrounding whitespace ignored, empty entries skipped).
fn parse_kv_list(raw: &str) -> Vec<(String, String)> {
    raw.split(',')
        .filter_map(|kv| {
            let (k, v) = kv.split_once('=')?;
            let k = k.trim();
            (!k.is_empty()).then(|| (k.to_string(), v.trim().to_string()))
        })
        .collect()
}

fn env_kv_list(name: &str) -> Vec<(String, String)> {
    std::env::var(name).map(|v| parse_kv_list(&v)).unwrap_or_default()
}

const DRIVER_PORT: i32 = 50051;
const EXECUTOR_PORT: i32 = 50052;

/// Role-specific parameters for a driver or executor Deployment.
struct PodRole<'a> {
    role: &'a str,
    name: &'a str,
    replicas: u32,
    args: Vec<String>,
    mem_mb: u64,
    extra_env: &'a [(&'a str, String)],
}

impl KubernetesBackend {
    pub async fn from_env() -> Result<Self> {
        let client = Client::try_default().await.map_err(|e| ClusterError::Backend(e.to_string()))?;
        Ok(Self {
            namespace: std::env::var("LAKEFORGE_K8S_NAMESPACE").unwrap_or_else(|_| "lakeforge-compute".into()),
            default_image: std::env::var("LAKEFORGE_FORGE_IMAGE")
                .unwrap_or_else(|_| "ghcr.io/lakeforge/forge:latest".into()),
            service_account: std::env::var("LAKEFORGE_K8S_SERVICE_ACCOUNT").ok(),
            pod_labels: env_kv_list("LAKEFORGE_FORGE_POD_LABELS"),
            node_selector: env_kv_list("LAKEFORGE_FORGE_NODE_SELECTOR"),
            extra_env: env_kv_list("LAKEFORGE_FORGE_ENV"),
            client,
        })
    }

    fn names(id: &str) -> (String, String, String) {
        let short: String = id.chars().filter(|c| c.is_ascii_alphanumeric()).take(12).collect::<String>().to_lowercase();
        (format!("forge-{short}-driver"), format!("forge-{short}-exec"), format!("forge-{short}-driver"))
    }

    fn labels(id: &str, role: &str) -> serde_json::Value {
        json!({ "app.kubernetes.io/name": "forge", "lakeforge.io/cluster": id, "lakeforge.io/role": role })
    }

    fn env_json(&self, spec: &LaunchSpec, extra: &[(&str, String)]) -> serde_json::Value {
        let mut v: Vec<serde_json::Value> = self
            .extra_env
            .iter()
            .filter(|(k, _)| !spec.env.contains_key(k))
            .map(|(k, val)| json!({ "name": k, "value": val }))
            .collect();
        v.extend(spec.env.iter().map(|(k, val)| json!({ "name": k, "value": val })));
        for (k, val) in spec.conf.iter().map(|(k, v)| (format!("FORGE_CONF_{}", k.replace('.', "_").to_uppercase()), v.clone())) {
            v.push(json!({ "name": k, "value": val }));
        }
        for (k, val) in extra {
            v.push(json!({ "name": k, "value": val }));
        }
        v.push(json!({ "name": "FORGE_ADVERTISE_HOST", "valueFrom": { "fieldRef": { "fieldPath": "status.podIP" } } }));
        v.push(json!({ "name": "FORGE_EXECUTOR_ID", "valueFrom": { "fieldRef": { "fieldPath": "metadata.name" } } }));
        serde_json::Value::Array(v)
    }

    fn deployment(&self, spec: &LaunchSpec, pod: PodRole<'_>) -> Deployment {
        let PodRole { role, name, replicas, args, mem_mb, extra_env } = pod;
        let labels = Self::labels(&spec.cluster_id, role);
        let mut pod_labels = labels.clone();
        for (k, v) in &self.pod_labels {
            pod_labels[k] = json!(v);
        }
        let image = spec.image.clone().unwrap_or_else(|| self.default_image.clone());
        let port = if role == "driver" { DRIVER_PORT } else { EXECUTOR_PORT };
        let mut pod_spec = json!({
            "containers": [{
                "name": "forge",
                "image": image,
                "args": args,
                "env": self.env_json(spec, extra_env),
                "ports": [{ "containerPort": port, "name": "grpc" }],
                "resources": {
                    "requests": { "memory": format!("{mem_mb}Mi"), "cpu": if role == "driver" { "500m".to_string() } else { spec.slots_per_worker.max(1).to_string() } },
                    "limits": { "memory": format!("{mem_mb}Mi") }
                },
                "volumeMounts": [{ "name": "work", "mountPath": "/tmp/forge" }]
            }],
            "volumes": [{ "name": "work", "emptyDir": {} }]
        });
        if let Some(sa) = &self.service_account {
            pod_spec["serviceAccountName"] = json!(sa);
        }
        if !self.node_selector.is_empty() {
            pod_spec["nodeSelector"] = self.node_selector.iter().map(|(k, v)| (k.clone(), json!(v))).collect::<serde_json::Map<_, _>>().into();
        }
        serde_json::from_value(json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": { "name": name, "namespace": self.namespace, "labels": labels },
            "spec": {
                "replicas": replicas,
                "selector": { "matchLabels": labels },
                "template": { "metadata": { "labels": pod_labels }, "spec": pod_spec }
            }
        }))
        .expect("valid deployment")
    }
}

#[async_trait]
impl ClusterBackend for KubernetesBackend {
    fn name(&self) -> &'static str {
        "kubernetes"
    }

    async fn launch(&self, spec: &LaunchSpec) -> Result<ClusterHandle> {
        let (driver_name, exec_name, svc_name) = Self::names(&spec.cluster_id);
        let driver_addr = format!("http://{svc_name}.{}.svc:{DRIVER_PORT}", self.namespace);
        let deps: Api<Deployment> = Api::namespaced(self.client.clone(), &self.namespace);
        let svcs: Api<Service> = Api::namespaced(self.client.clone(), &self.namespace);

        let driver = self.deployment(
            spec,
            PodRole {
                role: "driver",
                name: &driver_name,
                replicas: 1,
                args: vec!["driver".into(), "--bind".into(), format!("0.0.0.0:{DRIVER_PORT}"), "--no-local-fallback".into()],
                mem_mb: spec.driver_memory_mb,
                extra_env: &[],
            },
        );
        let exec_env = [("FORGE_DRIVER_ADDR", driver_addr.clone()), ("FORGE_MEMORY_LIMIT_MB", spec.worker_memory_mb.to_string())];
        let execs = self.deployment(
            spec,
            PodRole {
                role: "executor",
                name: &exec_name,
                replicas: spec.num_workers,
                args: vec!["executor".into(), "--bind".into(), format!("0.0.0.0:{EXECUTOR_PORT}"), "--slots".into(), spec.slots_per_worker.to_string()],
                mem_mb: spec.worker_memory_mb,
                extra_env: &exec_env,
            },
        );
        let svc: Service = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": { "name": svc_name, "namespace": self.namespace, "labels": Self::labels(&spec.cluster_id, "driver") },
            "spec": {
                "selector": Self::labels(&spec.cluster_id, "driver"),
                "ports": [{ "name": "grpc", "port": DRIVER_PORT, "targetPort": DRIVER_PORT }]
            }
        }))
        .expect("valid service");

        let pp = PostParams::default();
        let map = |e: kube::Error| ClusterError::Launch(e.to_string());
        svcs.create(&pp, &svc).await.map_err(map)?;
        deps.create(&pp, &driver).await.map_err(map)?;
        deps.create(&pp, &execs).await.map_err(map)?;

        Ok(ClusterHandle {
            backend: "kubernetes".into(),
            driver_addr,
            state: json!({ "namespace": self.namespace, "driver": driver_name, "executors": exec_name, "service": svc_name }),
        })
    }

    async fn status(&self, handle: &ClusterHandle) -> Result<BackendStatus> {
        let deps: Api<Deployment> = Api::namespaced(self.client.clone(), &self.namespace);
        let driver = handle.state["driver"].as_str().unwrap_or_default();
        let execs = handle.state["executors"].as_str().unwrap_or_default();
        let d = match deps.get_opt(driver).await.map_err(|e| ClusterError::Backend(e.to_string()))? {
            Some(d) => d,
            None => return Ok(BackendStatus { state: BackendState::Terminated, message: None, ready_workers: 0 }),
        };
        let driver_ready = d.status.as_ref().and_then(|s| s.ready_replicas).unwrap_or(0) > 0;
        let ready_workers = deps
            .get_opt(execs)
            .await
            .map_err(|e| ClusterError::Backend(e.to_string()))?
            .and_then(|e| e.status.and_then(|s| s.ready_replicas))
            .unwrap_or(0) as u32;
        Ok(BackendStatus {
            state: if driver_ready { BackendState::Running } else { BackendState::Pending },
            message: None,
            ready_workers,
        })
    }

    async fn resize(&self, spec: &LaunchSpec, handle: &ClusterHandle) -> Result<ClusterHandle> {
        let deps: Api<Deployment> = Api::namespaced(self.client.clone(), &self.namespace);
        let execs = handle.state["executors"].as_str().unwrap_or_default();
        let patch = json!({ "spec": { "replicas": spec.num_workers } });
        deps.patch(execs, &PatchParams::default(), &Patch::Merge(&patch))
            .await
            .map_err(|e| ClusterError::Backend(e.to_string()))?;
        Ok(handle.clone())
    }

    async fn terminate(&self, handle: &ClusterHandle) -> Result<()> {
        let id_selector = handle.state["driver"].as_str().unwrap_or_default();
        let deps: Api<Deployment> = Api::namespaced(self.client.clone(), &self.namespace);
        let svcs: Api<Service> = Api::namespaced(self.client.clone(), &self.namespace);
        let dp = DeleteParams::default();
        let _ = deps.delete(id_selector, &dp).await;
        if let Some(e) = handle.state["executors"].as_str() {
            let _ = deps.delete(e, &dp).await;
        }
        if let Some(s) = handle.state["service"].as_str() {
            let _ = svcs.delete(s, &dp).await;
        }
        Ok(())
    }
}
