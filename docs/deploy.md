# Deploying Lakeforge

Every path ends with the same two container images:

| Image | Built from | Contents |
| --- | --- | --- |
| `ghcr.io/<owner>/lakeforge-api` | `deploy/docker/Dockerfile.api` | control plane (`lakeforge-api`), web UI, `forge` binary, Python 3 + `lakeforge-sdk` for notebook kernels |
| `ghcr.io/<owner>/lakeforge-forge` | `deploy/docker/Dockerfile.forge` | `forge` binary only — driver/executor pods on Kubernetes |

`images.yml` publishes both to GHCR on every push to `main` (`latest`, `sha-…`)
and on `v*` tags. Build locally with `docker build -f deploy/docker/Dockerfile.api -t lakeforge-api .`.

`deploy/deploy.sh <target>` is the one-click entry point for all targets below;
`deploy/deploy.sh <target> --destroy` tears down. Extra `--key value` flags
are passed through as Terraform variables (cloud targets) or Helm `--set`
(kubernetes target).

## 1. Local — Docker Compose

```bash
deploy/deploy.sh local            # pulls ghcr images, starts postgres + control plane
deploy/deploy.sh local --build    # build images from this checkout first
# → http://localhost:8080   admin@lakeforge.local / admin
```

Environment knobs (`deploy/docker/docker-compose.yml`): `LAKEFORGE_PORT`,
`LAKEFORGE_ADMIN_PASSWORD`, `LAKEFORGE_JWT_SECRET`, `LAKEFORGE_PUBLIC_URL`,
`LAKEFORGE_IMAGE_TAG`, `POSTGRES_PASSWORD`. Clusters run as `forge` child
processes inside the API container (`LAKEFORGE_CLUSTER_BACKEND=local`); data
lives in the `pgdata` and `lakeforge-data` volumes.

## 2. Any Kubernetes cluster — Helm

```bash
deploy/deploy.sh kubernetes                       # current kubectl context, embedded postgres
deploy/deploy.sh kubernetes --ingress.enabled true --ingress.host lakeforge.example.com
# or directly:
helm upgrade --install lakeforge deploy/helm/lakeforge -n lakeforge --create-namespace \
  --set database.url=postgres://user:pass@host:5432/lakeforge \
  --set storage.root=s3://my-bucket/lakeforge \
  --set controlPlane.publicUrl=https://lakeforge.example.com
```

The chart (`deploy/helm/lakeforge`) creates:

- `Deployment`/`Service` for the control plane, optional `Ingress`.
- A `Secret` with `admin-password`, `jwt-secret`, `database-url`. Values are
  generated on first install and **preserved across upgrades** (the template
  looks up the existing secret). Point `controlPlane.existingSecret` at your
  own secret to manage them externally.
- Two `ServiceAccount`s (control plane, Forge compute) with annotations for
  IRSA / Workload Identity / Azure workload identity, and a `Role` in the
  compute namespace (`forge.namespace`, default `lakeforge-compute`) allowing
  the control plane to create Deployments/Services/Pods for clusters (the
  compute `Namespace` is created by the chart).
- Optional embedded PostgreSQL `StatefulSet` (`database.embeddedPostgres.enabled`)
  for dev/test; use a managed database in production.
- A `PersistentVolumeClaim` mounted at `/var/lib/lakeforge/data`
  (`storage.persistence.enabled`, default on) that backs the default local
  `storage.root`; set `storage.root` to `s3://`, `gs://` or `az://` for object
  storage (the PVC is then only scratch space for notebook kernels and can be
  disabled).

Key values:

| Value | Purpose |
| --- | --- |
| `image.repository` / `image.tag`, `forge.image` / `forge.tag` | images; tags default to the chart `appVersion` |
| `controlPlane.publicUrl` | external URL used in links and CORS |
| `controlPlane.adminUser` / `adminPassword` / `jwtSecret` | bootstrap credentials; blank = generated |
| `controlPlane.cloud` | `kubernetes` / `aws` / `gcp` / `azure` — reported in `/lakeforge/info`, warehouse and pipeline metadata |
| `database.url` | `postgres://…` or `sqlite://…`; blank + embedded postgres = in-cluster DB |
| `storage.root`, `storage.env` | object-store root and extra env (`AWS_REGION`, `AZURE_STORAGE_ACCOUNT_NAME`, …) |
| `forge.namespace`, `forge.env`, `forge.podLabels`, `forge.nodeSelector` | where and how Forge pods run; rendered to `LAKEFORGE_FORGE_*` for the cluster manager |
| `serviceAccount.annotations`, `serviceAccount.compute.annotations` | cloud identity bindings |
| `ingress.*`, `service.type`, `resources`, `nodeSelector`, `tolerations` | usual knobs |

`helm lint deploy/helm/lakeforge` and `helm template …` are run in CI.

## 3. AWS — EKS + RDS + S3

```bash
aws sso login   # or any authenticated AWS CLI session
deploy/deploy.sh aws --region us-east-1 [--name lakeforge] [--public_url https://…]
```

`deploy/terraform/aws` provisions: VPC (public + private subnets, NAT), EKS
with a `control` node group and an autoscaling `compute` node group
(`compute_instance_type`, `compute_min_nodes`, `compute_max_nodes`), the EBS
CSI driver via IRSA, an S3 bucket (versioned, SSE, public access blocked),
IRSA roles for the control plane and Forge pods scoped to that bucket, an RDS
PostgreSQL 16 instance in the private subnets, and the Helm release with
`storage.root=s3://…`, `storage.env.AWS_REGION` and the IRSA annotations set.
Outputs: `workspace_url`, `admin_user`, `admin_password_command`,
`kubeconfig_command`, `workspace_bucket`, `database_endpoint`.

## 4. GCP — GKE + Cloud SQL + GCS

```bash
gcloud auth application-default login
deploy/deploy.sh gcp --project my-project [--region us-central1]
```

`deploy/terraform/gcp` provisions: VPC + subnet with secondary ranges, Cloud
NAT, Private Services Access, a GKE cluster with `control` and autoscaling
`compute` node pools and Workload Identity enabled, a GCS bucket (versioned,
uniform access), GCP service accounts bound to the two Kubernetes service
accounts, a Cloud SQL PostgreSQL 16 instance on the private network, and the
Helm release with `storage.root=gs://…`.

## 5. Azure — AKS + PostgreSQL Flexible Server + ADLS Gen2

```bash
az login
deploy/deploy.sh azure --subscription_id <id> [--location eastus]
```

`deploy/terraform/azure` provisions: resource group, VNet with an AKS subnet
and a delegated PostgreSQL subnet + private DNS zone, AKS with OIDC issuer and
workload identity, `control` and autoscaling `compute` node pools, a storage
account with hierarchical namespace + `lakeforge` filesystem, two user-assigned
managed identities with federated credentials for the Kubernetes service
accounts and `Storage Blob Data Contributor` on the account, a PostgreSQL
Flexible Server, and the Helm release with `storage.root=az://lakeforge/…`,
`storage.env.AZURE_STORAGE_ACCOUNT_NAME` and
`forge.podLabels."azure.workload.identity/use"=true`.

## Common Terraform variables

All three roots share: `name`, `namespace`, `api_image`, `forge_image`,
`image_tag`, `admin_user`, `admin_password` (blank = generated), `public_url`,
`compute_min_nodes` / `compute_max_nodes`, `force_destroy_storage`,
`extra_helm_values` (map merged into the chart values last). The `modules/lakeforge`
module is the shared Helm installer; each cloud root only adds infrastructure
and identity plumbing.

Expect 15–25 minutes for a fresh cloud deploy; most of it is the managed
Kubernetes and database creation.

## After deploying

```bash
$(terraform -chdir=deploy/terraform/aws output -raw kubeconfig_command)
kubectl -n lakeforge get pods                         # control plane
kubectl -n lakeforge-compute get pods                 # Forge drivers/executors, one set per cluster
PASS=$(eval "$(terraform -chdir=deploy/terraform/aws output -raw admin_password_command)")
lakeforge --host "$(terraform -chdir=deploy/terraform/aws output -raw workspace_url)" clusters list
```

Rotate the admin password from Settings in the UI or
`POST /api/2.0/lakeforge/password`, then create personal access tokens for
automation.

## Production checklist

- Put the control plane behind TLS (`ingress.tls` or a cloud load balancer
  certificate) and set `controlPlane.publicUrl` to the `https://` URL.
- Use a managed PostgreSQL (`database.url`) — the embedded StatefulSet has no
  backups or HA.
- Set `controlPlane.jwtSecret` explicitly or via `existingSecret` so sessions,
  tokens and secret-scope encryption survive a chart reinstall.
- Object storage (`storage.root`) rather than the PVC, with workload identity
  (never static keys).
- Restrict `compute_max_nodes` / node sizes to your budget; Forge executors are
  memory-bound (`worker_memory_mb` per cluster spec).
- Authorization is coarse today (see `docs/parity.md`): all workspace users
  can see and operate every object. Do not expose a single deployment to
  mutually untrusted teams.
