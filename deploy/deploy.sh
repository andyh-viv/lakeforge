#!/usr/bin/env bash
# One-click Lakeforge deployment.
#
#   deploy/deploy.sh local                       # docker compose on this machine
#   deploy/deploy.sh kubernetes                  # helm into current kubectl context
#   deploy/deploy.sh aws   [--region us-east-1]  # VPC+EKS+RDS+S3, then helm
#   deploy/deploy.sh gcp   --project my-proj     # VPC+GKE+CloudSQL+GCS, then helm
#   deploy/deploy.sh azure --subscription_id <id>  # RG+AKS+Postgres+ADLS, then helm
#   deploy/deploy.sh <target> --destroy          # tear down
#
# Extra `--key value` flags are forwarded to Terraform as `-var key=value`
# (cloud targets) or to `helm upgrade` as `--set key=value` (kubernetes target).
# Requires: docker (local); kubectl+helm (kubernetes); terraform + the cloud CLI
# already authenticated (aws / gcloud / az).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET="${1:-}"
shift || true

DESTROY=0
NAME="${LAKEFORGE_NAME:-lakeforge}"
IMAGE_TAG="${LAKEFORGE_IMAGE_TAG:-latest}"
BUILD=0
declare -a EXTRA=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --destroy) DESTROY=1; shift ;;
    --build) BUILD=1; shift ;;
    --name) NAME="$2"; shift 2 ;;
    --tag) IMAGE_TAG="$2"; shift 2 ;;
    --*) EXTRA+=("${1#--}" "$2"); shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

log() { printf '\033[1;36m==> %s\033[0m\n' "$*"; }
need() { command -v "$1" >/dev/null 2>&1 || { echo "missing required tool: $1" >&2; exit 1; }; }

tf_vars() {
  local i
  echo "-var" "name=$NAME" "-var" "image_tag=$IMAGE_TAG"
  for ((i = 0; i < ${#EXTRA[@]}; i += 2)); do
    echo "-var" "${EXTRA[i]}=${EXTRA[i + 1]}"
  done
}

deploy_terraform() {
  local cloud="$1"
  need terraform
  local dir="$ROOT/deploy/terraform/$cloud"
  mapfile -t vars < <(tf_vars)
  (
    cd "$dir"
    terraform init -input=false -upgrade >/dev/null
    if [[ $DESTROY == 1 ]]; then
      log "Destroying Lakeforge on $cloud"
      terraform destroy -input=false -auto-approve "${vars[@]}"
      exit 0
    fi
    log "Deploying Lakeforge on $cloud (this creates a Kubernetes cluster, database and bucket; expect 15-25 minutes)"
    terraform apply -input=false -auto-approve "${vars[@]}"
    echo
    log "Done. Workspace:"
    terraform output -raw workspace_url; echo
    echo "admin user: $(terraform output -raw admin_user)"
    echo "admin password: \$($(terraform output -raw admin_password_command))"
    echo "kubeconfig:     $(terraform output -raw kubeconfig_command)"
  )
}

case "$TARGET" in
  local)
    need docker
    cd "$ROOT/deploy/docker"
    if [[ $DESTROY == 1 ]]; then
      docker compose down -v
      exit 0
    fi
    export LAKEFORGE_IMAGE_TAG="$IMAGE_TAG"
    if [[ $BUILD == 1 ]]; then
      log "Building images"
      docker compose build
    fi
    log "Starting Lakeforge (postgres + control plane)"
    docker compose up -d
    port="${LAKEFORGE_PORT:-8080}"
    for _ in $(seq 1 60); do
      curl -sf "http://localhost:${port}/health" >/dev/null 2>&1 && break
      sleep 2
    done
    log "Lakeforge is up: http://localhost:${port}  (admin@lakeforge.local / ${LAKEFORGE_ADMIN_PASSWORD:-admin})"
    ;;

  kubernetes|k8s)
    need kubectl; need helm
    ns="${LAKEFORGE_NAMESPACE:-lakeforge}"
    if [[ $DESTROY == 1 ]]; then
      helm -n "$ns" uninstall "$NAME" || true
      kubectl delete namespace "$ns" --ignore-not-found
      exit 0
    fi
    sets=("--set" "image.tag=$IMAGE_TAG" "--set" "forge.tag=$IMAGE_TAG")
    for ((i = 0; i < ${#EXTRA[@]}; i += 2)); do sets+=("--set" "${EXTRA[i]}=${EXTRA[i + 1]}"); done
    log "Installing Helm release $NAME into namespace $ns ($(kubectl config current-context))"
    helm upgrade --install "$NAME" "$ROOT/deploy/helm/lakeforge" \
      --namespace "$ns" --create-namespace --wait --timeout 10m \
      --set database.embeddedPostgres.enabled=true "${sets[@]}"
    ;;

  aws|gcp|azure)
    case "$TARGET" in
      aws) need aws ;;
      gcp) need gcloud ;;
      azure) need az ;;
    esac
    deploy_terraform "$TARGET"
    ;;

  *)
    sed -n '2,15p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
    exit 2
    ;;
esac
