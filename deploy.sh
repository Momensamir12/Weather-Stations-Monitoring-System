#!/usr/bin/env bash
# Build the Docker images, load them into the cluster, and deploy the stack in
#
#
# Usage:
#   ./deploy.sh                          # kind cluster named "kind"
#   KIND_CLUSTER_NAME=dev ./deploy.sh    # kind cluster with another name
#   CLUSTER=minikube ./deploy.sh         # minikube (MINIKUBE_PROFILE to pick a profile)
#   CLUSTER=none ./deploy.sh             # cluster pulls from a registry; push images yourself
#   SKIP_BUILD=true ./deploy.sh          # skip docker build + image load, only (re)apply manifests

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

# ----------------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------------
CLUSTER="${CLUSTER:-kind}"                      # kind | minikube | none
KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-kind}"
MINIKUBE_PROFILE="${MINIKUBE_PROFILE:-minikube}"
SKIP_BUILD="${SKIP_BUILD:-false}"
WAIT_TIMEOUT="${WAIT_TIMEOUT:-300}"             # seconds, per rollout

K8S_DIR="kubernetes"


BUILD_TARGETS=(
  "$K8S_DIR/base-central-station|BaseCentralStation/Dockerfile|."
  "$K8S_DIR/weather-stations|WeatherStation/Dockerfile|."
  "$K8S_DIR/open-meteo-adapter|OpenMeteoAdapter/Dockerfile|."
)

# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------
log()  { printf '\n==> %s\n' "$*"; }
warn() { printf 'warning: %s\n' "$*" >&2; }
die()  { printf 'error: %s\n' "$*" >&2; exit 1; }

need() { command -v "$1" >/dev/null 2>&1 || die "'$1' is not installed or not on PATH"; }

image_from_manifest() {
  grep -hE '^[[:space:]]*image:' "$1"/*.yaml 2>/dev/null \
    | head -n1 | awk '{print $2}' | tr -d "\"'" || true
}

load_image() {
  local image="$1"
  case "$CLUSTER" in
    kind)     kind load docker-image "$image" --name "$KIND_CLUSTER_NAME" ;;
    minikube) minikube image load "$image" -p "$MINIKUBE_PROFILE" ;;
    none)     warn "CLUSTER=none: not loading $image. Push it to a registry the cluster can pull from." ;;
    *)        die "unknown CLUSTER='$CLUSTER' (use kind, minikube or none)" ;;
  esac
}

build_and_load_images() {
  local target manifest_dir dockerfile context image
  for target in "${BUILD_TARGETS[@]}"; do
    IFS='|' read -r manifest_dir dockerfile context <<< "$target"

    image="$(image_from_manifest "$manifest_dir")"
    [[ -n "$image" ]]      || die "no 'image:' line found in $manifest_dir/*.yaml"
    [[ -f "$dockerfile" ]] || die "Dockerfile not found: $dockerfile (edit BUILD_TARGETS at the top of this script)"

    log "Building $image  (Dockerfile: $dockerfile, context: $context)"
    docker build -f "$dockerfile" -t "$image" "$context"

    log "Loading $image into the cluster ($CLUSTER)"
    load_image "$image"
  done
}

apply_files() {
  local f
  for f in "$@"; do
    [[ -f "$f" ]] || die "manifest not found: $f"
    kubectl apply -f "$f"
  done
}

deploy_app() {
  local label="$1" path="$2" before d
  log "Deploying $label"
  before="$(kubectl get deployments -o name 2>/dev/null || true)"
  kubectl apply -f "$path"
  kubectl get -f "$path" -o name 2>/dev/null | grep '^deployment' | while read -r d; do
    if grep -qx "$d" <<< "$before"; then
      kubectl rollout restart "$d"
    fi
  done || true
}

wait_for_kafka() {
  log "Waiting for Kafka"
  kubectl rollout status statefulset/kafka --timeout="${WAIT_TIMEOUT}s"
  for _ in $(seq 1 60); do
    if kubectl exec kafka-0 -- kafka-topics --bootstrap-server kafka:9092 --list >/dev/null 2>&1; then
      echo "Kafka is accepting connections."
      return 0
    fi
    sleep 5
  done
  die "Kafka did not become ready in time (try: kubectl logs kafka-0)"
}

wait_for_schema_registry() {
  log "Waiting for Schema Registry"
  kubectl rollout status deployment/schema-registry --timeout="${WAIT_TIMEOUT}s"
  for _ in $(seq 1 30); do
    if kubectl exec deploy/schema-registry -- curl -sf http://localhost:8081/subjects >/dev/null 2>&1; then
      echo "Schema Registry is responding."
      return 0
    fi
    sleep 4
  done
  warn "Schema Registry pod is up but did not answer on :8081 (or curl is missing in the image). Continuing."
}

# ----------------------------------------------------------------------------
# Preflight
# ----------------------------------------------------------------------------
need kubectl
[[ "$SKIP_BUILD" == "true" ]] || need docker
if [[ "$SKIP_BUILD" != "true" ]]; then
  case "$CLUSTER" in
    kind)     need kind ;;
    minikube) need minikube ;;
  esac
fi

kubectl cluster-info >/dev/null 2>&1 || die "cannot reach a cluster with the current kubectl context"
echo "kubectl context : $(kubectl config current-context)"
echo "cluster type    : $CLUSTER"

# ----------------------------------------------------------------------------
# Deploy
# ----------------------------------------------------------------------------
if [[ "$SKIP_BUILD" == "true" ]]; then
  log "SKIP_BUILD=true: not building or loading images"
else
  build_and_load_images
fi

# 1. Kafka (config map and service before the StatefulSet that uses them)
log "Deploying Kafka"
apply_files \
  "$K8S_DIR/kafka-schema-registry/kafka-config-map.yaml" \
  "$K8S_DIR/kafka-schema-registry/kafka-service.yaml" \
  "$K8S_DIR/kafka-schema-registry/kafka-statefulset.yaml"
wait_for_kafka

# 2. Schema Registry (glob on schema-*.yaml so the file-name spelling doesn't matter)
log "Deploying Schema Registry"
shopt -s nullglob
registry_files=("$K8S_DIR"/kafka-schema-registry/schema-*.yaml)
shopt -u nullglob
[[ ${#registry_files[@]} -gt 0 ]] || die "no schema-*.yaml files in $K8S_DIR/kafka-schema-registry"
apply_files "${registry_files[@]}"
wait_for_schema_registry

# 3-5. Application services
deploy_app "Open-Meteo adapter"     "$K8S_DIR/open-meteo-adapter"
deploy_app "10 weather stations"    "$K8S_DIR/weather-stations"
deploy_app "base central station"   "$K8S_DIR/base-central-station"

# Wait for everything
log "Waiting for all deployments to be ready"
for d in $(kubectl get deployments -o name); do
  kubectl rollout status "$d" --timeout="${WAIT_TIMEOUT}s"
done

log "Done"
kubectl get pods
cat <<'EOF'

Useful next steps:
  kubectl logs -f -l app=base-central-station
  kubectl get pv,pvc
EOF