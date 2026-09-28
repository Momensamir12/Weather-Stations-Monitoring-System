#!/usr/bin/env bash
# Remove everything deploy.sh created, in reverse dependency order:
# central -> stations -> Open-Meteo -> Schema Registry -> Kafka.
#
# Usage:
#   ./teardown.sh                 # asks for confirmation, deletes workloads AND stored data
#   ./teardown.sh -y              # no confirmation prompt
#   KEEP_DATA=true ./teardown.sh  # keep the PVCs (Kafka topic history, Bitcask + Parquet data)
#
# Note: with KEEP_DATA=false (the default) the Kafka PVC is deleted too, so topic history and
# consumer offsets are gone. The next deploy starts from a completely clean slate.

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."   # repo root (this script lives in scripts/)

KEEP_DATA="${KEEP_DATA:-false}"
K8S_DIR="kubernetes"

# PVC names (from the manifests / `kubectl get pv`)
CENTRAL_PVC="base-central-storage"
KAFKA_PVC="kafka-data-kafka-0"

ASSUME_YES=false
[[ "${1:-}" == "-y" || "${1:-}" == "--yes" ]] && ASSUME_YES=true

log() { printf '\n==> %s\n' "$*"; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }

command -v kubectl >/dev/null 2>&1 || die "'kubectl' is not installed or not on PATH"
kubectl cluster-info >/dev/null 2>&1 || die "cannot reach a cluster with the current kubectl context"

echo "kubectl context : $(kubectl config current-context)"
if [[ "$KEEP_DATA" == "true" ]]; then
  echo "data (PVCs)     : will be KEPT"
else
  echo "data (PVCs)     : will be DELETED ($CENTRAL_PVC, $KAFKA_PVC)"
fi

if [[ "$ASSUME_YES" != "true" ]]; then
  read -r -p "Delete the weather-station stack from this cluster? [y/N] " answer
  [[ "$answer" =~ ^[Yy]$ ]] || { echo "Aborted."; exit 0; }
fi

# --- Application services (reverse of deploy order) ---------------------------------------

# base-central.yaml also defines the PVC, so delete the Deployment and Service by name.
# That way KEEP_DATA=true really keeps the volume.
log "Removing base central station"
kubectl delete deployment base-central-station-deployment --ignore-not-found
kubectl delete service base-central-service --ignore-not-found

log "Removing 10 weather stations"
kubectl delete -f "$K8S_DIR/weather-stations" --ignore-not-found

log "Removing Open-Meteo adapter"
kubectl delete -f "$K8S_DIR/open-meteo-adapter" --ignore-not-found

# --- Infrastructure --------------------------------------------------------------------------

shopt -s nullglob

log "Removing Schema Registry"
for f in "$K8S_DIR"/kafka-schema-registry/schema-*.yaml; do
  kubectl delete -f "$f" --ignore-not-found
done

log "Removing Kafka"
for f in "$K8S_DIR"/kafka-schema-registry/kafka-*.yaml; do
  kubectl delete -f "$f" --ignore-not-found
done

shopt -u nullglob

# --- Persistent data ---------------------------------------------------------------------------
# StatefulSet PVCs are not removed when the StatefulSet is deleted, so do it explicitly.
if [[ "$KEEP_DATA" != "true" ]]; then
  log "Deleting persistent volume claims"
  kubectl delete pvc "$CENTRAL_PVC" "$KAFKA_PVC" --ignore-not-found
fi

log "Done. Remaining resources:"
kubectl get pods,pvc,pv 2>/dev/null || true