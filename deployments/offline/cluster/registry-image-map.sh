# Shared canonical -> private registry image mapping.
registry_image() {
  case "$1" in
    registry.k8s.io/coredns/coredns:*) printf '%s/kubernetes/coredns:%s\n' "$DTS_REGISTRY" "${1##*:}" ;;
    registry.k8s.io/*) printf '%s/kubernetes/%s\n' "$DTS_REGISTRY" "${1#registry.k8s.io/}" ;;
    ghcr.io/flannel-io/*) printf '%s/flannel/%s\n' "$DTS_REGISTRY" "${1#ghcr.io/flannel-io/}" ;;
    docker.io/library/registry:*) printf '%s/library/registry:%s\n' "$DTS_REGISTRY" "${1##*:}" ;;
    dts/*) printf '%s/%s\n' "$DTS_REGISTRY" "$1" ;;
    *) echo "ERROR: unmapped image $1" >&2; return 1 ;;
  esac
}
