# Helm example

Chart path: [`helm/example`](../helm/example).

This is a **starting point** for OpenShift/Kubernetes: StatefulSet, headless Service, PodDisruptionBudget, split probes, in-process drain. Pin your image registry and review CPU/memory before production.

```bash
# Build and load/push the image first.
helm upgrade --install mocache ./helm/example \
  --namespace your-ns \
  --set image.repository=your.registry/mocache \
  --set image.tag=0.1.0 \
  --set replicaCount=3 \
  --set capacity=100000
```

Release notes printed by Helm list the DNS names to paste into Go and Python clients.

## What the chart encodes for safe rollouts

| Setting | Why |
|---|---|
| `podManagementPolicy: OrderedReady` | One ordinal at a time |
| `updateStrategy: RollingUpdate` | Image/config changes replace pods, not recreate the set |
| `livenessProbe: /livez` | Drain must not look like a crash |
| `readinessProbe: /readyz` (2s period) | Endpoints drop quickly on SIGTERM |
| `startupProbe: /livez` | Slow start does not trip liveness |
| `-drain=5s` | In-process wait; works with a `scratch` image (no `sleep` binary) |
| `terminationGracePeriodSeconds: 30` | Longer than drain + Shutdown |
| `podDisruptionBudget.minAvailable: 2` | Voluntary disruptions leave N-1 pods |
| `publishNotReadyAddresses: false` | Unready/terminating pods are not in the Service |
| No PVC | Restarts are empty by design |

## Changing replica count

`replicaCount` **and** every client's node list must change together. Helm cannot update application config inside consumer pods.

## Rendering without a cluster

```bash
helm template mocache ./helm/example
```
