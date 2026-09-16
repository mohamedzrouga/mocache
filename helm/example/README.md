# helm/example

Runnable Helm chart for a 3-node MoCache StatefulSet. This is an example, not a packaged product chart: pin the image, review resources, and copy the DNS names into both SDKs.

```bash
helm upgrade --install mocache ./helm/example \
  --set image.repository=mocache \
  --set image.tag=0.1.0 \
  --set replicaCount=3
```

Details: [docs/operations.md](../../docs/operations.md) and [docs/helm.md](../../docs/helm.md).
