# Raw Kubernetes Manifests

Apply in this order to avoid dependency races:

1. `00-namespace.yaml`
2. `01-configmap-secret.yaml`
3. `02-serviceaccount-rbac.yaml`
4. `03-ingest.yaml`
5. `04-worker.yaml`
6. `05-observability.yaml`
7. `06-cronjob-pdb-networkpolicy.yaml`
