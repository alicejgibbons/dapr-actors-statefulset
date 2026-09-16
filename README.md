# Dapr Actors on a StatefulSet

A local test application demonstrating: one Dapr actor type per StatefulSet
replica, a single actor client invoking every replica's actor type, and
demand-driven autoscaling (via KEDA/HPA) that adds exactly one pod per one
new actor instance needed.

See [`/Users/alicegibbons/.claude/plans/make-a-test-application-synthetic-boot.md`](../../.claude/plans/make-a-test-application-synthetic-boot.md)
for the full design rationale.

## Components

- **`src/ActorContracts`** — shared `IWorkerActor` interface + DTOs.
- **`src/ActorHost`** — ASP.NET Core app deployed as the `actor-host` StatefulSet.
  Each pod parses its own ordinal from its hostname (`actor-host-N`) and registers
  exactly one actor type, `WorkerActor-N`, at startup.
- **`src/ActorClient`** — ASP.NET Core app deployed as a single `actor-client`
  Deployment. It watches the `actor-host` StatefulSet's pods (via the Kubernetes
  API), invokes `DoWorkAsync` on every currently-known actor type, and exposes
  `/metrics/desired-instances` for KEDA to poll.
- **KEDA `ScaledObject`** — targets the `actor-host` StatefulSet and scales its
  replica count 1:1 with the client's desired-instance count.

## How it works

### Request flow

```
actor-client (1 pod)
  │
  │  ActorProxy.Create<IWorkerActor>(ActorId("job-N"), "WorkerActor-N")
  │       .DoWorkAsync(item)
  ▼
actor-client's Dapr sidecar
  │  looks up "WorkerActor-N" in the placement table it received
  │  from the Dapr placement service
  ▼
actor-host-N's Dapr sidecar   (the only pod that ever registered WorkerActor-N)
  ▼
actor-host-N's WorkerActor instance
  │  StateManager.SetStateAsync("invocationCount", n)
  ▼
Redis (via the `statestore` Dapr component, actorStateStore: "true")
```

The client never targets a pod, IP, or Dapr `app-id` directly — it only ever
asks for an actor **type** + **id**, and the Dapr runtime resolves that to
whichever sidecar is currently hosting it.

### How one actor type ends up pinned to exactly one pod

Every `actor-host` replica shares the same Dapr `app-id` (`actor-host`) and
runs the identical container image. What makes replica *N* different is one
line it runs at startup (`src/ActorHost/Program.cs` + `PodIdentity.cs`):

1. It reads its own hostname, which for a StatefulSet pod is always
   `actor-host-<ordinal>` — no downward-API plumbing needed, `$HOSTNAME`
   already has it.
2. It computes `actorTypeName = $"WorkerActor-{ordinal}"`.
3. It calls `options.Actors.RegisterActor<WorkerActor>(actorTypeName)` —
   registering that *one* type name and no others.

Each Dapr sidecar reports to the placement service exactly the set of actor
types its local app registered (confirmed in `actor-host-0`'s sidecar log:
`"Reporting initial host to placement service with initial types
[WorkerActor-0]"`). Placement builds one consistent-hash ring **per actor
type**, so a type advertised by only one sidecar always resolves to that
sidecar — regardless of every replica sharing an `app-id`. Scaling the
StatefulSet to a new ordinal simply means a new type appears in the cluster
that didn't exist before; nothing else needs to be told about it.

### How the client discovers new actor types

`StatefulSetWatcher` (`src/ActorClient/Services/StatefulSetWatcher.cs`) polls
the Kubernetes API every 5s for Ready pods matching `app=actor-host`, derives
each one's actor type using the same `WorkerActor-<ordinal>` convention as
the host, and reconciles that into `JobQueue`'s known-types map — this map's
size **is** "the number of pods in the StatefulSet" the client is tracking.
`ActorInvokerWorker` then enqueues and drains one invocation job per known
type every 5s, so a newly-scaled-up pod gets its first call within seconds of
becoming Ready, with no restart of the client.

### How scaling is driven

Scaling isn't CPU-based — it's driven by "one new actor instance needed → one
new pod," via KEDA:

1. `JobQueue.DesiredInstanceCount` starts at 1 and is bumped by
   `DemandGeneratorWorker` (a synthetic load generator, +1 every 20s, capped
   at 8) or by `POST /demand/add` / `POST /demand/reset`.
2. `GET /metrics/desired-instances` exposes that count as `{"value": N}`.
3. KEDA's `ScaledObject` (`k8s/40-keda-scaledobject.yaml`) has a
   `metrics-api` trigger polling that endpoint, with `targetValue: "1"` —
   so KEDA computes `desiredReplicas = metricValue / targetValue = N`, an
   exact 1:1 mapping. KEDA creates a real `HorizontalPodAutoscaler`
   (`keda-hpa-actor-host-scaledobject`) under the hood that targets the
   `actor-host` StatefulSet directly.
4. The StatefulSet controller creates/removes ordinal pods to match, each
   new one self-registering its own actor type as described above.

### Validated end-to-end

This was actually run on a local kind cluster (not just built), which
surfaced two bugs no compiler could have caught:

- **Actor DTOs must not be C# records.** Dapr's actor-remoting client
  serializes call payloads with `DataContractSerializer`, which needs a
  public parameterless constructor; records with primary constructors don't
  have one and failed at invocation time with
  `InvalidDataContractException`. Fixed by making `WorkItem`/`WorkResult`/
  `ActorInfo` (`src/ActorContracts/Models.cs`) plain classes with a default
  constructor and settable properties.
- **KEDA's `metrics-api` scaler has no `method` field for the HTTP verb.**
  A stray `method: "GET"` in the trigger metadata (metrics-api always issues
  GET; `method` there only selects header-vs-query placement for `apiKey`
  auth) made the `ScaledObject` fail admission with `parameter "method"
  value "GET" must be one of [header query]`. Removed it.

With those fixed, a full `./scripts/up.sh` → demo → `./scripts/down.sh` cycle
was exercised twice from a clean cluster and confirmed:
- `actor-host-0` self-registers `WorkerActor-0` and the client invokes it,
  with `invocationCount` incrementing correctly in Redis across calls
  (`redis-cli keys '*'` → `actor-host||WorkerActor-0||job-0||invocationCount`).
- Raising demand scales `actor-host` from 1 → 8 replicas; each new pod
  (`actor-host-1` … `actor-host-7`) is discovered and invoked by the client
  within one poll cycle (≤5s) of becoming Ready.
- Resetting demand to 1 scales back down to a single replica within ~30s
  (the configured `scaleDown.stabilizationWindowSeconds`), and the client's
  `/status` stops listing the removed actor types.

## Prerequisites

`kind`, `docker`, `kubectl`, `helm`, and the `dapr` CLI, all available locally.

## Run it

```bash
./scripts/up.sh
```

This creates a new kind cluster named `dapr-actors-statefulset`, installs the
Dapr control plane and KEDA, builds and loads both container images, and
applies the manifests in `k8s/`.

## Demo walkthrough

1. **Watch the initial state** (one actor-host replica, one client):

   ```bash
   kubectl get pods -n actors-demo -w
   ```

2. **Watch the client discover and invoke the first actor type:**

   ```bash
   kubectl logs -n actors-demo deploy/actor-client -f
   ```

   You should see it invoking `WorkerActor-0` on `actor-host-0` every few
   seconds, with an increasing `invocationCount` (proving the actor's state
   persists in Redis across calls).

3. **Generate demand for new actor instances.** A synthetic generator inside
   the client already raises `desiredInstanceCount` by 1 every 20 seconds
   (capped at 8), or trigger it immediately. The app image has no shell
   utilities, so hit its endpoints from a throwaway pod or a port-forward:

   ```bash
   kubectl run curl --rm -i --restart=Never --image=curlimages/curl:8.10.1 -n actors-demo -- \
     curl -s -X POST http://actor-client.actors-demo.svc.cluster.local:8080/demand/add
   ```

   Check the current demand and known actor types at any time:

   ```bash
   kubectl run curl --rm -i --restart=Never --image=curlimages/curl:8.10.1 -n actors-demo -- \
     curl -s http://actor-client.actors-demo.svc.cluster.local:8080/status
   ```

4. **Watch KEDA scale the StatefulSet up, one pod per unit of demand:**

   ```bash
   kubectl get scaledobject,hpa,pods -n actors-demo -w
   ```

   New pods `actor-host-1`, `actor-host-2`, ... appear as demand rises. Each
   one self-registers its own `WorkerActor-N` type — nothing outside the
   pod's own startup code decides this.

5. **Confirm the single client reaches every replica automatically** — its
   logs and `/status` output should show `WorkerActor-1`, `WorkerActor-2`,
   etc. being invoked within a few seconds of each new pod becoming ready,
   with no restart or redeploy of the client.

6. **Inspect persisted actor state directly in Redis:**

   ```bash
   kubectl exec -n actors-demo deploy/redis -- redis-cli keys '*'
   ```

7. **Watch scale-down.** Reset demand back down to 1 (and optionally disable
   the generator first so it doesn't immediately push it back up):

   ```bash
   kubectl -n actors-demo set env deployment/actor-client DEMAND_GENERATOR_ENABLED=false
   kubectl run curl --rm -i --restart=Never --image=curlimages/curl:8.10.1 -n actors-demo -- \
     curl -s -X POST http://actor-client.actors-demo.svc.cluster.local:8080/demand/reset
   kubectl get hpa,pods -n actors-demo -w
   ```

   The HPA's `scaleDown.stabilizationWindowSeconds: 30` (set in
   `k8s/40-keda-scaledobject.yaml`) keeps this quick to observe; replicas
   drop one at a time back to 1, and the client's `/status` and logs stop
   mentioning the removed actor types. Re-enable the generator afterwards
   with `kubectl -n actors-demo set env deployment/actor-client DEMAND_GENERATOR_ENABLED-`.

## Tear down

```bash
./scripts/down.sh
```

(See **How it works** above for the design rationale behind the shared
`app-id` and the choice of KEDA over a plain `HorizontalPodAutoscaler`.)
