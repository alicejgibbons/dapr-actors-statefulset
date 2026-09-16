# Dapr Actors on a StatefulSet

A local test application demonstrating: one Dapr actor type per StatefulSet
replica, a single actor client invoking every replica's actor type, and
job-driven autoscaling (via KEDA/HPA) that adds one pod per job that needs
running and removes it again as soon as that job finishes.

See [`/Users/alicegibbons/.claude/plans/make-a-test-application-synthetic-boot.md`](../../.claude/plans/make-a-test-application-synthetic-boot.md)
for the full design rationale.

## Components

- **`src/ActorContracts`** — shared `IWorkerActor` interface + DTOs.
- **`src/ActorHost`** — ASP.NET Core app deployed as the `actor-host` StatefulSet.
  Each pod parses its own ordinal from its hostname (`actor-host-N`) and registers
  exactly one actor type, `WorkerActor-N`, at startup.
- **`src/ActorClient`** — ASP.NET Core app deployed as a single `actor-client`
  Deployment. It watches the `actor-host` StatefulSet's pods (via the
  Kubernetes API), runs a real job queue (submit → assign to an idle known
  actor type → invoke `DoWorkAsync` → free the slot when it returns), and
  exposes `/metrics/desired-instances` (jobs still pending or running) for
  KEDA to poll.
- **KEDA `ScaledObject`** — targets the `actor-host` StatefulSet and scales its
  replica count 1:1 with the client's outstanding job count, in both
  directions.

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

### The job queue, and why finishing a job scales its pod back down

`JobQueue` (`src/ActorClient/Services/JobQueue.cs`) holds real jobs, each with
a simulated duration, moving through `Pending → Running → Completed`:

- **Submit**: `POST /jobs` (or the built-in synthetic generator, see below)
  adds a `Job` to a pending queue.
- **Assign**: every 2s, `ActorInvokerWorker` asks the queue to match pending
  jobs to idle known ordinals (an ordinal already running a job is skipped).
  Each match fires `DoWorkAsync` on that ordinal's actor type concurrently —
  multiple jobs run in parallel across different pods.
- **Complete**: when the call returns (or fails), the ordinal's slot is freed
  immediately — the job is done, so it stops counting as demand.

`JobQueue.DesiredInstanceCount` is defined as `PendingJobCount + ActiveJobCount`
— literally "jobs that still need a pod." There is no separate "shut it down
now" step: a job finishing removes it from that count on its own, which is
what makes scale-down automatic instead of something you have to trigger.

A `JobGeneratorWorker` (`src/ActorClient/Services/JobGeneratorWorker.cs`)
submits one synthetic job every 15s with a random 20-35s duration (config:
`JOB_GENERATOR_INTERVAL_SECONDS`, `JOB_DURATION_MS_MIN/MAX`), so you can watch
the whole cycle happen on its own without calling any endpoint.

### How scaling is driven

Scaling isn't CPU-based — it's driven directly by outstanding job count, via
KEDA, in both directions:

1. `GET /metrics/desired-instances` exposes `JobQueue.DesiredInstanceCount`
   as `{"value": N}`.
2. KEDA's `ScaledObject` (`k8s/40-keda-scaledobject.yaml`) has a
   `metrics-api` trigger polling that endpoint, with `targetValue: "1"` —
   so KEDA computes `desiredReplicas = metricValue / targetValue = N`, an
   exact 1:1 mapping. KEDA creates a real `HorizontalPodAutoscaler`
   (`keda-hpa-actor-host-scaledobject`) under the hood that targets the
   `actor-host` StatefulSet directly.
3. The StatefulSet controller creates/removes ordinal pods to match. A new
   pod self-registers its own actor type as described above; when demand
   drops because jobs finished, the *highest*-ordinal pod(s) are removed
   (that's how StatefulSets always scale down), and the client's watcher
   drops the corresponding actor type(s) from its known set on its next poll.
4. `minReplicaCount: 1` means it never goes below one pod — there's always
   an idle worker ready to pick up the next job immediately — and
   `scaleDown.stabilizationWindowSeconds: 30` keeps the down-scale quick
   enough to watch instead of waiting on the default 5-minute HPA window.

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
- Sustained job submission (one every 15s, each running 20-35s) scaled
  `actor-host` from 1 up to 8 replicas; each new pod (`actor-host-1` …
  `actor-host-7`) was discovered and got its first job within one poll cycle
  (≤5s) of becoming Ready.
- **Scale-down driven purely by job completion, with no manual trigger**,
  was also confirmed directly: with 8 replicas up, `JobQueue` was observed
  freeing an ordinal's slot the instant each `DoWorkAsync` call returned
  (client log: `Job 5bbf1cf2 finished on WorkerActor-0/job-0 ... ->
  invocationCount=1009`, immediately followed by `desiredInstanceCount`
  dropping). Polling `kubectl get hpa,pods` every 5s while jobs finished and
  none were resubmitted showed the count fall 8 → 3 → 2 → 1 in well under a
  minute, settling exactly at `minReplicaCount: 1` (`keda-hpa-actor-host-
  scaledobject` targets `0/1 (avg)`, `REPLICAS 1`) once
  `desiredInstanceCount` hit 0 — matching `scaleDown.stabilizationWindowSeconds:
  30`. The client's `/status` stopped listing each removed ordinal's actor
  type on its next 5s poll.

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

   Every ~15s you should see a synthetic job get submitted, dispatched to
   `WorkerActor-0` on `actor-host-0`, and finish with an increasing
   `invocationCount` (proving the actor's state persists in Redis across
   calls) — then the pattern repeats.

3. **Submit a burst of jobs on demand instead of waiting on the generator.**
   The app image has no shell utilities, so hit its endpoints from a
   throwaway pod:

   ```bash
   for i in 1 2 3 4; do
     kubectl run curl-$i --rm -i --restart=Never --image=curlimages/curl:8.10.1 -n actors-demo -- \
       curl -s -X POST 'http://actor-client.actors-demo.svc.cluster.local:8080/jobs?durationMs=30000'
   done
   ```

   Check outstanding jobs and known actor types at any time:

   ```bash
   kubectl run curl --rm -i --restart=Never --image=curlimages/curl:8.10.1 -n actors-demo -- \
     curl -s http://actor-client.actors-demo.svc.cluster.local:8080/status
   ```

4. **Watch KEDA scale the StatefulSet up, one pod per outstanding job:**

   ```bash
   kubectl get scaledobject,hpa,pods -n actors-demo -w
   ```

   New pods `actor-host-1`, `actor-host-2`, ... appear as jobs pile up. Each
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

7. **Watch scale-down happen on its own as jobs finish** — no manual step
   needed. Once the 4 jobs from step 3 complete (30s each), just keep
   watching:

   ```bash
   kubectl get hpa,pods -n actors-demo -w
   ```

   As each job's `DoWorkAsync` call returns, `ActorInvokerWorker` frees that
   ordinal's slot, `desiredInstanceCount` drops, and — once nothing is left
   pending or running — KEDA scales `actor-host` back down to 1 replica
   within `scaleDown.stabilizationWindowSeconds: 30` (`k8s/40-keda-
   scaledobject.yaml`). The client's `/status` stops listing the removed
   actor types on its next 5s poll. If you want a quiet cluster to watch
   this in isolation, pause the synthetic generator first:

   ```bash
   kubectl -n actors-demo set env deployment/actor-client JOB_GENERATOR_ENABLED=false
   # ...re-enable afterwards:
   kubectl -n actors-demo set env deployment/actor-client JOB_GENERATOR_ENABLED-
   ```

## Tear down

```bash
./scripts/down.sh
```

(See **How it works** above for the design rationale behind the shared
`app-id` and the choice of KEDA over a plain `HorizontalPodAutoscaler`.)
