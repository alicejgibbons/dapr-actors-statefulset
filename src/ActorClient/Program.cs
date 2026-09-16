using ActorClient.Services;
using k8s;
using k8s.Models;
using KubernetesClient.Informer.Client;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton<JobQueue>();

builder.Services.AddSingleton<IKubernetes>(_ =>
{
    KubernetesClientConfiguration config;
    try
    {
        config = KubernetesClientConfiguration.InClusterConfig();
    }
    catch (Exception)
    {
        // Fallback for running the client outside the cluster during local development.
        config = KubernetesClientConfiguration.BuildDefaultConfig();
    }

    return new Kubernetes(config);
});

// KubernetesClient.Informer's RegisterResourceInformer<T>() convenience extension has no way
// to pass a namespace (it always watches cluster-wide), which doesn't fit our namespace-scoped
// RBAC -- so this constructs the informer directly instead, scoped to POD_NAMESPACE.
var podNamespace = builder.Configuration["POD_NAMESPACE"] ?? "actors-demo";
builder.Services.AddSingleton<IResourceInformer<V1Pod>>(sp => new ResourceInformer<V1Pod>(
    sp.GetRequiredService<IKubernetes>(),
    sp.GetRequiredService<IHostApplicationLifetime>(),
    sp.GetRequiredService<ILogger<ResourceInformer<V1Pod>>>(),
    selector: null,
    @namespace: podNamespace));
builder.Services.AddHostedService(sp => (ResourceInformer<V1Pod>)sp.GetRequiredService<IResourceInformer<V1Pod>>());

builder.Services.AddHostedService<StatefulSetWatcher>();
builder.Services.AddHostedService<ActorInvokerWorker>();
builder.Services.AddHostedService<JobGeneratorWorker>();

var app = builder.Build();

app.MapGet("/healthz", () => Results.Ok(new { status = "ok" }));

app.MapGet("/metrics/desired-instances", (JobQueue jobQueue) =>
    Results.Ok(new { value = jobQueue.DesiredInstanceCount }));

app.MapPost("/jobs", (JobQueue jobQueue, int durationMs = 25_000) =>
{
    var job = jobQueue.Submit(durationMs);
    return Results.Ok(new { jobId = job.JobId, durationMs, desiredInstanceCount = jobQueue.DesiredInstanceCount });
});

app.MapGet("/status", (JobQueue jobQueue) => Results.Ok(new
{
    podCount = jobQueue.PodCount,
    pendingJobCount = jobQueue.PendingJobCount,
    activeJobCount = jobQueue.ActiveJobCount,
    desiredInstanceCount = jobQueue.DesiredInstanceCount,
    knownActorTypes = jobQueue.KnownActorTypes,
}));

app.Run();
