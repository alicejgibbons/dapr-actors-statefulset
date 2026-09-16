using ActorClient.Services;
using k8s;

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
