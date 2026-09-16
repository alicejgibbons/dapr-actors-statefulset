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
builder.Services.AddHostedService<DemandGeneratorWorker>();

var app = builder.Build();

app.MapGet("/healthz", () => Results.Ok(new { status = "ok" }));

app.MapGet("/metrics/desired-instances", (JobQueue jobQueue) =>
    Results.Ok(new { value = jobQueue.DesiredInstanceCount }));

app.MapPost("/demand/add", (JobQueue jobQueue, int by = 1) =>
{
    var updated = jobQueue.IncreaseDemand(by);
    return Results.Ok(new { desiredInstanceCount = updated });
});

app.MapPost("/demand/reset", (JobQueue jobQueue) =>
{
    var updated = jobQueue.ResetDemand();
    return Results.Ok(new { desiredInstanceCount = updated });
});

app.MapGet("/status", (JobQueue jobQueue) => Results.Ok(new
{
    podCount = jobQueue.PodCount,
    desiredInstanceCount = jobQueue.DesiredInstanceCount,
    knownActorTypes = jobQueue.KnownActorTypes,
}));

app.Run();
