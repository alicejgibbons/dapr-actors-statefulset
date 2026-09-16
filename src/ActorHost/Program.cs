using ActorHost;

var builder = WebApplication.CreateBuilder(args);

var actorTypePrefix = builder.Configuration["ACTOR_TYPE_PREFIX"] ?? "WorkerActor";
var identity = PodIdentity.FromEnvironment(actorTypePrefix);

builder.Services.AddSingleton(identity);

builder.Services.AddActors(options =>
{
    options.Actors.RegisterActor<WorkerActor>(identity.ActorTypeName);
    options.ActorIdleTimeout = TimeSpan.FromMinutes(10);
    options.ActorScanInterval = TimeSpan.FromSeconds(30);
    options.DrainOngoingCallTimeout = TimeSpan.FromSeconds(30);
    options.DrainRebalancedActors = true;
});

var app = builder.Build();

app.MapActorsHandlers();

app.MapGet("/healthz", () => Results.Ok(new
{
    podName = identity.PodName,
    ordinal = identity.Ordinal,
    actorType = identity.ActorTypeName,
}));

app.Logger.LogInformation(
    "Starting ActorHost. Pod={PodName} Ordinal={Ordinal} ActorType={ActorType}",
    identity.PodName, identity.Ordinal, identity.ActorTypeName);

app.Run();
