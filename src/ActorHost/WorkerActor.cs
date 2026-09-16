using ActorContracts;
using Dapr.Actors.Runtime;

namespace ActorHost;

/// <summary>
/// Single generic actor implementation. What makes each pod host a distinct
/// "actor type" is not this class -- it's the type name it gets registered
/// under at startup (see Program.cs / PodIdentity), which is derived from the
/// pod's own StatefulSet ordinal.
/// </summary>
public sealed class WorkerActor : Actor, IWorkerActor
{
    private const string InvocationCountKey = "invocationCount";

    private readonly PodIdentity _identity;

    public WorkerActor(Dapr.Actors.Runtime.ActorHost host, PodIdentity identity) : base(host)
    {
        _identity = identity;
    }

    public async Task<WorkResult> DoWorkAsync(WorkItem item)
    {
        var count = await StateManager.TryGetStateAsync<int>(InvocationCountKey) switch
        {
            { HasValue: true } result => result.Value + 1,
            _ => 1,
        };

        await StateManager.SetStateAsync(InvocationCountKey, count);
        await StateManager.SaveStateAsync();

        if (item.SimulatedDurationMs > 0)
        {
            await Task.Delay(item.SimulatedDurationMs);
        }

        return new WorkResult(
            item.JobId,
            _identity.ActorTypeName,
            Id.GetId(),
            _identity.PodName,
            count,
            DateTimeOffset.UtcNow);
    }

    public async Task<ActorInfo> GetInfoAsync()
    {
        var result = await StateManager.TryGetStateAsync<int>(InvocationCountKey);
        var count = result.HasValue ? result.Value : 0;

        return new ActorInfo(_identity.ActorTypeName, _identity.Ordinal, _identity.PodName, count);
    }
}
