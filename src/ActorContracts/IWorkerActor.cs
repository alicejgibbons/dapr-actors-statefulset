using Dapr.Actors;

namespace ActorContracts;

/// <summary>
/// Contract implemented by every actor hosted on the actor-host StatefulSet.
/// One running pod registers exactly one dynamically-named actor type
/// (e.g. "WorkerActor-0", "WorkerActor-1", ...) against this same interface.
/// </summary>
public interface IWorkerActor : IActor
{
    Task<WorkResult> DoWorkAsync(WorkItem item);

    Task<ActorInfo> GetInfoAsync();
}
