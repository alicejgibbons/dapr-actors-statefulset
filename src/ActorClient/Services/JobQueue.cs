using System.Collections.Concurrent;
using System.Threading.Channels;

namespace ActorClient.Services;

public sealed record ActorInvocationJob(int Ordinal, string ActorTypeName);

/// <summary>
/// The client's single job queue. It does two things:
///  1. Tracks the set of pods currently in the actor-host StatefulSet (kept in
///     sync by <see cref="StatefulSetWatcher"/>) -- PodCount below is exactly
///     "the number of pods in the StatefulSet" the client currently knows about.
///  2. Queues actor-invocation jobs, one per known pod/actor type, that
///     <see cref="ActorInvokerWorker"/> drains and dispatches through Dapr.
/// It also holds the "desired instance count" demand signal that KEDA polls
/// via /metrics/desired-instances to decide how far to scale the StatefulSet.
/// </summary>
public sealed class JobQueue
{
    private readonly ConcurrentDictionary<int, string> _knownActorTypesByOrdinal = new();
    private readonly Channel<ActorInvocationJob> _channel = Channel.CreateUnbounded<ActorInvocationJob>();
    private int _desiredInstanceCount = 1;

    public const int MaxDesiredInstances = 8;

    public int PodCount => _knownActorTypesByOrdinal.Count;

    public IReadOnlyDictionary<int, string> KnownActorTypes => _knownActorTypesByOrdinal;

    public int DesiredInstanceCount => Volatile.Read(ref _desiredInstanceCount);

    /// <summary>Reconciles the known-pods map against what was just observed in Kubernetes.</summary>
    public (IReadOnlyList<int> added, IReadOnlyList<int> removed) SyncKnownPods(
        IReadOnlyDictionary<int, string> observedOrdinalToActorType)
    {
        var added = new List<int>();
        var removed = new List<int>();

        foreach (var (ordinal, actorType) in observedOrdinalToActorType)
        {
            if (_knownActorTypesByOrdinal.TryAdd(ordinal, actorType))
            {
                added.Add(ordinal);
            }
        }

        foreach (var ordinal in _knownActorTypesByOrdinal.Keys)
        {
            if (!observedOrdinalToActorType.ContainsKey(ordinal) && _knownActorTypesByOrdinal.TryRemove(ordinal, out _))
            {
                removed.Add(ordinal);
            }
        }

        return (added, removed);
    }

    /// <summary>Adjusts desired instance count by <paramref name="by"/> (negative to reduce demand), clamped to [1, MaxDesiredInstances].</summary>
    public int IncreaseDemand(int by = 1)
    {
        int updated;
        int current;
        do
        {
            current = Volatile.Read(ref _desiredInstanceCount);
            updated = Math.Clamp(current + by, 1, MaxDesiredInstances);
        } while (Interlocked.CompareExchange(ref _desiredInstanceCount, updated, current) != current);

        return updated;
    }

    /// <summary>Hard-resets desired instance count back to 1 -- handy for demoing scale-down.</summary>
    public int ResetDemand()
    {
        Volatile.Write(ref _desiredInstanceCount, 1);
        return 1;
    }

    public bool TryEnqueue(ActorInvocationJob job) => _channel.Writer.TryWrite(job);

    public IAsyncEnumerable<ActorInvocationJob> DequeueAllAsync(CancellationToken cancellationToken) =>
        _channel.Reader.ReadAllAsync(cancellationToken);
}
