using System.Collections.Concurrent;

namespace ActorClient.Services;

public enum JobStatus { Pending, Running, Completed, Failed }

public sealed class Job
{
    public required string JobId { get; init; }
    public required int SimulatedDurationMs { get; init; }
    public JobStatus Status { get; set; } = JobStatus.Pending;
    public int? AssignedOrdinal { get; set; }
}

public sealed record ActorInvocationJob(int Ordinal, string ActorTypeName, Job Job);

/// <summary>
/// The client's single job queue. It:
///  1. Tracks the set of pods currently in the actor-host StatefulSet (kept in
///     sync by <see cref="StatefulSetWatcher"/>) -- PodCount is exactly "the
///     number of pods in the StatefulSet" the client currently knows about.
///  2. Queues real jobs and assigns each to an idle known actor type/pod.
///     DesiredInstanceCount is simply "jobs still pending or running" -- when
///     a job finishes (see <see cref="CompleteJob"/>), its slot is freed
///     immediately, the count drops, and KEDA scales that pod back down. No
///     manual reset is needed: scale-down is a direct consequence of jobs
///     finishing.
/// </summary>
public sealed class JobQueue
{
    public const int MaxOutstandingJobs = 8;

    private readonly ConcurrentDictionary<int, string> _knownActorTypesByOrdinal = new();
    private readonly ConcurrentQueue<Job> _pendingJobs = new();
    private readonly ConcurrentDictionary<int, Job> _activeJobsByOrdinal = new();

    public int PodCount => _knownActorTypesByOrdinal.Count;

    public IReadOnlyDictionary<int, string> KnownActorTypes => _knownActorTypesByOrdinal;

    public int PendingJobCount => _pendingJobs.Count;

    public int ActiveJobCount => _activeJobsByOrdinal.Count;

    /// <summary>Jobs pending + jobs actively running. Drops to 0 once everything finishes; the
    /// StatefulSet's minReplicaCount then floors actual replicas at 1 regardless.</summary>
    public int DesiredInstanceCount => PendingJobCount + ActiveJobCount;

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
            if (observedOrdinalToActorType.ContainsKey(ordinal) || !_knownActorTypesByOrdinal.TryRemove(ordinal, out _))
            {
                continue;
            }

            removed.Add(ordinal);

            // The pod backing this ordinal disappeared mid-job (e.g. it was evicted) --
            // put the job back in the queue instead of losing it.
            if (_activeJobsByOrdinal.TryRemove(ordinal, out var orphanedJob))
            {
                orphanedJob.Status = JobStatus.Pending;
                orphanedJob.AssignedOrdinal = null;
                _pendingJobs.Enqueue(orphanedJob);
            }
        }

        return (added, removed);
    }

    public Job Submit(int simulatedDurationMs)
    {
        var job = new Job
        {
            JobId = Guid.NewGuid().ToString("N")[..8],
            SimulatedDurationMs = simulatedDurationMs,
        };
        _pendingJobs.Enqueue(job);
        return job;
    }

    /// <summary>Assigns as many pending jobs as possible to known ordinals that aren't already running one.</summary>
    public IReadOnlyList<ActorInvocationJob> AssignPendingJobs()
    {
        var assigned = new List<ActorInvocationJob>();

        foreach (var (ordinal, actorType) in _knownActorTypesByOrdinal)
        {
            if (_activeJobsByOrdinal.ContainsKey(ordinal))
            {
                continue;
            }

            if (!_pendingJobs.TryDequeue(out var job))
            {
                break;
            }

            job.Status = JobStatus.Running;
            job.AssignedOrdinal = ordinal;
            _activeJobsByOrdinal[ordinal] = job;
            assigned.Add(new ActorInvocationJob(ordinal, actorType, job));
        }

        return assigned;
    }

    /// <summary>Frees the ordinal's slot -- the job is done (or failed), so it no longer counts toward demand.</summary>
    public void CompleteJob(int ordinal, bool succeeded)
    {
        if (_activeJobsByOrdinal.TryRemove(ordinal, out var job))
        {
            job.Status = succeeded ? JobStatus.Completed : JobStatus.Failed;
        }
    }
}
