namespace ActorContracts;

// Plain classes with a parameterless constructor and settable properties --
// Dapr's actor remoting client serializes these with DataContractSerializer,
// which (without explicit [DataContract]/[DataMember] attributes) requires a
// public parameterless constructor; C# records with primary constructors
// don't have one, so they fail to serialize at invocation time.

public class WorkItem
{
    public string JobId { get; set; } = "";
    public int SimulatedDurationMs { get; set; } = 250;

    public WorkItem() { }

    public WorkItem(string jobId, int simulatedDurationMs = 250)
    {
        JobId = jobId;
        SimulatedDurationMs = simulatedDurationMs;
    }
}

public class WorkResult
{
    public string JobId { get; set; } = "";
    public string ActorType { get; set; } = "";
    public string ActorId { get; set; } = "";
    public string PodName { get; set; } = "";
    public int InvocationCount { get; set; }
    public DateTimeOffset ProcessedAtUtc { get; set; }

    public WorkResult() { }

    public WorkResult(string jobId, string actorType, string actorId, string podName, int invocationCount, DateTimeOffset processedAtUtc)
    {
        JobId = jobId;
        ActorType = actorType;
        ActorId = actorId;
        PodName = podName;
        InvocationCount = invocationCount;
        ProcessedAtUtc = processedAtUtc;
    }
}

public class ActorInfo
{
    public string ActorType { get; set; } = "";
    public int Ordinal { get; set; }
    public string PodName { get; set; } = "";
    public int InvocationCount { get; set; }

    public ActorInfo() { }

    public ActorInfo(string actorType, int ordinal, string podName, int invocationCount)
    {
        ActorType = actorType;
        Ordinal = ordinal;
        PodName = podName;
        InvocationCount = invocationCount;
    }
}
