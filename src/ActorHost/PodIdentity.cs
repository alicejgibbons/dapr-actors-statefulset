using System.Text.RegularExpressions;

namespace ActorHost;

/// <summary>
/// Derives this pod's StatefulSet ordinal from its own hostname (which, for a
/// StatefulSet, is always "&lt;statefulset-name&gt;-&lt;ordinal&gt;") and computes
/// the single dynamically-named actor type this pod will register.
/// </summary>
public sealed record PodIdentity(string PodName, int Ordinal, string ActorTypeName)
{
    public static PodIdentity FromEnvironment(string actorTypePrefix)
    {
        var podName = Environment.GetEnvironmentVariable("POD_NAME")
            ?? Environment.GetEnvironmentVariable("HOSTNAME")
            ?? Environment.MachineName;

        var match = Regex.Match(podName, @"-(\d+)$");
        var ordinal = match.Success ? int.Parse(match.Groups[1].Value) : 0;

        return new PodIdentity(podName, ordinal, $"{actorTypePrefix}-{ordinal}");
    }
}
