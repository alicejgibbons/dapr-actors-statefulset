using System.Text.RegularExpressions;
using k8s;
using k8s.Models;
using KubernetesClient.Informer.Client;

namespace ActorClient.Services;

/// <summary>
/// Keeps <see cref="JobQueue"/>'s known-pod/actor-type map in sync with the
/// actor-host StatefulSet's pods, primarily via a Kubernetes informer
/// (<see cref="IResourceInformer{V1Pod}"/>, registered in Program.cs) that
/// delivers ADDED/MODIFIED/DELETED events in real time -- no polling needed
/// for the common case. Because the informer library has no built-in
/// periodic resync (only a relist on a dropped/expired watch), this also runs
/// its own defensive full reconciliation on a timer, as a belt-and-suspenders
/// backstop against a silently missed event.
///
/// This is what lets the single actor client discover new actor types
/// automatically as KEDA scales the StatefulSet up, and drop them again when
/// it scales down.
/// </summary>
public sealed class StatefulSetWatcher(
    IKubernetes kubernetes,
    IResourceInformer<V1Pod> informer,
    JobQueue jobQueue,
    IConfiguration configuration,
    ILogger<StatefulSetWatcher> logger) : BackgroundService
{
    private static readonly Regex OrdinalSuffix = new(@"-(\d+)$", RegexOptions.Compiled);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var podNamespace = configuration["POD_NAMESPACE"] ?? "actors-demo";
        var actorTypePrefix = configuration["ACTOR_TYPE_PREFIX"] ?? "WorkerActor";
        var (labelKey, labelValue) = ParseLabelSelector(configuration["ACTOR_HOST_LABEL_SELECTOR"] ?? "app=actor-host");
        var fullResyncSeconds = int.TryParse(configuration["FULL_RESYNC_INTERVAL_SECONDS"], out var r) ? r : 300;

        // The informer library's ResourceSelector only supports field selectors, not label
        // selectors, so it streams every Pod in the namespace -- we filter by label ourselves.
        using var registration = informer.Register((eventType, pod) =>
            HandlePodEvent(eventType, pod, labelKey, labelValue, actorTypePrefix));

        informer.StartWatching();
        await informer.ReadyAsync(stoppingToken);
        logger.LogInformation(
            "Informer ready; watching Pods (label {Key}={Value}) in namespace {Namespace}",
            labelKey, labelValue, podNamespace);

        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(TimeSpan.FromSeconds(fullResyncSeconds), stoppingToken);
            await FullResyncAsync(podNamespace, labelKey, labelValue, actorTypePrefix, stoppingToken);
        }
    }

    private void HandlePodEvent(WatchEventType eventType, V1Pod pod, string labelKey, string labelValue, string actorTypePrefix)
    {
        if (pod.Metadata?.Name is null
            || pod.Metadata.Labels is null
            || !pod.Metadata.Labels.TryGetValue(labelKey, out var value)
            || value != labelValue)
        {
            return;
        }

        var match = OrdinalSuffix.Match(pod.Metadata.Name);
        if (!match.Success)
        {
            return;
        }

        var ordinal = int.Parse(match.Groups[1].Value);
        var isReady = eventType != WatchEventType.Deleted && IsPodReady(pod);

        if (isReady)
        {
            var actorType = $"{actorTypePrefix}-{ordinal}";
            if (jobQueue.MarkPodKnown(ordinal, actorType))
            {
                logger.LogInformation(
                    "Informer: discovered actor-host-{Ordinal} hosting actor type {ActorType}", ordinal, actorType);
            }
        }
        else if (jobQueue.MarkPodGone(ordinal))
        {
            logger.LogInformation("Informer: actor-host-{Ordinal} is gone/not Ready; dropping its actor type", ordinal);
        }
    }

    private async Task FullResyncAsync(
        string podNamespace, string labelKey, string labelValue, string actorTypePrefix, CancellationToken cancellationToken)
    {
        try
        {
            var pods = await kubernetes.CoreV1.ListNamespacedPodAsync(
                podNamespace,
                labelSelector: $"{labelKey}={labelValue}",
                cancellationToken: cancellationToken);

            var observed = new Dictionary<int, string>();
            foreach (var pod in pods.Items)
            {
                if (pod.Metadata?.Name is null || !IsPodReady(pod))
                {
                    continue;
                }

                var match = OrdinalSuffix.Match(pod.Metadata.Name);
                if (match.Success)
                {
                    var ordinal = int.Parse(match.Groups[1].Value);
                    observed[ordinal] = $"{actorTypePrefix}-{ordinal}";
                }
            }

            var (added, removed) = jobQueue.SyncKnownPods(observed);

            if (added.Count > 0 || removed.Count > 0)
            {
                logger.LogWarning(
                    "Full resync found drift the informer missed: added={Added} removed={Removed}",
                    string.Join(',', added), string.Join(',', removed));
            }
            else
            {
                logger.LogDebug("Full resync: no drift from the informer's view");
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Full resync failed to list actor-host pods");
        }
    }

    private static bool IsPodReady(V1Pod pod) =>
        pod.Status?.Phase == "Running" && pod.Status?.ContainerStatuses?.All(c => c.Ready) == true;

    private static (string Key, string Value) ParseLabelSelector(string selector)
    {
        var parts = selector.Split('=', 2);
        return parts.Length == 2 ? (parts[0], parts[1]) : (selector, "");
    }
}
