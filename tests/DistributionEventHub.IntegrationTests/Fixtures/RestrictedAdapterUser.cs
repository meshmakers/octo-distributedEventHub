using DotNet.Testcontainers.Containers;

namespace Meshmakers.Octo.Common.DistributionEventHub.IntegrationTests.Fixtures;

/// <summary>
///     The restricted broker user of adapters and edge sites (AB#7338): everything on vhost <c>/</c> except resources whose
///     name contains <c>octo::method::</c> (the CK method queues, their send exchanges and endpoint exchanges, see
///     <c>CkMethodQueueNames.Prefix</c> in octo-common-services). RabbitMQ matches the permission patterns unanchored; the
///     negative lookahead has to be at the start.
/// </summary>
/// <remarks>
///     The same three patterns are deployed by meshmakers-infrastructure (role <c>k8s-infrastructure</c>, task
///     <c>rabbitmq.yml</c>, variable <c>rabbitmq_adapter_user_permissions</c>) and by octo-tools (local broker). Change them in
///     all three places together.
/// </remarks>
public static class RestrictedAdapterUser
{
    public const string Name = "octo-adapter-mq-user";

    public const string Configure = @"^(?!.*octo::method::).*$";
    public const string Write = @"^(?!amq[.]default$)(?!.*octo::method::).*$";
    public const string Read = @"^(?!.*octo::method::).*$";

    public static async Task CreateAsync(IContainer container, string password)
    {
        await RunAsync(container, "rabbitmqctl", "add_user", Name, password);
        await RunAsync(container, "rabbitmqctl", "set_permissions", "-p", "/", Name, Configure, Write, Read);
    }

    private static async Task RunAsync(IContainer container, params string[] command)
    {
        var result = await container.ExecAsync(command);
        if (result.ExitCode != 0)
        {
            throw new InvalidOperationException(
                $"'{command[0]} {command[1]}' failed in the broker container (exit {result.ExitCode}): {result.Stderr}");
        }
    }
}
