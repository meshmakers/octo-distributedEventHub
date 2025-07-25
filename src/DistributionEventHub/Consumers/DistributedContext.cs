using MassTransit;
using Meshmakers.Octo.Common.DistributionEventHub.Services;
using Microsoft.Extensions.Logging;

namespace Meshmakers.Octo.Common.DistributionEventHub.Consumers;

internal class DistributedContext<TMessage> : IDistributedContext<TMessage>
    where TMessage : class
{
    private readonly ConsumeContext<TMessage> _context;
    private readonly ILogger<DistributedContext<TMessage>> _logger;
    private readonly IDistributionEventHubService _eventHub;

    public DistributedContext(ILogger<DistributedContext<TMessage>> logger, ConsumeContext<TMessage> context, IDistributionEventHubService eventHub)
    {
        _logger = logger;
        _context = context;
        _eventHub = eventHub;
    }

    public TMessage Message => _context.Message;

    public Task RespondAsync<T>(T message) where T : class
    {
        _logger.LogInformation("{TMessage}: Responding to message with message type '{T}'", typeof(TMessage), typeof(T));
        return _context.RespondAsync(message);
    }

    public Task PublishAsync<T>(T message) where T : class
    {
        _logger.LogInformation("{TMessage}: Publish message with {T}", typeof(TMessage), typeof(T));
        // Use IDistributionEventHubService for proper instance prefix handling
        return _eventHub.PublishAsync(message);
    }
}