using MassTransit;
using Microsoft.Extensions.Logging;

namespace Meshmakers.Octo.Common.DistributionEventHub.Consumers;

internal class DistributedContext<TMessage> : IDistributedContext<TMessage>
    where TMessage : class
{
    private readonly ILogger<DistributedContext<TMessage>> _logger;
    private readonly ConsumeContext<TMessage> _context;

    public DistributedContext(ILogger<DistributedContext<TMessage>> logger, ConsumeContext<TMessage> context)
    {
        _logger = logger;
        _context = context;
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
        return _context.Publish(message);
    }
}