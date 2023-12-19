using MassTransit;
using Microsoft.Extensions.Logging;

namespace Meshmakers.Octo.Common.DistributionEventHub.Commands;

internal class DistributedConsumer<TConsumer, TMessage> : IConsumer<TMessage>
    where TConsumer : class, IDistributedConsumer<TMessage>
    where TMessage : class
{
    private readonly ILogger<DistributedConsumer<TConsumer, TMessage>> _logger;
    private readonly ILoggerFactory _loggerFactory;
    private readonly TConsumer _consumer;

    public DistributedConsumer(ILogger<DistributedConsumer<TConsumer, TMessage> > logger, ILoggerFactory loggerFactory, TConsumer consumer)
    {
        _logger = logger;
        _loggerFactory = loggerFactory;
        _consumer = consumer;
    }

    public Task Consume(ConsumeContext<TMessage> context)
    {
        _logger.LogInformation("Distributed consumer received message");
        var distributedContext = new DistributedContext<TMessage>(_loggerFactory.CreateLogger<DistributedContext<TMessage>>(), context);
        return _consumer.ConsumeAsync(distributedContext);
    }
}