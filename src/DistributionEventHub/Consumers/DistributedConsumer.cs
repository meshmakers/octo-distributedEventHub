using MassTransit;
using Meshmakers.Octo.Common.DistributionEventHub.Services;
using Microsoft.Extensions.Logging;

namespace Meshmakers.Octo.Common.DistributionEventHub.Consumers;

// ReSharper disable once ClassNeverInstantiated.Global
internal class DistributedConsumer<TConsumer, TMessage>(
    ILogger<DistributedConsumer<TConsumer, TMessage>> logger,
    ILoggerFactory loggerFactory,
    IDistributionEventHubService eventHub,
    TConsumer consumer)
    : IConsumer<TMessage>
    where TConsumer : class, IDistributedConsumer<TMessage>
    where TMessage : class
{
    public Task Consume(ConsumeContext<TMessage> context)
    {
        logger.LogInformation("Received message of type '{MessageType}'", typeof(TMessage).Name);
        var distributedContext = new DistributedContext<TMessage>(loggerFactory.CreateLogger<DistributedContext<TMessage>>(), context, eventHub);
        return consumer.ConsumeAsync(distributedContext);
    }
}