using System.Text;
using System.Text.Json;
using FluentAssertions;
using Meshmakers.Octo.Common.DistributionEventHub.Services;
using RabbitMQ.Client;
using Xunit;

namespace Meshmakers.Octo.Common.DistributionEventHub.UnitTests.Services;

/// <summary>
///     Choice of the legacy command bridge's classic probe (AB#8408): the error-free mandatory publish only while bound,
///     the passive declare in every other state and after the broker refused the publish.
/// </summary>
[Trait("Category", "Unit")]
public class LegacyClassicProbeTests
{
    [Fact]
    public void Bound_UsesTheMandatoryPublish()
    {
        LegacyClassicProbe.Select(LegacyCommandBridgeState.Bound, publishProbeUnavailable: false)
            .Should().Be(LegacyClassicProbeKind.MandatoryPublish);
    }

    // the state enum is internal, so the theories take its value
    [Theory]
    [InlineData((int)LegacyCommandBridgeState.Unknown)]
    [InlineData((int)LegacyCommandBridgeState.LegacyConsumerPresent)]
    [InlineData((int)LegacyCommandBridgeState.SharedQueueMissing)]
    [InlineData((int)LegacyCommandBridgeState.Faulted)]
    public void EveryOtherState_UsesThePassiveDeclare_SoALiveClassicConsumerNeverReceivesProbes(
        int state)
    {
        LegacyClassicProbe.Select((LegacyCommandBridgeState)state, publishProbeUnavailable: false)
            .Should().Be(LegacyClassicProbeKind.PassiveDeclare);
    }

    // the state enum is internal, so the theories take its value
    [Theory]
    [InlineData((int)LegacyCommandBridgeState.Unknown)]
    [InlineData((int)LegacyCommandBridgeState.LegacyConsumerPresent)]
    [InlineData((int)LegacyCommandBridgeState.Bound)]
    [InlineData((int)LegacyCommandBridgeState.SharedQueueMissing)]
    [InlineData((int)LegacyCommandBridgeState.Faulted)]
    public void PublishRefusedBefore_AlwaysUsesThePassiveDeclare(int state)
    {
        LegacyClassicProbe.Select((LegacyCommandBridgeState)state, publishProbeUnavailable: true)
            .Should().Be(LegacyClassicProbeKind.PassiveDeclare);
    }

    [Fact]
    public void Properties_TransientAndExpireAtOnce()
    {
        var id = Guid.NewGuid();

        var properties = LegacyClassicProbe.CreateProperties(id);

        properties.DeliveryMode.Should().Be(DeliveryModes.Transient);
        properties.Expiration.Should().Be("0", "a probe that cannot be delivered at once must not stay in a classic queue");
        properties.ContentType.Should().Be("application/vnd.masstransit+json");
        properties.MessageId.Should().Be(id.ToString("D"));
        properties.Type.Should().Be(LegacyClassicProbe.MessageTypeUrn);
    }

    [Fact]
    public void Body_IsAMassTransitEnvelopeOfTheUnhandledProbeType()
    {
        var id = Guid.NewGuid();

        using var json = JsonDocument.Parse(Encoding.UTF8.GetString(LegacyClassicProbe.CreateBody(id)));

        json.RootElement.GetProperty("messageId").GetString().Should().Be(id.ToString("D"));
        json.RootElement.GetProperty("messageType").EnumerateArray().Select(e => e.GetString())
            .Should().Equal(LegacyClassicProbe.MessageTypeUrn);
        json.RootElement.GetProperty("message").ValueKind.Should().Be(JsonValueKind.Object);
    }
}
