namespace Meshmakers.Octo.Common.DistributionEventHub.IntegrationTests.Fixtures;

public static class TestHelpers
{
    public static async Task WaitForCondition(
        Func<bool> condition,
        TimeSpan timeout,
        TimeSpan? pollInterval = null)
    {
        var interval = pollInterval ?? TimeSpan.FromMilliseconds(100);
        var deadline = DateTime.UtcNow + timeout;

        while (!condition() && DateTime.UtcNow < deadline)
        {
            await Task.Delay(interval);
        }

        if (!condition())
        {
            throw new TimeoutException($"Condition not met within {timeout.TotalSeconds} seconds");
        }
    }

    public static async Task<T> WaitForResult<T>(
        Func<T?> getter,
        TimeSpan timeout,
        TimeSpan? pollInterval = null) where T : class
    {
        var interval = pollInterval ?? TimeSpan.FromMilliseconds(100);
        var deadline = DateTime.UtcNow + timeout;

        while (DateTime.UtcNow < deadline)
        {
            var result = getter();
            if (result != null) return result;
            await Task.Delay(interval);
        }

        throw new TimeoutException($"Result not available within {timeout.TotalSeconds} seconds");
    }

    public static async Task WaitForConditionAsync(
        Func<Task<bool>> condition,
        TimeSpan timeout,
        TimeSpan? pollInterval = null)
    {
        var interval = pollInterval ?? TimeSpan.FromMilliseconds(100);
        var deadline = DateTime.UtcNow + timeout;

        while (DateTime.UtcNow < deadline)
        {
            if (await condition())
                return;
            await Task.Delay(interval);
        }

        throw new TimeoutException($"Condition not met within {timeout.TotalSeconds} seconds");
    }
}
