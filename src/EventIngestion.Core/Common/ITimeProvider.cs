namespace EventIngestion.Core.Common;

// Interface for getting current time.
// Used so we can fast-forward time in unit tests.
public interface ITimeProvider
{
    // Returns the current UTC date and time.
    DateTimeOffset GetUtcNow();

    // Returns the current UTC time in milliseconds for fast calculations.
    long GetUtcNowMs();
}

// Uses the real computer clock to get current time.
public sealed class SystemTimeProvider : ITimeProvider
{
    public static readonly SystemTimeProvider Instance = new SystemTimeProvider();

    // Returns current system UTC time.
    public DateTimeOffset GetUtcNow()
    {
        return DateTimeOffset.UtcNow;
    }

    // Returns current system UTC time in milliseconds.
    public long GetUtcNowMs()
    {
        return DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
    }
}
