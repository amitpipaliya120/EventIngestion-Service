namespace EventIngestion.Tests.Helpers;

using EventIngestion.Core.Common;

/// <summary>
/// Controllable time provider for testing sliding window aging and time progression.
/// </summary>
public sealed class TestTimeProvider : ITimeProvider
{
    private DateTimeOffset _currentUtc;

    public TestTimeProvider(DateTimeOffset initialUtc)
    {
        _currentUtc = initialUtc;
    }

    public DateTimeOffset GetUtcNow() => _currentUtc;

    public long GetUtcNowMs() => _currentUtc.ToUnixTimeMilliseconds();

    public void Advance(TimeSpan delta)
    {
        _currentUtc = _currentUtc.Add(delta);
    }

    public void SetUtcNow(DateTimeOffset newUtc)
    {
        _currentUtc = newUtc;
    }
}
