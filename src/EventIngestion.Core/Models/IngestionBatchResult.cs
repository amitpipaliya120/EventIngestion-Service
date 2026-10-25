namespace EventIngestion.Core.Models;

// Response returned after processing a batch, showing accepted vs dropped count.
public sealed record IngestionBatchResult(
    int Accepted,
    int Dropped = 0
);
