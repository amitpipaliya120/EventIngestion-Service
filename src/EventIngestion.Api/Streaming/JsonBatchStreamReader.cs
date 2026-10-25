namespace EventIngestion.Api.Streaming;

using System.Buffers;
using System.IO.Pipelines;
using System.Text.Json;
using EventIngestion.Core.Models;
using EventIngestion.Core.Storage;

// High-speed parser that streams JSON readings directly from the network pipe without creating a List.
public static class JsonBatchStreamReader
{
    private static readonly byte[] DeviceIdProp = "deviceId"u8.ToArray();
    private static readonly byte[] TimestampProp = "timestamp"u8.ToArray();
    private static readonly byte[] ValueProp = "value"u8.ToArray();

    // Reads the HTTP byte stream chunk by chunk, parsing readings and inserting them directly into storage.
    public static async Task<IngestionBatchResult> ReadBatchAsync(
        PipeReader reader,
        IDeviceStore store,
        DeviceIdCache cache,
        CancellationToken cancellationToken = default)
    {
        int accepted = 0;
        int dropped = 0;

        JsonReaderState readerState = default;
        bool isInsideObject = false;

        string? currentDeviceId = null;
        long currentTimestampUtcMs = 0;
        bool hasTimestamp = false;
        double currentValue = 0.0;
        bool hasValue = false;
        byte[]? currentProperty = null;

        while (true)
        {
            ReadResult result = await reader.ReadAsync(cancellationToken);
            ReadOnlySequence<byte> buffer = result.Buffer;

            if (buffer.IsEmpty && result.IsCompleted)
            {
                break;
            }

            var jsonReader = new Utf8JsonReader(buffer, isFinalBlock: result.IsCompleted, readerState);

            try
            {
                while (jsonReader.Read())
                {
                    switch (jsonReader.TokenType)
                    {
                        case JsonTokenType.StartArray:
                        case JsonTokenType.EndArray:
                            break;

                        case JsonTokenType.StartObject:
                            isInsideObject = true;
                            currentDeviceId = null;
                            currentTimestampUtcMs = 0;
                            hasTimestamp = false;
                            currentValue = 0.0;
                            hasValue = false;
                            currentProperty = null;
                            break;

                        case JsonTokenType.PropertyName:
                            ReadOnlySpan<byte> propSpan = jsonReader.HasValueSequence
                                ? jsonReader.ValueSequence.ToArray()
                                : jsonReader.ValueSpan;

                            if (propSpan.SequenceEqual(DeviceIdProp))
                            {
                                currentProperty = DeviceIdProp;
                            }
                            else if (propSpan.SequenceEqual(TimestampProp))
                            {
                                currentProperty = TimestampProp;
                            }
                            else if (propSpan.SequenceEqual(ValueProp))
                            {
                                currentProperty = ValueProp;
                            }
                            else
                            {
                                currentProperty = null;
                            }
                            break;

                        case JsonTokenType.String:
                            if (currentProperty == DeviceIdProp)
                            {
                                ReadOnlySpan<byte> valSpan = jsonReader.HasValueSequence
                                    ? jsonReader.ValueSequence.ToArray()
                                    : jsonReader.ValueSpan;

                                currentDeviceId = cache.GetOrAdd(valSpan);
                            }
                            else if (currentProperty == TimestampProp)
                            {
                                if (jsonReader.TryGetDateTimeOffset(out DateTimeOffset dto))
                                {
                                    currentTimestampUtcMs = dto.ToUnixTimeMilliseconds();
                                    hasTimestamp = true;
                                }
                            }
                            currentProperty = null;
                            break;

                        case JsonTokenType.Number:
                            if (currentProperty == ValueProp)
                            {
                                if (jsonReader.TryGetDouble(out double val))
                                {
                                    currentValue = val;
                                    hasValue = true;
                                }
                            }
                            currentProperty = null;
                            break;

                        case JsonTokenType.EndObject:
                            if (isInsideObject && currentDeviceId is not null && hasTimestamp && hasValue)
                            {
                                ReadingEntry entry = new ReadingEntry(currentTimestampUtcMs, currentValue);
                                if (store.IngestReading(currentDeviceId, entry))
                                {
                                    accepted++;
                                }
                                else
                                {
                                    dropped++;
                                }
                            }
                            isInsideObject = false;
                            break;
                    }
                }
            }
            finally
            {
                readerState = jsonReader.CurrentState;
            }

            reader.AdvanceTo(buffer.GetPosition(jsonReader.BytesConsumed), buffer.End);

            if (result.IsCompleted)
            {
                break;
            }
        }

        return new IngestionBatchResult(Accepted: accepted, Dropped: dropped);
    }
}
