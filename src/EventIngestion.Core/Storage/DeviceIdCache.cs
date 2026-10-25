namespace EventIngestion.Core.Storage;

using System.Text;

// Custom hash table that maps incoming UTF-8 byte spans to existing string objects.
// Avoids creating duplicate string objects on the heap for the same device ID.
public sealed class DeviceIdCache
{
    private sealed class Node
    {
        public readonly byte[] Utf8Bytes;
        public readonly string StringValue;
        public readonly Node? Next;

        public Node(byte[] bytes, string value, Node? next)
        {
            Utf8Bytes = bytes;
            StringValue = value;
            Next = next;
        }
    }

    private const int BucketCount = 32768;
    private const uint BucketMask = BucketCount - 1;

    private readonly Node?[] _buckets = new Node?[BucketCount];
    private readonly object _writeLock = new object();

    // Fast lock-free lookup for existing strings. Only locks if the device ID is new.
    public string GetOrAdd(ReadOnlySpan<byte> utf8Span)
    {
        uint hash = ComputeHash(utf8Span);
        int index = (int)(hash & BucketMask);

        Node? current = Volatile.Read(ref _buckets[index]);
        while (current is not null)
        {
            if (utf8Span.SequenceEqual(current.Utf8Bytes))
            {
                return current.StringValue;
            }
            current = current.Next;
        }

        return AddMiss(utf8Span, index);
    }

    // Creates and caches a new string under lock when seen for the first time.
    private string AddMiss(ReadOnlySpan<byte> utf8Span, int index)
    {
        lock (_writeLock)
        {
            Node? current = _buckets[index];
            while (current is not null)
            {
                if (utf8Span.SequenceEqual(current.Utf8Bytes))
                {
                    return current.StringValue;
                }
                current = current.Next;
            }

            string stringValue = Encoding.UTF8.GetString(utf8Span);
            byte[] bytesCopy = utf8Span.ToArray();
            Node newNode = new Node(bytesCopy, stringValue, _buckets[index]);
            Volatile.Write(ref _buckets[index], newNode);

            return stringValue;
        }
    }

    // Fast 32-bit FNV-1a hash algorithm to distribute device IDs evenly across buckets.
    private static uint ComputeHash(ReadOnlySpan<byte> bytes)
    {
        uint hash = 2166136261u;
        for (int i = 0; i < bytes.Length; i++)
        {
            hash = (hash ^ bytes[i]) * 16777619u;
        }
        return hash;
    }
}
