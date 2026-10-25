# System Design Document

**Project:** High-Throughput Event Ingestion Service (.NET 10)  
**Author:** Amit Pipaliya  

---

## 1. The Data Structures Chosen and Why

To handle up to ~10,000 distinct IoT devices emitting batches of up to 50,000 readings without external databases, I structured storage into three purpose-built components:

### 1. `ConcurrentDictionary<string, DeviceState>` (Fleet Registry)
- **Why chosen:** The system tracks up to 10,000 distinct devices. A thread-safe dictionary allows requests writing to different devices to run concurrently with zero contention. It provides lock-free reads (`TryGetValue`) and thread-safe additions (`GetOrAdd`).
- **Memory optimization:** The keys are interned string references retrieved from `DeviceIdCache`, preventing duplicate string allocations for the same device ID.

### 2. `DeviceCircularBuffer` with `ReadingEntry[]` (Per-Device Window)
Each device maintains its active readings in a timestamp-sorted circular ring buffer. Each reading is an unmanaged 16-byte struct:
```csharp
[StructLayout(LayoutKind.Sequential)]
public readonly record struct ReadingEntry(long TimestampUtcMs, double Value);
```
- **Why chosen over `List<T>`, `LinkedList<T>`, or `Queue<T>`:**
  - **Zero per-reading heap allocations:** Structs are stored directly in contiguous array memory allocated once per device. A linked list would allocate a 48-byte node object per reading, adding enormous Garbage Collection overhead.
  - **O(1) Head Eviction:** As time advances, expired readings are evicted from the front (`head`) by incrementing an index (`_head = (_head + 1) % _entries.Length`). This takes a few nanoseconds and requires zero memory shifting.
  - **CPU Cache Locality:** Scanning or iterating contiguous array elements is fast because modern CPU L1/L2 caches prefetch adjacent memory.
  - **Running Aggregates:** The buffer maintains rolling `_sum` and `_count` fields. Calculating the average on query takes $O(1)$ time (`_sum / _count`).

### 3. `DeviceIdCache` (UTF-8 String Interning)
- **Why chosen:** Devices repeatedly send their IDs as raw UTF-8 bytes in JSON (e.g. `"sensor-0042"`). Calling `Encoding.UTF8.GetString(...)` for 50,000 items creates 50,000 duplicate string objects on the heap.
- `DeviceIdCache` uses a 32,768-bucket lock-free hash table over `ReadOnlySpan<byte>` using 32-bit FNV-1a hashing. Once a device ID is cached on its first reading, all subsequent readings reuse the existing string instance with **zero heap allocations**.

---

## 2. How Allocations Were Minimized on the Ingestion Path

Ingestion is the hot path. Under batches of up to 50,000 readings, standard deserialization creates millions of short-lived objects, triggering frequent Gen 1 and Gen 2 Garbage Collection pauses that freeze the application.

Allocations were eliminated using the following techniques:

### 1. Direct Stream Processing (`PipeReader` + `Utf8JsonReader`)
- We bypass `JsonSerializer.Deserialize<List<Reading>>()`, which would materialize 50,000 C# objects in memory.
- Instead, we read directly from ASP.NET Core's raw socket stream (`Request.BodyReader`).
- `Utf8JsonReader` processes byte buffers sequentially. As each reading is parsed, it is inserted directly into the device's ring buffer.
- Payload memory is bounded by Kestrel's ~32 KB pipeline buffer, regardless of whether the incoming JSON is 500 KB or 20 MB.

### 2. 16-Byte Unmanaged Value Types (`ReadingEntry`)
- Each reading consists of a `long TimestampUtcMs` (8 bytes) and a `double Value` (8 bytes).
- Storing value types directly inside pre-allocated arrays produces zero GC handles and zero object header overhead.

### 3. Elimination of Closure Allocations
- When resolving devices in `InMemoryDeviceStore.IngestReading`, `GetOrAdd` uses a named static method delegate instead of a capturing lambda. This avoids allocating a compiler display class on every reading.

### 4. Benchmark Validation
Comparing a 50,000-reading batch in BenchmarkDotNet:
- **Standard `JsonSerializer`:** Allocated **3,368 KB (3.4 MB)** and triggered **125 Gen 1/Gen 2 GC collections**.
- **Our Streaming Parser:** Allocated **21.29 KB** (over **99% reduction**) and triggered **0 GC collections**.

---

## 3. The Concurrency Model and Why It Is Correct

### 1. Fine-Grained Locking vs. Single Global Lock
- **Why not a global lock:** A single lock would force all 10,000 devices to wait in a single queue, eliminating multi-core parallelism.
- **Fine-Grained Monitor Lock:** Each `DeviceState` contains its own dedicated lock object (`_syncRoot`).
  - Writes to different devices (e.g., `sensor-01` and `sensor-02`) run completely in parallel without blocking each other.
  - If two threads write to the *same* device, they synchronize briefly on that device's private monitor lock (~15 nanoseconds per insert).

### 2. Why `Monitor` (`lock`) over `ReaderWriterLockSlim`
- `ReaderWriterLockSlim` has higher acquisition overhead (~40-60ns) and allocates small internal state objects on the heap.
- Our aggregate queries complete in sub-microsecond time because running totals are maintained in memory. The minor theoretical benefit of concurrent readers on the same device does not justify the constant CPU and allocation cost of reader-writer locks.

### 3. Correctness Guarantees
- **No Lost Updates:** Every buffer insertion, sum update, and count increment occurs inside `lock (_syncRoot)`. Simultaneous requests for the same device queue sequentially, ensuring no reading is dropped or overwritten.
- **No Torn Reads:** `TryGetAggregates` acquires `_syncRoot`, prunes aged readings, reads the current count/min/max/average, and snapshots them into an immutable struct before releasing the lock. Readers never observe partially updated data.
- **Safe Device Eviction:** Background cleanup removes inactive devices using `_devices.TryRemove(KeyValuePair<string, DeviceState>)`. This atomic check ensures the device is only removed if no new writes arrived between the idle check and removal.

---

## 4. The Windowing Approach and Its Tradeoffs

### 1. Continuous Sliding Window vs. Bucketed Approximation
The specification requires aggregates over a sliding window of the last 5 minutes.

- **Continuous Sliding Window (Implemented):**
  - **Exact precision:** Readings expire at the exact millisecond of `now - 300,000 ms`.
  - **No boundary skew:** Fixed buckets (e.g., five 1-minute buckets) cause stair-step drops where an entire minute of data suddenly vanishes at bucket boundaries.
  - **Memory efficiency:** The ring buffer dynamically holds only the readings that actually occurred, avoiding pre-allocating hundreds of empty time buckets for inactive devices.

### 2. Out-of-Order Reading Handling
Readings may arrive slightly out of order due to network jitter:
- **In-Order Arrival (Fast Path):** If the new reading timestamp is $\ge$ the newest reading in the buffer, it appends to the tail in $O(1)$ time. This represents ~99% of normal traffic.
- **Out-of-Order Arrival:** If a reading arrives late, the buffer scans backward from the tail to find its sorted slot, shifts subsequent elements right by one position, and inserts it in chronological order.
- **Already Expired:** If a reading arrives with a timestamp older than 5 minutes, it is dropped immediately and counted as dropped.

### 3. Lazy Min/Max Recalculation
- Maintaining running sum and count during eviction is $O(1)$ (`_sum -= val`, `_count--`).
- However, if the evicted element was the active minimum or maximum, finding the new minimum requires scanning the remaining active readings ($O(N)$).
- **Tradeoff / Optimization:** Instead of scanning on every eviction during hot ingestion, the buffer sets `_minMaxDirty = true`. The rescan is deferred until an aggregate query arrives via `GET /api/devices/{deviceId}/aggregates`. Because writes are far more frequent than queries, this saves significant CPU time.

### 4. Preventing Memory Leaks (Idle Device Cleanup)
- Ingestion and queries prune expired readings on every access.
- To handle devices that disconnect and stop sending data completely, `WindowEvictionBackgroundService` runs in the background every 10 seconds.
- If a device has zero active readings and has been idle past its TTL (5 minutes), it is removed from memory and freed by the Garbage Collector.

---

## 5. What You Would Change to Scale Beyond a Single Instance

Section 6 notes that distributed coordination is out of scope for implementation, but requires an architectural discussion on scaling beyond a single instance:

### 1. Partitioned Message Broker (Apache Kafka / AWS Kinesis)
- Place an event log (Kafka or Kinesis) in front of the aggregation service.
- Use `deviceId` as the **partition key**.
- Kafka guarantees that all events with the same partition key always route to the same partition and consumer instance.
- This allows each worker node to maintain an in-memory sliding window ring buffer locally with **zero distributed locking** and zero inter-node communication.

### 2. Stateless Ingestion Gateway Tier
- The HTTP API layer becomes purely stateless: it accepts `POST /api/readings/batch`, validates the schema, and produces events into Kafka partitioned by `deviceId`.
- Ingestion gateways can scale out horizontally behind a standard round-robin load balancer.

### 3. Distributed Cache for Aggregate Queries
- To serve `GET /api/devices/{deviceId}/aggregates` without routing queries to specific worker instances:
  - Aggregation workers periodically publish their 5-minute aggregate snapshots to a distributed in-memory cache (Redis or Microsoft Garnet) with a 5-minute TTL.
  - Any query API instance can read directly from Redis with sub-millisecond response times.
