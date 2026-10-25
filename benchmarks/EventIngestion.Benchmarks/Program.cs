using BenchmarkDotNet.Running;
using EventIngestion.Benchmarks;

Console.WriteLine("===================================================================");
Console.WriteLine("  High-Throughput Event Ingestion Service - BenchmarkDotNet Suite  ");
Console.WriteLine("===================================================================");

BenchmarkRunner.Run<IngestionBenchmarks>();

