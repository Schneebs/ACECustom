using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

using log4net;

using ACE.Database.Entity;
using ACE.Database.Models.Shard;
using ACE.Entity.Enum;
using ACE.Entity.Enum.Properties;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Microsoft.EntityFrameworkCore; // Added for QueryTrackingBehavior

namespace ACE.Database
{
    public class SerializedShardDatabase
    {
        private static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        /// <summary>
        /// This is the base database that SerializedShardDatabase is a wrapper for.
        /// </summary>
        public readonly ShardDatabase BaseDatabase;

        protected readonly Stopwatch stopwatch = new Stopwatch();

        private readonly BlockingCollection<Task> _queue = new BlockingCollection<Task>();
        private readonly BlockingCollection<Task> _readOnlyQueue = new BlockingCollection<Task>();

        private List<Thread> _workerThreads;
        private List<Thread> _workerThreadsReadOnly;
        private readonly int _workerThreadCount = 4; // Configurable - adjust based on your server performance
        private readonly int _readOnlyWorkerThreadCount = 2; // Read operations are usually faster
        
        // Test cancellation support
        private volatile bool _cancelTests = false;

        internal SerializedShardDatabase(ShardDatabase shardDatabase)
        {
            BaseDatabase = shardDatabase;
        }

        public void Start()
        {
            _workerThreads = new List<Thread>();
            _workerThreadsReadOnly = new List<Thread>();
            
            // Create multiple worker threads for write operations
            for (int i = 0; i < _workerThreadCount; i++)
            {
                var workerThread = new Thread(DoWork)
                {
                    Name = $"Serialized Shard Database - Saving Worker {i + 1}"
                };
                _workerThreads.Add(workerThread);
                workerThread.Start();
            }
            
            // Create multiple worker threads for read operations
            for (int i = 0; i < _readOnlyWorkerThreadCount; i++)
            {
                var readOnlyWorkerThread = new Thread(DoReadOnlyWork)
                {
                    Name = $"Serialized Shard Database - Reading Worker {i + 1}"
                };
                _workerThreadsReadOnly.Add(readOnlyWorkerThread);
                readOnlyWorkerThread.Start();
            }
            
            Console.WriteLine($"[DATABASE] Started {_workerThreadCount} write worker threads and {_readOnlyWorkerThreadCount} read worker threads");
            stopwatch.Start();
        }

        public void Stop()
        {
            _queue.CompleteAdding();
            _readOnlyQueue.CompleteAdding();
            
            // Wait for all worker threads to complete
            foreach (var thread in _workerThreads)
            {
                thread.Join();
            }
            foreach (var thread in _workerThreadsReadOnly)
            {
                thread.Join();
            }
            
            Console.WriteLine($"[DATABASE] Stopped all {_workerThreadCount + _readOnlyWorkerThreadCount} worker threads");
        }

        public List<string> QueueReport()
        {
            return _queue.Select(x => x.AsyncState.ToString() ?? "Unknown Task").ToList();
        }

        public List<string> ReadOnlyQueueReport()
        {
            return _readOnlyQueue.Select(x => x.AsyncState.ToString() ?? "Unknown Task").ToList();
        }

        private void DoReadOnlyWork()
        {
            var threadName = Thread.CurrentThread.Name;
            Console.WriteLine($"[DATABASE] {threadName} started - Read-only queue processing beginning");
            
            while (!_readOnlyQueue.IsAddingCompleted)
            {
                try
                {
                    Task t;

                    bool tasked = _readOnlyQueue.TryTake(out t);
                    try
                    {
                        if (!tasked)
                        {
                            // no task to process, continue
                            continue;
                        }   
                        t.Start();
                    }
                                         catch (Exception e)
                     {
                         Console.WriteLine($"[DATABASE] DoReadOnlyWork task failed with exception: {e}");
                     }                   
                }
                catch (ObjectDisposedException)
                {
                    // the _queue has been disposed, we're good
                    break;
                }
                catch (InvalidOperationException)
                {
                    // _queue is empty and CompleteForAdding has been called -- we're done here
                    break;
                }
                catch (NullReferenceException)
                {
                    break;
                }
            }
        }
        private void DoWork()
        {
            var taskCounter = 0;
            var totalWaitTime = TimeSpan.Zero;
            var slowTaskThreshold = TimeSpan.FromMilliseconds(100); // Log tasks taking longer than 100ms
            var queueWaitStopwatch = new Stopwatch();
            var threadName = Thread.CurrentThread.Name;
            var lastQueueSize = 0;
            var queueSizeChangeThreshold = 5; // Log when queue size changes by this amount
            
            Console.WriteLine($"[DATABASE] {threadName} started - Queue processing beginning");
            
            while (!_queue.IsAddingCompleted)
            {
                try
                {
                    queueWaitStopwatch.Restart();
                    Task t;
                    bool tasked = _queue.TryTake(out t);
                    var queueWaitTime = queueWaitStopwatch.Elapsed;
                    totalWaitTime = totalWaitTime.Add(queueWaitTime);

                    try
                    {
                        if (!tasked)
                        {
                            if (queueWaitTime.TotalMilliseconds > 10) // Log significant waits
                            {
                                Console.WriteLine($"[DATABASE] No tasks available, waited {queueWaitTime.TotalMilliseconds:F1}ms for task, queue count: {_queue.Count}");
                            }
                            continue; // no task to process, continue
                        }
                        
                        taskCounter++;
                        var taskName = t.AsyncState?.ToString() ?? "UnknownTask";
                        var queueSize = _queue.Count;
                        
                        // Log queue size changes
                        if (Math.Abs(queueSize - lastQueueSize) >= queueSizeChangeThreshold)
                        {
                            var change = queueSize - lastQueueSize;
                            var changeText = change > 0 ? $"increased by {change}" : $"decreased by {Math.Abs(change)}";
                            Console.WriteLine($"[DATABASE] Queue size {changeText}: {lastQueueSize} -> {queueSize} (change: {change:+0;-0})");
                            lastQueueSize = queueSize;
                        }
                        
                        // Log queue wait time if significant
                        if (queueWaitTime.TotalMilliseconds > 1)
                        {
                            Console.WriteLine($"[DATABASE] Task #{taskCounter} '{taskName}' waited {queueWaitTime.TotalMilliseconds:F1}ms in queue, queue size: {queueSize}");
                        }
                        
                        stopwatch.Restart();
                        var taskStartTime = DateTime.UtcNow;
                        
                        Console.WriteLine($"[DATABASE] {threadName} starting task #{taskCounter}: '{taskName}' at {taskStartTime:HH:mm:ss.fff}, queue size: {queueSize}");
                        
                        t.Start();
                        
                        bool isReadOnlyTask = t.AsyncState != null && (t.AsyncState.ToString().Contains("GetPossessedBiotasInParallel") ||
                                                        t.AsyncState.ToString().Contains("GetMaxGuidFoundInRange") ||
                                                        t.AsyncState.ToString().Contains("GetSequenceGaps") ||
                                                        t.AsyncState.ToString().Contains("GetInventoryInParallel") ||
                                                        t.AsyncState.ToString().Contains("GetCharacter"));
                        
                        if (isReadOnlyTask)
                        {
                            Console.WriteLine($"[DATABASE] Task #{taskCounter} '{taskName}' running on background thread (read-only)");
                            //continue on background thread
                        }
                        else
                        {
                            Console.WriteLine($"[DATABASE] Task #{taskCounter} '{taskName}' waiting for completion (write task)");
                            t.Wait();
                        }
                        
                        var taskDuration = stopwatch.Elapsed;
                        var endTime = DateTime.UtcNow;
                        
                        // Log ALL task completions with detailed timing
                        Console.WriteLine($"[DATABASE] Task #{taskCounter} '{taskName}' COMPLETED at {endTime:HH:mm:ss.fff}, duration: {taskDuration.TotalMilliseconds:F1}ms, queue remaining: {_queue.Count}");
                        
                        // Log slow tasks with additional detail
                        if (taskDuration >= slowTaskThreshold)
                        {
                            Console.WriteLine($"[DATABASE] SLOW TASK #{taskCounter}: '{taskName}' took {taskDuration.TotalMilliseconds:F1}ms, completed at {endTime:HH:mm:ss.fff}, queue remaining: {_queue.Count}");
                        }
                        
                        // Log all tasks that took more than 5 seconds as errors
                        if (stopwatch.Elapsed.Seconds >= 5)
                        {
                            Console.WriteLine($"[DATABASE] VERY SLOW TASK #{taskCounter}: '{taskName}' took {stopwatch.ElapsedMilliseconds}ms, queue: {_queue.Count}");
                        }
                        
                        // Periodic summary every 100 tasks
                        if (taskCounter % 100 == 0)
                        {
                            var avgQueueWait = totalWaitTime.TotalMilliseconds / taskCounter;
                            var avgTaskDuration = stopwatch.Elapsed.TotalMilliseconds / taskCounter;
                            Console.WriteLine($"[DATABASE] Processed {taskCounter} tasks. Avg queue wait: {avgQueueWait:F1}ms, Avg task duration: {avgTaskDuration:F1}ms, Current queue size: {_queue.Count}");
                        }
                    }
                                         catch (Exception ex)
                     {
                         Console.WriteLine($"[DATABASE] Task #{taskCounter} '{t?.AsyncState?.ToString() ?? "Unknown"}' failed with exception: {ex}");
                         // perhaps add failure callbacks?
                         // swallow for now.  can't block other db work because 1 fails.
                     }
                    
                }
                catch (ObjectDisposedException)
                {
                    // the _queue has been disposed, we're good
                    break;
                }
                catch (InvalidOperationException)
                {
                    // _queue is empty and CompleteForAdding has been called -- we're done here
                    break;
                }
                catch (NullReferenceException)
                {
                    break;
                }
            }
        }


        public int QueueCount => _queue.Count;
        public int ReadOnlyQueueCount => _readOnlyQueue.Count;
        
        // Performance tracking
        private static readonly Dictionary<string, (int count, double totalTime, double maxTime)> _operationStats = 
            new Dictionary<string, (int count, double totalTime, double maxTime)>();
        private static readonly object _statsLock = new object();
        
        public string GetPerformanceReport()
        {
            lock (_statsLock)
            {
                if (_operationStats.Count == 0)
                    return "No database operations recorded yet.";
                
                var report = new StringBuilder();
                report.AppendLine("=== DATABASE PERFORMANCE REPORT ===");
                report.AppendLine($"Total operations tracked: {_operationStats.Values.Sum(x => x.count)}");
                report.AppendLine();
                
                foreach (var kvp in _operationStats.OrderByDescending(x => x.Value.totalTime))
                {
                    var (count, totalTime, maxTime) = kvp.Value;
                    var avgTime = count > 0 ? totalTime / count : 0;
                    report.AppendLine($"{kvp.Key}:");
                    report.AppendLine($"  Count: {count}, Avg: {avgTime:F1}ms, Max: {maxTime:F1}ms, Total: {totalTime:F1}ms");
                }
                
                return report.ToString();
            }
        }
        
        public void ClearPerformanceStats()
        {
            lock (_statsLock)
            {
                _operationStats.Clear();
            }
        }

        /// <summary>
        /// Test single-threaded vs multi-threaded performance by running the same operations both ways
        /// </summary>
        public void TestSingleThreadedPerformance(int testCount = 100)
        {
            Console.WriteLine($"[DATABASE] Testing single-threaded vs multi-threaded performance with {testCount} operations...");
            Console.WriteLine($"[DATABASE] Current queue size before test: {_queue.Count}");
            Console.WriteLine($"[DATABASE] Current worker thread count: {_workerThreadCount}");
            
            // Create test operations with varying durations
            var testOperations = new List<(string name, int duration)>();
            var random = new Random();
            
            for (int i = 0; i < testCount; i++)
            {
                var operationType = random.Next(3);
                var duration = operationType switch
                {
                    0 => random.Next(5, 15),    // Quick operation
                    1 => random.Next(20, 50),   // Medium operation
                    2 => random.Next(60, 150),  // Slow operation
                    _ => random.Next(10, 30)
                };
                
                testOperations.Add(($"TestOperation_{i}", duration));
            }
            
            // Test 1: Single-threaded execution (sequential processing)
            Console.WriteLine($"[DATABASE] ===== SINGLE-THREADED TEST STARTING =====");
            var singleThreadStopwatch = Stopwatch.StartNew();
            
            foreach (var (name, duration) in testOperations)
            {
                var operationStopwatch = Stopwatch.StartNew();
                Thread.Sleep(duration); // Simulate the actual work
                operationStopwatch.Stop();
                
                Console.WriteLine($"[DATABASE] Single-threaded: {name} completed in {operationStopwatch.ElapsedMilliseconds}ms (simulated: {duration}ms)");
            }
            
            singleThreadStopwatch.Stop();
            var singleThreadTime = singleThreadStopwatch.Elapsed;
            Console.WriteLine($"[DATABASE] Single-threaded test completed in {singleThreadTime.TotalMilliseconds:F1}ms");
            
            // Test 2: Multi-threaded execution (using the existing queue system)
            Console.WriteLine($"[DATABASE] ===== MULTI-THREADED TEST STARTING =====");
            var multiThreadStopwatch = Stopwatch.StartNew();
            var completedCount = 0;
            var lockObject = new object();
            
            // Add all operations to the queue
            foreach (var (name, duration) in testOperations)
            {
                _queue.Add(new Task((x) =>
                {
                    var operationStopwatch = Stopwatch.StartNew();
                    Thread.Sleep(duration); // Simulate the actual work
                    operationStopwatch.Stop();
                    
                    lock (lockObject)
                    {
                        completedCount++;
                        Console.WriteLine($"[DATABASE] Multi-threaded: {name} completed in {operationStopwatch.ElapsedMilliseconds}ms (simulated: {duration}ms) - {completedCount}/{testCount} done");
                    }
                }, name));
            }
            
            Console.WriteLine($"[DATABASE] Added {testCount} operations to multi-threaded queue. Waiting for completion...");
            
            // Wait for all operations to complete
            while (completedCount < testCount)
            {
                Thread.Sleep(100);
                if (multiThreadStopwatch.Elapsed.TotalSeconds > 60) // Timeout after 1 minute
                {
                    Console.WriteLine($"[DATABASE] Multi-threaded test timed out after 60 seconds. Completed: {completedCount}/{testCount}");
                    break;
                }
            }
            
            multiThreadStopwatch.Stop();
            var multiThreadTime = multiThreadStopwatch.Elapsed;
            
            // Performance comparison
            Console.WriteLine($"[DATABASE] ===== PERFORMANCE COMPARISON =====");
            Console.WriteLine($"[DATABASE] Single-threaded total time: {singleThreadTime.TotalMilliseconds:F1}ms");
            Console.WriteLine($"[DATABASE] Multi-threaded total time: {multiThreadTime.TotalMilliseconds:F1}ms");
            
            if (singleThreadTime > multiThreadTime)
            {
                var timeSaved = singleThreadTime - multiThreadTime;
                var percentageSaved = (timeSaved.TotalMilliseconds / singleThreadTime.TotalMilliseconds) * 100;
                Console.WriteLine($"[DATABASE] ✅ Multi-threading saved {timeSaved.TotalMilliseconds:F1}ms ({percentageSaved:F1}% improvement)");
            }
            else
            {
                var timeLost = multiThreadTime - singleThreadTime;
                var percentageLost = (timeLost.TotalMilliseconds / singleThreadTime.TotalMilliseconds) * 100;
                Console.WriteLine($"[DATABASE] ⚠️  Multi-threading overhead cost {timeLost.TotalMilliseconds:F1}ms ({percentageLost:F1}% overhead)");
            }
            
            Console.WriteLine($"[DATABASE] ===== TEST COMPLETED =====");
        }

        /// <summary>
        /// Cancel any running performance tests
        /// </summary>
        public void CancelTests()
        {
            _cancelTests = true;
            Console.WriteLine($"[DATABASE] Test cancellation requested. Tests will stop at next check.");
        }

        /// <summary>
        /// Reset test cancellation flag
        /// </summary>
        public void ResetTestCancellation()
        {
            _cancelTests = false;
            Console.WriteLine($"[DATABASE] Test cancellation flag reset.");
        }

        /// <summary>
        /// Quick performance comparison test with smaller operation count for faster results
        /// </summary>
        public void QuickPerformanceComparison(int testCount = 50)
        {
            // Reset cancellation flag at start of new test
            _cancelTests = false;
            
            Console.WriteLine($"[DATABASE] Quick performance comparison test with {testCount} operations...");
            
            // Create test operations with fixed durations for consistent comparison
            var testOperations = new List<(string name, int duration)>();
            
            for (int i = 0; i < testCount; i++)
            {
                // Use fixed durations for more predictable comparison
                var duration = 20 + (i % 3) * 10; // 20ms, 30ms, or 40ms
                testOperations.Add(($"QuickTest_{i}", duration));
            }
            
            // Single-threaded test
            Console.WriteLine($"[DATABASE] Starting single-threaded test...");
            var singleThreadStopwatch = Stopwatch.StartNew();
            foreach (var (name, duration) in testOperations)
            {
                Thread.Sleep(duration);
            }
            singleThreadStopwatch.Stop();
            var singleThreadTime = singleThreadStopwatch.Elapsed;
            Console.WriteLine($"[DATABASE] Single-threaded test completed in {singleThreadTime.TotalMilliseconds:F1}ms");
            
            // Multi-threaded test
            Console.WriteLine($"[DATABASE] Starting multi-threaded test...");
            var multiThreadStopwatch = Stopwatch.StartNew();
            var completedCount = 0;
            var lockObject = new object();
            
            // Add all operations to the queue
            for (int i = 0; i < testOperations.Count; i++)
            {
                var (name, duration) = testOperations[i];
                _queue.Add(new Task((x) =>
                {
                    Thread.Sleep(duration);
                    lock (lockObject)
                    {
                        completedCount++;
                        // Log progress every 100 completions for large tests
                        if (completedCount % 100 == 0 || completedCount == testCount)
                        {
                            Console.WriteLine($"[DATABASE] Multi-threaded progress: {completedCount}/{testCount} completed ({completedCount * 100.0 / testCount:F1}%)");
                        }
                    }
                }, name));
                
                // Log progress every 500 additions for large tests
                if ((i + 1) % 500 == 0)
                {
                    Console.WriteLine($"[DATABASE] Added {i + 1}/{testCount} operations to queue...");
                }
            }
            
            Console.WriteLine($"[DATABASE] Added all {testCount} operations to multi-threaded queue. Waiting for completion...");
            
            // Calculate appropriate timeout based on operation count
            var estimatedSingleThreadTime = singleThreadTime.TotalMilliseconds;
            var estimatedMultiThreadTime = estimatedSingleThreadTime / 4; // Assume 4x improvement with 4 threads
            var timeoutSeconds = Math.Max(30, Math.Min(300, estimatedMultiThreadTime / 1000 + 60)); // Between 30s and 5 minutes
            
            Console.WriteLine($"[DATABASE] Estimated completion time: {estimatedMultiThreadTime / 1000:F1}s, Timeout set to: {timeoutSeconds}s");
            
            // Wait for completion with dynamic timeout
            var startWait = DateTime.UtcNow;
            var lastProgressReport = DateTime.UtcNow;
            
            while (completedCount < testCount)
            {
                // Check for cancellation
                if (_cancelTests)
                {
                    Console.WriteLine($"[DATABASE] Test cancelled by user. Completed: {completedCount}/{testCount} ({completedCount * 100.0 / testCount:F1}%)");
                    break;
                }
                
                Thread.Sleep(100);
                var elapsed = (DateTime.UtcNow - startWait).TotalSeconds;
                
                // Progress report every 10 seconds for long-running tests
                if ((DateTime.UtcNow - lastProgressReport).TotalSeconds >= 10)
                {
                    var progress = completedCount * 100.0 / testCount;
                    var remaining = testCount - completedCount;
                    Console.WriteLine($"[DATABASE] Progress: {completedCount}/{testCount} ({progress:F1}%) - {remaining} remaining - Elapsed: {elapsed:F1}s");
                    lastProgressReport = DateTime.UtcNow;
                }
                
                // Check timeout
                if (elapsed > timeoutSeconds)
                {
                    Console.WriteLine($"[DATABASE] Multi-threaded test timed out after {timeoutSeconds}s. Completed: {completedCount}/{testCount} ({completedCount * 100.0 / testCount:F1}%)");
                    break;
                }
            }
            
            multiThreadStopwatch.Stop();
            var multiThreadTime = multiThreadStopwatch.Elapsed;
            
            // Results
            Console.WriteLine($"[DATABASE] ===== QUICK COMPARISON RESULTS =====");
            Console.WriteLine($"[DATABASE] Single-threaded: {singleThreadTime.TotalMilliseconds:F1}ms");
            Console.WriteLine($"[DATABASE] Multi-threaded: {multiThreadTime.TotalMilliseconds:F1}ms");
            Console.WriteLine($"[DATABASE] Operations completed: {completedCount}/{testCount}");
            
            if (completedCount == testCount)
            {
                if (singleThreadTime > multiThreadTime)
                {
                    var improvement = ((singleThreadTime.TotalMilliseconds - multiThreadTime.TotalMilliseconds) / singleThreadTime.TotalMilliseconds) * 100;
                    Console.WriteLine($"[DATABASE] ✅ Multi-threading provided {improvement:F1}% improvement");
                }
                else
                {
                    var overhead = ((multiThreadTime.TotalMilliseconds - singleThreadTime.TotalMilliseconds) / singleThreadTime.TotalMilliseconds) * 100;
                    Console.WriteLine($"[DATABASE] ⚠️  Multi-threading overhead: {overhead:F1}%");
                }
            }
            else
            {
                Console.WriteLine($"[DATABASE] ⚠️  Test incomplete - only {completedCount}/{testCount} operations finished");
            }
            
            Console.WriteLine($"[DATABASE] ===== QUICK TEST COMPLETED =====");
        }

        /// <summary>
        /// Get detailed queue analysis to identify bottlenecks
        /// </summary>
        public string GetDetailedQueueAnalysis()
        {
            var analysis = new StringBuilder();
            analysis.AppendLine("=== DETAILED QUEUE ANALYSIS ===");
            analysis.AppendLine($"Current time: {DateTime.UtcNow:HH:mm:ss.fff}");
            analysis.AppendLine($"Write queue size: {_queue.Count}");
            analysis.AppendLine($"Read-only queue size: {_readOnlyQueue.Count}");
            analysis.AppendLine($"Active worker threads: {_workerThreads?.Count ?? 0}");
            analysis.AppendLine($"Active read-only worker threads: {_workerThreadsReadOnly?.Count ?? 0}");
            analysis.AppendLine();
            
            // Queue content analysis
            if (_queue.Count > 0)
            {
                analysis.AppendLine("Write Queue Contents:");
                var queueItems = _queue.Take(_queue.Count).ToList();
                var operationTypes = queueItems.GroupBy(x => x.AsyncState?.ToString() ?? "Unknown")
                                             .Select(g => new { Operation = g.Key, Count = g.Count() })
                                             .OrderByDescending(x => x.Count);
                
                foreach (var op in operationTypes)
                {
                    analysis.AppendLine($"  {op.Operation}: {op.Count} operations");
                }
                
                // Put items back in queue
                foreach (var item in queueItems)
                {
                    _queue.Add(item);
                }
            }
            
            analysis.AppendLine();
            analysis.AppendLine("Performance Recommendations:");
            
            if (_queue.Count > 100)
                analysis.AppendLine("  ⚠️  High queue size detected - consider increasing worker threads or optimizing operations");
            
            if (_queue.Count > 0 && _workerThreadCount < 8)
                analysis.AppendLine("  💡  Consider increasing worker thread count for better throughput");
            
            if (_queue.Count == 0)
                analysis.AppendLine("  ✅  Queue is empty - system is keeping up with demand");
            
            return analysis.ToString();
        }

        /// <summary>
        /// Get current queue wait time and detailed analysis
        /// </summary>
        public void GetCurrentQueueWaitTime(Action<TimeSpan> callback)
        {
            var initialCallTime = DateTime.UtcNow;
            var queueSize = _queue.Count;
            Console.WriteLine($"[DATABASE] ENQUEUE: GetCurrentQueueWaitTime at {initialCallTime:HH:mm:ss.fff}, queue size before: {queueSize}");
            
            // Add detailed queue analysis
            var analysis = GetDetailedQueueAnalysis();
            Console.WriteLine(analysis);
            
            _queue.Add(new Task((x) =>
            {
                var currentTime = DateTime.UtcNow;
                var waitTime = currentTime - initialCallTime;
                var finalQueueSize = _queue.Count;
                
                Console.WriteLine($"[DATABASE] GetCurrentQueueWaitTime completed at {currentTime:HH:mm:ss.fff}");
                Console.WriteLine($"[DATABASE]   Wait time: {waitTime.TotalMilliseconds:F1}ms");
                Console.WriteLine($"[DATABASE]   Queue size change: {queueSize} -> {finalQueueSize} (change: {finalQueueSize - queueSize:+0;-0})");
                
                callback?.Invoke(waitTime);
            }, "GetCurrentQueueWaitTime"));
        }

        /// <summary>
        /// Stress test the database queue to identify bottlenecks
        /// </summary>
        public void StressTestQueue(int operationCount)
        {
            Console.WriteLine($"[DATABASE] Starting queue stress test with {operationCount} operations...");
            var stopwatch = Stopwatch.StartNew();
            
            // Add stress operations
            for (int i = 0; i < operationCount; i++)
            {
                var testId = (uint)(2000000 + i);
                _queue.Add(new Task((x) =>
                {
                    // Simulate various database operations with different durations
                    var random = new Random();
                    var operationType = random.Next(3);
                    
                    switch (operationType)
                    {
                        case 0: // Quick operation
                            Thread.Sleep(random.Next(5, 15));
                            break;
                        case 1: // Medium operation
                            Thread.Sleep(random.Next(20, 50));
                            break;
                        case 2: // Slow operation
                            Thread.Sleep(random.Next(60, 150));
                            break;
                    }
                }, $"StressTest_{testId}"));
                
                // Add some delay between operations to simulate real-world usage
                if (i % 100 == 0)
                {
                    Thread.Sleep(10);
                    Console.WriteLine($"[DATABASE] Added {i + 1} stress operations to queue...");
                }
            }
            
            stopwatch.Stop();
            Console.WriteLine($"[DATABASE] Stress test setup completed in {stopwatch.ElapsedMilliseconds}ms");
            Console.WriteLine($"[DATABASE] Added {operationCount} operations to queue. Monitor console for performance metrics.");
        }


        /// <summary>
        /// Will return uint.MaxValue if no records were found within the range provided.
        /// </summary>
        public void GetMaxGuidFoundInRange(uint min, uint max, Action<uint> callback)
        {
            _readOnlyQueue.Add(new Task((x) =>
            {
                var result = BaseDatabase.GetMaxGuidFoundInRange(min, max);
                callback?.Invoke(result);
            }, "GetMaxGuidFoundInRange: " + min));
        }

        /// <summary>
        /// This will return available id's, in the form of sequence gaps starting from min.<para />
        /// If a gap is just 1 value wide, then both start and end will be the same number.
        /// </summary>
        public void GetSequenceGaps(uint min, uint limitAvailableIDsReturned, Action<List<(uint start, uint end)>> callback)
        {
            _readOnlyQueue.Add(new Task((x) =>
            {
                var result = BaseDatabase.GetSequenceGaps(min, limitAvailableIDsReturned);
                callback?.Invoke(result);
            }, "GetSequenceGaps: " + min));
        }


        public void SaveBiota(ACE.Entity.Models.Biota biota, ReaderWriterLockSlim rwLock, Action<bool> callback)
        {
            var queueSize = _queue.Count;
            var enqueueTime = DateTime.UtcNow;
                         var taskName = "SaveBiota: " + biota.Id;
             
             Console.WriteLine($"[DATABASE] ENQUEUE: {taskName} at {enqueueTime:HH:mm:ss.fff}, queue size before: {queueSize}");
             
             if (queueSize > 50)
            {
                Console.WriteLine($"[DATABASE] HIGH QUEUE SIZE: {taskName} enqueued with {queueSize} tasks already waiting!");
            }
            
            _queue.Add(new Task((x) =>
            {
                var saveStartTime = DateTime.UtcNow;
                var stopwatch = Stopwatch.StartNew();
                
                var result = BaseDatabase.SaveBiota(biota, rwLock);
                
                stopwatch.Stop();
                var saveEndTime = DateTime.UtcNow;
                var totalTime = stopwatch.Elapsed;
                
                if (totalTime.TotalMilliseconds > 50) // Log slow individual saves
                {
                    Console.WriteLine($"[DATABASE] SLOW INDIVIDUAL SAVE: {taskName} took {totalTime.TotalMilliseconds:F1}ms " +
                            $"({saveStartTime:HH:mm:ss.fff} to {saveEndTime:HH:mm:ss.fff})");
                }
                
                callback?.Invoke(result);
            }, taskName));
        }


        public void SaveBiotasInParallel(IEnumerable<(ACE.Entity.Models.Biota biota, ReaderWriterLockSlim rwLock)> biotas, Action<bool> callback, string sourceTrace)
        {
            var queueSize = _queue.Count;
            var enqueueTime = DateTime.UtcNow;
            var biotasArray = biotas?.ToArray();
            var biotaCount = biotasArray?.Length ?? 0;
            var taskName = $"SaveBiotasInParallel {sourceTrace} ({biotaCount} biotas)";
            
            Console.WriteLine($"[DATABASE] ENQUEUE: {taskName} at {enqueueTime:HH:mm:ss.fff}, queue size before: {queueSize}");
            
            if (queueSize > 50)
            {
                Console.WriteLine($"[DATABASE] HIGH QUEUE SIZE: {taskName} enqueued with {queueSize} tasks already waiting!");
            }
            
            if (biotaCount > 10)
            {
                Console.WriteLine($"[DATABASE] LARGE BATCH: {taskName} contains {biotaCount} biotas - this may take significant time");
            }
            
            _queue.Add(new Task((x) =>
            {
                var batchStartTime = DateTime.UtcNow;
                var stopwatch = Stopwatch.StartNew();
                
                Console.WriteLine($"[DATABASE] BATCH START: {taskName} beginning at {batchStartTime:HH:mm:ss.fff}");
                
                var result = BaseDatabase.SaveBiotasInParallel(biotasArray);
                
                stopwatch.Stop();
                var batchEndTime = DateTime.UtcNow;
                var totalTime = stopwatch.Elapsed;
                var avgTimePerBiota = totalTime.TotalMilliseconds / biotaCount;
                
                Console.WriteLine($"[DATABASE] BATCH COMPLETE: {taskName} finished at {batchEndTime:HH:mm:ss.fff}, " +
                        $"total time: {totalTime.TotalMilliseconds:F1}ms, " +
                        $"avg per biota: {avgTimePerBiota:F1}ms, " +
                        $"queue remaining: {_queue.Count}");
                
                // Calculate theoretical single-threaded time
                var estimatedSingleThreadTime = totalTime.TotalMilliseconds * biotaCount;
                var timeSaved = estimatedSingleThreadTime - totalTime.TotalMilliseconds;
                
                if (biotaCount > 10)
                {
                    Console.WriteLine($"[DATABASE] PARALLEL BENEFIT: {taskName} saved {timeSaved:F1}ms " +
                            $"(would have taken {estimatedSingleThreadTime:F1}ms single-threaded vs {totalTime.TotalMilliseconds:F1}ms parallel)");
                }
                
                // Track performance stats
                lock (_statsLock)
                {
                    var operationKey = $"SaveBiotasInParallel_{sourceTrace}";
                    if (_operationStats.ContainsKey(operationKey))
                    {
                        var (count, existingTotalTime, maxTime) = _operationStats[operationKey];
                        _operationStats[operationKey] = (count + 1, existingTotalTime + totalTime.TotalMilliseconds, 
                            Math.Max(maxTime, totalTime.TotalMilliseconds));
                    }
                    else
                    {
                        _operationStats[operationKey] = (1, totalTime.TotalMilliseconds, totalTime.TotalMilliseconds);
                    }
                }
                
                callback?.Invoke(result);
            }, taskName));
        }

        public void RemoveBiota(uint id, Action<bool> callback)
        {
            _queue.Add(new Task((x) =>
            {
                var result = BaseDatabase.RemoveBiota(id);
                callback?.Invoke(result);
            }, "RemoveBiota: " + id));
        }

        public void RemoveBiota(uint id, Action<bool> callback, Action<TimeSpan, TimeSpan> performanceResults)
        {
            var initialCallTime = DateTime.UtcNow;

            _queue.Add(new Task( (x) =>
            {
                var taskStartTime = DateTime.UtcNow;
                var result = BaseDatabase.RemoveBiota(id);
                var taskCompletedTime = DateTime.UtcNow;
                callback?.Invoke(result);
                performanceResults?.Invoke(taskStartTime - initialCallTime, taskCompletedTime - taskStartTime);
            }, "RemoveBiota2:" + id));
        }

        public void RemoveBiotasInParallel(IEnumerable<uint> ids, Action<bool> callback, Action<TimeSpan, TimeSpan> performanceResults)
        {
            var initialCallTime = DateTime.UtcNow;

            _queue.Add(new Task((x) =>
            {
                var taskStartTime = DateTime.UtcNow;
                var result = BaseDatabase.RemoveBiotasInParallel(ids);
                var taskCompletedTime = DateTime.UtcNow;
                callback?.Invoke(result);
                performanceResults?.Invoke(taskStartTime - initialCallTime, taskCompletedTime - taskStartTime);
            }, "RemoveBiotasInParallel: " +ids.Count()));
        }


        public void GetPossessedBiotasInParallel(uint id, Action<PossessedBiotas> callback)
        {
            _readOnlyQueue.Add(new Task((x) =>
            {
                var c = BaseDatabase.GetPossessedBiotasInParallel(id);
                callback?.Invoke(c);
            }, "GetPossessedBiotasInParallel: " + id));
        }

        public void GetInventoryInParallel(uint parentId, bool includedNestedItems, Action<List<Biota>> callback)
        {
            _readOnlyQueue.Add(new Task((x) =>
            {
                var c = BaseDatabase.GetInventoryInParallel(parentId, includedNestedItems);
                callback?.Invoke(c);
            }, "GetInventoryInParallel: " + parentId));

        }


        public void IsCharacterNameAvailable(string name, Action<bool> callback)
        {
            _readOnlyQueue.Add(new Task(() =>
            {
                var result = BaseDatabase.IsCharacterNameAvailable(name);
                callback?.Invoke(result);
            }));
        }

        public void GetCharacters(uint accountId, bool includeDeleted, Action<List<Character>> callback)
        {
            _readOnlyQueue.Add(new Task((x) =>
            {
                var result = BaseDatabase.GetCharacters(accountId, includeDeleted);
                callback?.Invoke(result);
            }, "GetCharacters: " + accountId ));
        }

        public void GetLoginCharacters(uint accountId, bool includeDeleted, Action<List<LoginCharacter>> callback)
        {
            _readOnlyQueue.Add(new Task((x) =>
            {
                var result = BaseDatabase.GetCharacterListForLogin(accountId, includeDeleted);
                callback?.Invoke(result);
            }, "GetCharacterListForLogin: " + accountId));
        }

        public void GetCharacter(uint characterId, Action<Character> callback)
        {
            _readOnlyQueue.Add(new Task((x) =>
            {
                var result = BaseDatabase.GetCharacter(characterId);
                callback?.Invoke(result);
            }, "GetCharacter: " + characterId));
        }

        public Character GetCharacterSynchronous(uint characterId)
        {
            return BaseDatabase.GetCharacter(characterId);            
        }
        
        public void SaveCharacter(Character character, ReaderWriterLockSlim rwLock, Action<bool> callback)
        {
            _queue.Add(new Task((x) =>
            {
                var result = BaseDatabase.SaveCharacter(character, rwLock);
                callback?.Invoke(result);
            }, "SaveCharacter: " + character.Id));
        }

        public bool SaveCharacterSynchronous(Character character, ReaderWriterLockSlim rwLock)
        {
            return BaseDatabase.SaveCharacter(character, rwLock);
        }

        public void RenameCharacter(Character character, string newName, ReaderWriterLockSlim rwLock, Action<bool> callback)
        {
            _queue.Add(new Task(() =>
            {
                var result = BaseDatabase.RenameCharacter(character, newName, rwLock);
                callback?.Invoke(result);
            }));
        }

        public void SetCharacterAccessLevelByName(string name, AccessLevel accessLevel, Action<uint> callback)
        {
            // TODO
            throw new NotImplementedException();
        }


        public void AddCharacterInParallel(ACE.Entity.Models.Biota biota, ReaderWriterLockSlim biotaLock, IEnumerable<(ACE.Entity.Models.Biota biota, ReaderWriterLockSlim rwLock)> possessions, Character character, ReaderWriterLockSlim characterLock, Action<bool> callback)
        {
            _queue.Add(new Task((x) =>
            {
                var result = BaseDatabase.AddCharacterInParallel(biota, biotaLock, possessions, character, characterLock);
                callback?.Invoke(result);
            }, "AddCharacterInParallel: " + character.Id));
        }

        /// <summary>
        /// Test real inventory scan performance by querying multiple players' inventories in parallel
        /// This simulates the actual database load of inventory scans
        /// </summary>
        public void TestInventoryScanPerformance(int playerCount = 10)
        {
            // Reset cancellation flag at start of new test
            _cancelTests = false;
            
            Console.WriteLine($"[DATABASE] Starting REAL inventory scan performance test with {playerCount} players...");
            Console.WriteLine($"[DATABASE] This will perform actual database queries to simulate real inventory scans");
            
            // Get a list of player IDs from the database for testing
            var testPlayerIds = GetTestPlayerIdsFromDatabase(playerCount);
            if (testPlayerIds == null || testPlayerIds.Count == 0)
            {
                Console.WriteLine($"[DATABASE] ERROR: No test players found in database. Test cannot proceed.");
                return;
            }
            
            Console.WriteLine($"[DATABASE] Found {testPlayerIds.Count} test players for inventory scanning");
            
            // Debug: Show the actual player IDs we're testing
            Console.WriteLine($"[DATABASE] Testing player IDs: {string.Join(", ", testPlayerIds.Select(id => $"0x{id:X8}"))}");
            
            // Test 1: Single-threaded inventory scans (sequential processing)
            Console.WriteLine($"[DATABASE] ===== SINGLE-THREADED INVENTORY SCAN TEST STARTING =====");
            var singleThreadStopwatch = Stopwatch.StartNew();
            var singleThreadResults = new List<(uint playerId, int itemCount, TimeSpan duration)>();
            
            foreach (var playerId in testPlayerIds)
            {
                var operationStopwatch = Stopwatch.StartNew();
                
                // Use the base database directly for synchronous testing
                List<Biota> inventory = null;
                try
                {
                    Console.WriteLine($"[DATABASE] DEBUG: Attempting to scan inventory for player 0x{playerId:X8}...");
                    inventory = BaseDatabase.GetInventoryInParallel(playerId, true);
                    Console.WriteLine($"[DATABASE] DEBUG: Inventory scan completed for player 0x{playerId:X8}, found {inventory?.Count ?? 0} items");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[DATABASE] ERROR scanning inventory for player 0x{playerId:X8}: {ex.Message}");
                    Console.WriteLine($"[DATABASE] ERROR Stack trace: {ex.StackTrace}");
                    inventory = new List<Biota>(); // Empty list on error
                }
                
                operationStopwatch.Stop();
                
                var itemCount = inventory?.Count ?? 0;
                var duration = operationStopwatch.Elapsed;
                singleThreadResults.Add((playerId, itemCount, duration));
                
                Console.WriteLine($"[DATABASE] Single-threaded: Player 0x{playerId:X8} inventory scan completed in {duration.TotalMilliseconds:F1}ms ({itemCount} items)");
            }
            
            singleThreadStopwatch.Stop();
            var singleThreadTime = singleThreadStopwatch.Elapsed;
            var totalItemsSingleThread = singleThreadResults.Sum(r => r.itemCount);
            var avgItemsPerPlayer = totalItemsSingleThread / (double)testPlayerIds.Count;
            
            Console.WriteLine($"[DATABASE] Single-threaded test completed in {singleThreadTime.TotalMilliseconds:F1}ms");
            Console.WriteLine($"[DATABASE] Total items scanned: {totalItemsSingleThread}, Average items per player: {avgItemsPerPlayer:F1}");
            
            // Test 2: Multi-threaded inventory scans (using the queue system)
            Console.WriteLine($"[DATABASE] ===== MULTI-THREADED INVENTORY SCAN TEST STARTING =====");
            var multiThreadStopwatch = Stopwatch.StartNew();
            var completedCount = 0;
            var multiThreadResults = new List<(uint playerId, int itemCount, TimeSpan duration)>();
            var lockObject = new object();
            
            // Add all inventory scan operations to the queue
            for (int i = 0; i < testPlayerIds.Count; i++)
            {
                var playerId = testPlayerIds[i];
                var taskName = $"InventoryScan_{playerId:X8}";
                
                _queue.Add(new Task((x) =>
                {
                    var operationStopwatch = Stopwatch.StartNew();
                    
                    // Use the base database directly for consistent testing
                    List<Biota> inventory = null;
                    try
                    {
                        Console.WriteLine($"[DATABASE] DEBUG: Multi-threaded inventory scan for player 0x{playerId:X8}...");
                        inventory = BaseDatabase.GetInventoryInParallel(playerId, true);
                        Console.WriteLine($"[DATABASE] DEBUG: Multi-threaded inventory scan completed for player 0x{playerId:X8}, found {inventory?.Count ?? 0} items");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[DATABASE] ERROR scanning inventory for player 0x{playerId:X8}: {ex.Message}");
                        Console.WriteLine($"[DATABASE] ERROR Stack trace: {ex.StackTrace}");
                        inventory = new List<Biota>(); // Empty list on error
                    }
                    
                    operationStopwatch.Stop();
                    
                    var itemCount = inventory?.Count ?? 0;
                    var duration = operationStopwatch.Elapsed;
                    
                    lock (lockObject)
                    {
                        multiThreadResults.Add((playerId, itemCount, duration));
                        completedCount++;
                        
                        // Log progress every 10 completions for large tests
                        if (completedCount % 10 == 0 || completedCount == testPlayerIds.Count)
                        {
                            Console.WriteLine($"[DATABASE] Multi-threaded progress: {completedCount}/{testPlayerIds.Count} completed ({completedCount * 100.0 / testPlayerIds.Count:F1}%)");
                        }
                    }
                }, taskName));
                
                // Log progress every 50 additions for large tests
                if ((i + 1) % 50 == 0)
                {
                    Console.WriteLine($"[DATABASE] Added {i + 1}/{testPlayerIds.Count} inventory scan operations to queue...");
                }
            }
            
            Console.WriteLine($"[DATABASE] Added all {testPlayerIds.Count} inventory scan operations to multi-threaded queue. Waiting for completion...");
            
            // Calculate appropriate timeout based on single-threaded performance
            var estimatedMultiThreadTime = singleThreadTime.TotalMilliseconds / 4; // Assume 4x improvement with 4 threads
            var timeoutSeconds = Math.Max(30, Math.Min(300, estimatedMultiThreadTime / 1000 + 60)); // Between 30s and 5 minutes
            
            Console.WriteLine($"[DATABASE] Estimated completion time: {estimatedMultiThreadTime / 1000:F1}s, Timeout set to: {timeoutSeconds}s");
            
            // Wait for completion with dynamic timeout
            var startWait = DateTime.UtcNow;
            var lastProgressReport = DateTime.UtcNow;
            
            while (completedCount < testPlayerIds.Count)
            {
                // Check for cancellation
                if (_cancelTests)
                {
                    Console.WriteLine($"[DATABASE] Test cancelled by user. Completed: {completedCount}/{testPlayerIds.Count} ({completedCount * 100.0 / testPlayerIds.Count:F1}%)");
                    break;
                }
                
                Thread.Sleep(100);
                var elapsed = (DateTime.UtcNow - startWait).TotalSeconds;
                
                // Progress report every 5 seconds for inventory scans (they're slower than simulated operations)
                if ((DateTime.UtcNow - lastProgressReport).TotalSeconds >= 5)
                {
                    var progress = completedCount * 100.0 / testPlayerIds.Count;
                    var remaining = testPlayerIds.Count - completedCount;
                    Console.WriteLine($"[DATABASE] Progress: {completedCount}/{testPlayerIds.Count} ({progress:F1}%) - {remaining} remaining - Elapsed: {elapsed:F1}s");
                    lastProgressReport = DateTime.UtcNow;
                }
                
                // Check timeout
                if (elapsed > timeoutSeconds)
                {
                    Console.WriteLine($"[DATABASE] Multi-threaded test timed out after {timeoutSeconds}s. Completed: {completedCount}/{testPlayerIds.Count} ({completedCount * 100.0 / testPlayerIds.Count:F1}%)");
                    break;
                }
            }
            
            multiThreadStopwatch.Stop();
            var multiThreadTime = multiThreadStopwatch.Elapsed;
            var totalItemsMultiThread = multiThreadResults.Sum(r => r.itemCount);
            
            // Results
            Console.WriteLine($"[DATABASE] ===== INVENTORY SCAN PERFORMANCE RESULTS =====");
            Console.WriteLine($"[DATABASE] Single-threaded: {singleThreadTime.TotalMilliseconds:F1}ms");
            Console.WriteLine($"[DATABASE] Multi-threaded: {multiThreadTime.TotalMilliseconds:F1}ms");
            Console.WriteLine($"[DATABASE] Operations completed: {completedCount}/{testPlayerIds.Count}");
            Console.WriteLine($"[DATABASE] Single-threaded items: {totalItemsSingleThread}");
            Console.WriteLine($"[DATABASE] Multi-threaded items: {totalItemsMultiThread}");
            
            if (completedCount == testPlayerIds.Count)
            {
                if (singleThreadTime > multiThreadTime)
                {
                    var improvement = ((singleThreadTime.TotalMilliseconds - multiThreadTime.TotalMilliseconds) / singleThreadTime.TotalMilliseconds) * 100;
                    var timeSaved = singleThreadTime - multiThreadTime;
                    Console.WriteLine($"[DATABASE] ✅ Multi-threading provided {improvement:F1}% improvement");
                    Console.WriteLine($"[DATABASE] Time saved: {timeSaved.TotalMilliseconds:F1}ms");
                    Console.WriteLine($"[DATABASE] Items per second (single): {totalItemsSingleThread / singleThreadTime.TotalSeconds:F1}");
                    Console.WriteLine($"[DATABASE] Items per second (multi): {totalItemsMultiThread / multiThreadTime.TotalSeconds:F1}");
                }
                else
                {
                    var overhead = ((multiThreadTime.TotalMilliseconds - singleThreadTime.TotalMilliseconds) / singleThreadTime.TotalMilliseconds) * 100;
                    Console.WriteLine($"[DATABASE] ⚠️  Multi-threading overhead: {overhead:F1}%");
                }
            }
            else
            {
                Console.WriteLine($"[DATABASE] ⚠️  Test incomplete - only {completedCount}/{testPlayerIds.Count} operations finished");
            }
            
            // Detailed performance analysis
            Console.WriteLine($"[DATABASE] ===== DETAILED PERFORMANCE ANALYSIS =====");
            if (singleThreadResults.Count > 0)
            {
                var avgSingleThreadTime = singleThreadResults.Average(r => r.duration.TotalMilliseconds);
                var maxSingleThreadTime = singleThreadResults.Max(r => r.duration.TotalMilliseconds);
                var minSingleThreadTime = singleThreadResults.Min(r => r.duration.TotalMilliseconds);
                
                Console.WriteLine($"[DATABASE] Single-threaded performance:");
                Console.WriteLine($"[DATABASE]   Average scan time: {avgSingleThreadTime:F1}ms");
                Console.WriteLine($"[DATABASE]   Fastest scan: {minSingleThreadTime:F1}ms");
                Console.WriteLine($"[DATABASE]   Slowest scan: {maxSingleThreadTime:F1}ms");
            }
            
            if (multiThreadResults.Count > 0)
            {
                var avgMultiThreadTime = multiThreadResults.Average(r => r.duration.TotalMilliseconds);
                var maxMultiThreadTime = multiThreadResults.Max(r => r.duration.TotalMilliseconds);
                var minMultiThreadTime = multiThreadResults.Min(r => r.duration.TotalMilliseconds);
                
                Console.WriteLine($"[DATABASE] Multi-threaded performance:");
                Console.WriteLine($"[DATABASE]   Average scan time: {avgMultiThreadTime:F1}ms");
                Console.WriteLine($"[DATABASE]   Fastest scan: {minMultiThreadTime:F1}ms");
                Console.WriteLine($"[DATABASE]   Slowest scan: {maxMultiThreadTime:F1}ms");
            }
            
            Console.WriteLine($"[DATABASE] ===== INVENTORY SCAN TEST COMPLETED =====");
        }

        /// <summary>
        /// Get a list of player IDs from the database for testing purposes
        /// </summary>
        private List<uint> GetTestPlayerIds(int count)
        {
            try
            {
                // Use the base database to get actual player IDs from the Biota table
                var testIds = new List<uint>();
                
                using (var context = new ShardDbContext())
                {
                    context.ChangeTracker.QueryTrackingBehavior = QueryTrackingBehavior.NoTracking;
                    
                    // In ACE, players are stored in the Biota table with WeenieType.Creature
                    // and have specific properties that identify them as players
                    var playerBiotas = context.Biota
                        .Where(b => b.WeenieType == (int)WeenieType.Creature)
                        .Join(context.BiotaPropertiesInt, b => b.Id, p => p.ObjectId, (b, p) => new { Biota = b, Property = p })
                        .Where(bp => bp.Property.Type == (int)PropertyInt.PlayerKillerStatus) // Players have this property
                        .Select(bp => bp.Biota.Id)
                        .Distinct()
                        .OrderBy(id => id)
                        .Take(count)
                        .ToList();
                    
                    testIds.AddRange(playerBiotas);
                    
                    Console.WriteLine($"[DATABASE] Found {testIds.Count} player biotas in database");
                    
                    // If we don't have enough players, try to get some character IDs from the Character table as fallback
                    if (testIds.Count < count)
                    {
                        Console.WriteLine($"[DATABASE] Not enough player biotas found, trying Character table as fallback...");
                        
                        var characterIds = context.Character
                            .Where(c => c.DeleteTime == null) // Only non-deleted characters
                            .OrderBy(c => c.Id)
                            .Take(count - testIds.Count)
                            .Select(c => c.Id)
                            .ToList();
                        
                        testIds.AddRange(characterIds);
                        Console.WriteLine($"[DATABASE] Added {characterIds.Count} character IDs from Character table");
                    }
                }
                
                // If we still don't have enough, add some dummy IDs for testing the queue system
                while (testIds.Count < count)
                {
                    testIds.Add((uint)(0x70000000 + testIds.Count)); // Use high range IDs that likely don't exist
                }
                
                Console.WriteLine($"[DATABASE] Total test IDs prepared: {testIds.Count}");
                return testIds.Take(count).ToList();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[DATABASE] ERROR getting test player IDs: {ex.Message}");
                Console.WriteLine($"[DATABASE] Stack trace: {ex.StackTrace}");
                // Return some dummy IDs for testing the queue system
                return Enumerable.Range(0, count).Select(i => (uint)(0x70000000 + i)).ToList();
            }
        }

        /// <summary>
        /// NEW METHOD: Get a list of player IDs from the database for testing purposes
        /// This method uses a different approach to find players without relying on specific enum values
        /// </summary>
        /// <summary>
        /// NEW METHOD: Get a list of player IDs from the database for testing purposes
        /// This method dynamically discovers players without hardcoding names
        /// </summary>
        private List<uint> GetTestPlayerIdsFromDatabase(int count) {
            try {
                var testIds = new List<uint>();

                using (var context = new ShardDbContext()) {
                    context.ChangeTracker.QueryTrackingBehavior = QueryTrackingBehavior.NoTracking;

                                        // Method 1: Try to find players by looking for ANY Biota that has inventory items
                    // pointing to them (they are containers for other items)
                    
                    // First, let's debug what we have in the database
                    var allCreatures = context.Biota
                        .Where(b => b.WeenieType == (int)WeenieType.Creature)
                        .Select(b => b.Id)
                        .ToList();
                    Console.WriteLine($"[DATABASE] DEBUG: Found {allCreatures.Count} total creatures in database");
                    
                    var allContainerProperties = context.BiotaPropertiesIID
                        .Where(bpi => bpi.Type == (ushort)PropertyInstanceId.Container)
                        .Select(bpi => new { bpi.ObjectId, bpi.Value })
                        .ToList();
                    Console.WriteLine($"[DATABASE] DEBUG: Found {allContainerProperties.Count} container properties in database");
                    
                    // Show some examples
                    foreach (var prop in allContainerProperties.Take(5))
                    {
                        Console.WriteLine($"[DATABASE] DEBUG: Container property - ObjectId: 0x{prop.ObjectId:X8}, Value: 0x{prop.Value:X8}");
                    }
                    
                    // Find ANY Biota that is a container (has items pointing to it)
                    var playersWithInventory = context.Biota
                        .Where(b => context.BiotaPropertiesIID
                            .Any(bpi => bpi.Type == (ushort)PropertyInstanceId.Container && bpi.Value == b.Id))
                        .Select(b => b.Id)
                        .Distinct()
                        .OrderBy(id => id)
                        .Take(count)
                        .ToList();
                    
                    testIds.AddRange(playersWithInventory);
                    Console.WriteLine($"[DATABASE] Found {testIds.Count} players with inventory in database");

                    // Method 2: Find players by looking for Biota with WeenieType.Creature
                    // that have names (indicating they are named entities, likely players)
                    if (testIds.Count < count) {
                        Console.WriteLine($"[DATABASE] Not enough players with inventory found, searching for named creatures...");

                        var remainingCount = count - testIds.Count;
                        var namedCreatures = context.Biota
                            .Where(b => b.WeenieType == (int)WeenieType.Creature)
                            .Where(b => !testIds.Contains(b.Id)) // Don't duplicate
                            .Where(b => context.BiotaPropertiesString
                                .Any(s => s.Type == (ushort)PropertyString.Name && !string.IsNullOrEmpty(s.Value)))
                            .Select(b => b.Id)
                            .Distinct()
                            .OrderBy(id => id)
                            .Take(remainingCount)
                            .ToList();

                        testIds.AddRange(namedCreatures);
                        Console.WriteLine($"[DATABASE] Added {namedCreatures.Count} named creatures (likely players)");
                    }

                    // Method 3: Find players by looking for Biota with WeenieType.Creature
                    // that have specific player-related properties
                    if (testIds.Count < count) {
                        Console.WriteLine($"[DATABASE] Still need more players, searching for creatures with player properties...");

                        var remainingCount = count - testIds.Count;
                        var playersByProperties = context.Biota
                            .Where(b => b.WeenieType == (int)WeenieType.Creature)
                            .Where(b => !testIds.Contains(b.Id)) // Don't duplicate
                            .Where(b => context.BiotaPropertiesInt
                                .Any(p => p.Type == (ushort)PropertyInt.Level && p.Value > 0)) // Players have levels
                            .Select(b => b.Id)
                            .Distinct()
                            .OrderBy(id => id)
                            .Take(remainingCount)
                            .ToList();

                        testIds.AddRange(playersByProperties);
                        Console.WriteLine($"[DATABASE] Added {playersByProperties.Count} creatures with player properties");
                    }

                    // Method 4: Try to get character IDs from the Character table as fallback
                    if (testIds.Count < count) {
                        Console.WriteLine($"[DATABASE] Still need more players, trying Character table...");

                        var remainingCount = count - testIds.Count;
                        var characterIds = context.Character
                            .Where(c => c.DeleteTime == null) // Only non-deleted characters
                            .OrderBy(c => c.Id)
                            .Take(remainingCount)
                            .Select(c => c.Id)
                            .ToList();

                        testIds.AddRange(characterIds);
                        Console.WriteLine($"[DATABASE] Added {characterIds.Count} character IDs from Character table");
                    }

                    // Method 5: Find any remaining Biota with WeenieType.Creature
                    // that might be players (broadest search)
                    if (testIds.Count < count) {
                        Console.WriteLine($"[DATABASE] Still need more players, searching for any remaining Creature biotas...");

                        var remainingCount = count - testIds.Count;
                        var additionalCreatures = context.Biota
                            .Where(b => b.WeenieType == (int)WeenieType.Creature)
                            .Where(b => !testIds.Contains(b.Id)) // Don't duplicate
                            .OrderBy(id => id)
                            .Take(remainingCount)
                            .Select(b => b.Id)
                            .ToList();

                        testIds.AddRange(additionalCreatures);
                        Console.WriteLine($"[DATABASE] Added {additionalCreatures.Count} additional creature biotas");
                    }
                }

                // Only add dummy IDs if we have very few real players (less than 3)
                if (testIds.Count < 3) {
                    var dummyCount = Math.Min(count - testIds.Count, 3); // Limit dummy IDs to 3 max
                    Console.WriteLine($"[DATABASE] Very few real players found, adding {dummyCount} dummy IDs for queue testing");

                    for (int i = 0; i < dummyCount; i++) {
                        testIds.Add((uint)(0x70000000 + i));
                    }
                }

                Console.WriteLine($"[DATABASE] Total test IDs prepared: {testIds.Count}");
                return testIds.Take(count).ToList();
            }
            catch (Exception ex) {
                Console.WriteLine($"[DATABASE] ERROR in GetTestPlayerIdsFromDatabase: {ex.Message}");
                Console.WriteLine($"[DATABASE] Stack trace: {ex.StackTrace}");
                // Return some dummy IDs for testing the queue system
                return Enumerable.Range(0, count).Select(i => (uint)(0x70000000 + i)).ToList();
            }
        }
    }
}
