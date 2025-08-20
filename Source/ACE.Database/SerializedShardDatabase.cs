using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

using log4net;

using ACE.Database.Entity;
using ACE.Database.Models.Shard;
using ACE.Entity.Enum;
using System.Diagnostics;
using System.Linq;
using System.Text;

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
                        
                        // Log all slow tasks
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
                            Console.WriteLine($"[DATABASE] Processed {taskCounter} tasks. Avg queue wait: {avgQueueWait:F1}ms, Current queue size: {_queue.Count}");
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

        public void GetCurrentQueueWaitTime(Action<TimeSpan> callback)
        {
            var initialCallTime = DateTime.UtcNow;
            var queueSize = _queue.Count;
            Console.WriteLine($"[DATABASE] ENQUEUE: GetCurrentQueueWaitTime at {initialCallTime:HH:mm:ss.fff}, queue size before: {queueSize}");

            _queue.Add(new Task(() =>
            {
                callback?.Invoke(DateTime.UtcNow - initialCallTime);
            }));
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
    }
}
