using ACE.Database;
using ACE.DatLoader;
using ACE.DatLoader.FileTypes;
using ACE.Entity.Enum;
using ACE.Server.Managers;
using ACE.Server.Network;
using System;
using System.Collections.Generic;
using System.Linq;

namespace ACE.Server.Command.Handlers
{
    public static class ConsoleCommands
    {
        [CommandHandler("lbcache", AccessLevel.Admin, CommandHandlerFlag.ConsoleInvoke, 0, "Write out all cached landblocks.", "")]
        public static void WriteLandblockCache(Session session, params string[] parameters)
        {
            foreach (var LI in LandblockManager.landblocks)
            {
                Console.WriteLine($"LB Keys: {LI.Key.Landblock}, {LI.Key.Variant}. Values: {LI.Value.Id.Raw}, {LI.Value.VariationId}");
            }
        }

        [CommandHandler("lbhash", AccessLevel.Admin, CommandHandlerFlag.ConsoleInvoke, 0, "Write out all hashed loaded landblocks.", "")]
        public static void WriteLandblockHash(Session session, params string[] parameters)
        {
            foreach (var LI in LandblockManager.loadedLandblocks)
            {
                Console.WriteLine($"LB Keys: {LI.Value.Id.Raw}, {LI.Value.VariationId}");
            }
        }

        [CommandHandler("queuereport", AccessLevel.Admin, CommandHandlerFlag.ConsoleInvoke, 0, "Write out all tasks in the Shard Database Queue")]
        public static void QueueReport(Session session, params string[] parameters)
        {
            var tasks = DatabaseManager.Shard.QueueReport();
            Console.WriteLine($"QueueReport, Currently {tasks.Count} Shard Database Tasks");
            foreach (var task in tasks)
            {
                Console.WriteLine(task);
            }
            Console.WriteLine("End QueueReport");
        }

        [CommandHandler("readonlyqueuereport", AccessLevel.Admin, CommandHandlerFlag.ConsoleInvoke, 0, "Write out all tasks in the Shard Database Queue")]
        public static void ReadOnlyQueueReport(Session session, params string[] parameters)
        {
            var tasks = DatabaseManager.Shard.ReadOnlyQueueReport();
            Console.WriteLine($"ReadOnlyQueueReport, Currently {tasks.Count} Shard Database Tasks");
            foreach (var task in tasks)
            {
                Console.WriteLine(task);
            }
            Console.WriteLine("End ReadOnlyQueueReport");
        }

        [CommandHandler("dbperf", AccessLevel.Admin, CommandHandlerFlag.ConsoleInvoke, 0, "Show database performance statistics and timing analysis")]
        public static void ShowDatabasePerformance(Session session, params string[] parameters)
        {
            try
            {
                var report = DatabaseManager.Shard.GetPerformanceReport();
                Console.WriteLine(report);
                
                // Also show current queue status
                var queueCount = DatabaseManager.Shard.QueueCount;
                Console.WriteLine($"\nCurrent Database Queue Status:");
                Console.WriteLine($"Active tasks in queue: {queueCount}");
                
                if (queueCount > 0)
                {
                    var queueReport = DatabaseManager.Shard.QueueReport();
                    Console.WriteLine("Tasks waiting in queue:");
                    foreach (var task in queueReport.Take(10)) // Show first 10 tasks
                    {
                        Console.WriteLine($"  - {task}");
                    }
                    if (queueReport.Count > 10)
                    {
                        Console.WriteLine($"  ... and {queueReport.Count - 10} more tasks");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error getting database performance report: {ex.Message}");
            }
        }

        [CommandHandler("dbperf-clear", AccessLevel.Admin, CommandHandlerFlag.ConsoleInvoke, 0, "Clear database performance statistics")]
        public static void ClearDatabasePerformance(Session session, params string[] parameters)
        {
            try
            {
                DatabaseManager.Shard.ClearPerformanceStats();
                Console.WriteLine("Database performance statistics cleared.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error clearing database performance statistics: {ex.Message}");
            }
        }

        [CommandHandler("dbqueue-wait", AccessLevel.Admin, CommandHandlerFlag.ConsoleInvoke, 0, "Show current database queue wait time")]
        public static void ShowDatabaseQueueWait(Session session, params string[] parameters)
        {
            try
            {
                var queueCount = DatabaseManager.Shard.QueueCount;
                Console.WriteLine($"Current database queue count: {queueCount}");
                
                if (queueCount > 0)
                {
                    DatabaseManager.Shard.GetCurrentQueueWaitTime(result =>
                    {
                        Console.WriteLine($"Estimated wait time for new tasks: {result.TotalMilliseconds:F1}ms");
                    });
                }
                else
                {
                    Console.WriteLine("Database queue is empty - no wait time.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error getting database queue wait time: {ex.Message}");
            }
        }

        [CommandHandler("dbthreads", AccessLevel.Admin, CommandHandlerFlag.ConsoleInvoke, 0, "Show database worker thread status and performance")]
        public static void ShowDatabaseThreads(Session session, params string[] parameters)
        {
            try
            {
                Console.WriteLine("=== DATABASE WORKER THREAD STATUS ===");
                
                // Get current queue status
                var queueCount = DatabaseManager.Shard.QueueCount;
                var readOnlyQueueCount = DatabaseManager.Shard.ReadOnlyQueueCount;
                
                Console.WriteLine($"Write Queue: {queueCount} tasks waiting");
                Console.WriteLine($"Read Queue: {readOnlyQueueCount} tasks waiting");
                Console.WriteLine();
                
                // Show performance report
                var report = DatabaseManager.Shard.GetPerformanceReport();
                Console.WriteLine(report);
                
                // Show current queue contents
                if (queueCount > 0)
                {
                    Console.WriteLine("\n=== WRITE QUEUE CONTENTS ===");
                    var queueReport = DatabaseManager.Shard.QueueReport();
                    foreach (var task in queueReport.Take(10))
                    {
                        Console.WriteLine($"  - {task}");
                    }
                    if (queueReport.Count > 10)
                    {
                        Console.WriteLine($"  ... and {queueReport.Count - 10} more tasks");
                    }
                }
                
                if (readOnlyQueueCount > 0)
                {
                    Console.WriteLine("\n=== READ QUEUE CONTENTS ===");
                    var readOnlyQueueReport = DatabaseManager.Shard.ReadOnlyQueueReport();
                    foreach (var task in readOnlyQueueReport.Take(10))
                    {
                        Console.WriteLine($"  - {task}");
                    }
                    if (readOnlyQueueReport.Count > 10)
                    {
                        Console.WriteLine($"  ... and {readOnlyQueueReport.Count - 10} more tasks");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error getting database thread status: {ex.Message}");
            }
        }

        [CommandHandler("bank-usage", AccessLevel.Admin, CommandHandlerFlag.ConsoleInvoke, 0, "Show bank command usage statistics for the last hour")]
        public static void ShowBankUsage(Session session, params string[] parameters)
        {
            try
            {
                var oneHourAgo = DateTime.UtcNow.AddHours(-1);
                var logFilePath = "logs/ACE.log"; // Adjust path if needed
                
                if (!System.IO.File.Exists(logFilePath))
                {
                    Console.WriteLine($"Log file not found at: {logFilePath}");
                    Console.WriteLine("Please check your log4net configuration for the correct log file path.");
                    return;
                }
                
                var bankUsageLines = new List<string>();
                var uniquePlayers = new HashSet<string>();
                var uniqueAccounts = new HashSet<uint>();
                var totalUsage = 0;
                
                // Read log file and parse bank usage entries
                foreach (var line in System.IO.File.ReadLines(logFilePath))
                {
                    if (line.Contains("[BANK_USAGE]") && line.Contains("UTC"))
                    {
                        // Parse timestamp from log line
                        var timestampMatch = System.Text.RegularExpressions.Regex.Match(line, @"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}) UTC");
                        if (timestampMatch.Success)
                        {
                            if (DateTime.TryParse(timestampMatch.Groups[1].Value, out var logTime))
                            {
                                if (logTime >= oneHourAgo)
                                {
                                    bankUsageLines.Add(line);
                                    totalUsage++;
                                    
                                    // Extract player name and account ID
                                    var playerMatch = System.Text.RegularExpressions.Regex.Match(line, @"Player: ([^(]+) \(Account: (\d+)\)");
                                    if (playerMatch.Success)
                                    {
                                        uniquePlayers.Add(playerMatch.Groups[1].Value.Trim());
                                        if (uint.TryParse(playerMatch.Groups[2].Value, out var accountId))
                                        {
                                            uniqueAccounts.Add(accountId);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                
                Console.WriteLine("=== BANK COMMAND USAGE STATISTICS (Last Hour) ===");
                Console.WriteLine($"Total /b and /bank commands: {totalUsage}");
                Console.WriteLine($"Unique players: {uniquePlayers.Count}");
                Console.WriteLine($"Unique accounts: {uniqueAccounts.Count}");
                Console.WriteLine($"Time range: {oneHourAgo:yyyy-MM-dd HH:mm:ss} UTC to {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC");
                Console.WriteLine();
                
                if (bankUsageLines.Count > 0)
                {
                    Console.WriteLine("Recent bank command usage:");
                    foreach (var line in bankUsageLines.TakeLast(20)) // Show last 20 entries
                    {
                        // Extract and format the relevant part
                        var match = System.Text.RegularExpressions.Regex.Match(line, @"\[BANK_USAGE\] (.+)");
                        if (match.Success)
                        {
                            Console.WriteLine($"  {match.Groups[1].Value}");
                        }
                    }
                    
                    if (bankUsageLines.Count > 20)
                    {
                        Console.WriteLine($"  ... and {bankUsageLines.Count - 20} more entries");
                    }
                }
                else
                {
                    Console.WriteLine("No bank command usage found in the last hour.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error getting bank usage statistics: {ex.Message}");
                Console.WriteLine($"Stack trace: {ex.StackTrace}");
            }
        }


        [CommandHandler("version", AccessLevel.Admin, CommandHandlerFlag.ConsoleInvoke, 0, "Show server version information.", "")]
        public static void ShowVersion(Session session, params string[] parameters)
        {
            var msg = ServerBuildInfo.GetVersionInfo();
            Console.WriteLine(msg);
        }

        [CommandHandler("exit", AccessLevel.Admin, CommandHandlerFlag.ConsoleInvoke, 0, "Shut down server immediately.", "")]
        public static void Exit(Session session, params string[] parameters)
        {
            AdminShardCommands.ShutdownServerNow(session, parameters);
        }

        [CommandHandler("cell-export", AccessLevel.Admin, CommandHandlerFlag.ConsoleInvoke, 1, "Export contents of CELL DAT file.", "<export-directory-without-spaces>")]
        public static void ExportCellDatContents(Session session, params string[] parameters)
        {
            if (parameters?.Length != 1)
                Console.WriteLine("cell-export <export-directory-without-spaces>");

            string exportDir = parameters[0];

            Console.WriteLine($"Exporting cell.dat contents to {exportDir}.  This can take longer than an hour.");
            DatManager.CellDat.ExtractLandblockContents(exportDir);
            Console.WriteLine($"Export of cell.dat to {exportDir} complete.");
        }

        [CommandHandler("portal-export", AccessLevel.Admin, CommandHandlerFlag.ConsoleInvoke, 1, "Export contents of PORTAL DAT file.", "<export-directory-without-spaces>")]
        public static void ExportPortalDatContents(Session session, params string[] parameters)
        {
            if (parameters?.Length != 1)
                Console.WriteLine("portal-export <export-directory-without-spaces>");

            string exportDir = parameters[0];

            Console.WriteLine($"Exporting portal.dat contents to {exportDir}.  This will take a while.");
            DatManager.PortalDat.ExtractCategorizedPortalContents(exportDir);
            Console.WriteLine($"Export of portal.dat to {exportDir} complete.");
        }

        [CommandHandler("highres-export", AccessLevel.Admin, CommandHandlerFlag.ConsoleInvoke, 1, "Export contents of client_highres.dat file.", "<export-directory-without-spaces>")]
        public static void ExportHighresDatContents(Session session, params string[] parameters)
        {
            if (DatManager.HighResDat == null)
            {
                Console.WriteLine("client_highres.dat file was not loaded.");
                return;
            }
            if (parameters?.Length != 1)
                Console.WriteLine("highres-export <export-directory-without-spaces>");

            string exportDir = parameters[0];

            Console.WriteLine($"Exporting client_highres.dat contents to {exportDir}.  This will take a while.");
            DatManager.HighResDat.ExtractCategorizedPortalContents(exportDir);
            Console.WriteLine($"Export of client_highres.dat to {exportDir} complete.");
        }

        [CommandHandler("language-export", AccessLevel.Admin, CommandHandlerFlag.ConsoleInvoke, 1, "Export contents of client_local_English.dat file.", "<export-directory-without-spaces>")]
        public static void ExportLanguageDatContents(Session session, params string[] parameters)
        {
            if (DatManager.LanguageDat == null)
            {
                Console.WriteLine("client_highres.dat file was not loaded.");
                return;
            }
            if (parameters?.Length != 1)
                Console.WriteLine("language-export <export-directory-without-spaces>");

            string exportDir = parameters[0];

            Console.WriteLine($"Exporting client_local_English.dat contents to {exportDir}.  This will take a while.");
            DatManager.LanguageDat.ExtractCategorizedPortalContents(exportDir);
            Console.WriteLine($"Export of client_local_English.dat to {exportDir} complete.");
        }

        /// <summary>
        /// Export all wav files to a specific directory.
        /// </summary>
        [CommandHandler("wave-export", AccessLevel.Admin, CommandHandlerFlag.ConsoleInvoke, 0, "Export Wave Files")]
        public static void ExportWaveFiles(Session session, params string[] parameters)
        {
            if (parameters?.Length != 1)
            {
                Console.WriteLine("wave-export <export-directory-without-spaces>");
                return;
            }

            string exportDir = parameters[0];

            Console.WriteLine($"Exporting portal.dat WAV files to {exportDir}.  This may take a while.");
            foreach (KeyValuePair<uint, DatFile> entry in DatManager.PortalDat.AllFiles)
            {
                if (entry.Value.GetFileType(DatDatabaseType.Portal) == DatFileType.Wave)
                {
                    var wave = DatManager.PortalDat.ReadFromDat<Wave>(entry.Value.ObjectId);

                    wave.ExportWave(exportDir);
                }
            }
            Console.WriteLine($"Export to {exportDir} complete.");
        }

        /// <summary>
        /// Export all texture/image files to a specific directory.
        /// </summary>
        [CommandHandler("image-export", AccessLevel.Admin, CommandHandlerFlag.ConsoleInvoke, 0, "Export Texture/Image Files")]
        public static void ExportImageFile(Session session, params string[] parameters)
        {
            string syntax = "image-export <export-directory-without-spaces> [id]";
            if (parameters?.Length < 1)
            {
                Console.WriteLine(syntax);
                return;
            }

            string exportDir = parameters[0];
            if (exportDir.Length == 0 || !System.IO.Directory.Exists(exportDir))
            {
                Console.WriteLine(syntax);
                return;
            }

            if (parameters.Length > 1)
            {
                uint imageId;
                if (parameters[1].StartsWith("0x"))
                {
                    string hex = parameters[1].Substring(2);
                    if (!uint.TryParse(hex, System.Globalization.NumberStyles.HexNumber, System.Globalization.CultureInfo.CurrentCulture, out imageId))
                    {
                        Console.WriteLine(syntax);
                        return;
                    }
                }
                else
                if (!uint.TryParse(parameters[1], out imageId))
                {
                    Console.WriteLine(syntax);
                    return;
                }

                var image = DatManager.PortalDat.ReadFromDat<Texture>(imageId);
                image.ExportTexture(exportDir);

                Console.WriteLine($"Exported " + imageId.ToString("X8") + " to " + exportDir + ".");
            }
            else
            {
                int portalFiles = 0;
                int highresFiles = 0;
                Console.WriteLine($"Exporting client_portal.dat textures and images to {exportDir}.  This may take a while.");
                foreach (KeyValuePair<uint, DatFile> entry in DatManager.PortalDat.AllFiles)
                {
                    if (entry.Value.GetFileType(DatDatabaseType.Portal) == DatFileType.Texture)
                    {
                        var image = DatManager.PortalDat.ReadFromDat<Texture>(entry.Value.ObjectId);
                        image.ExportTexture(exportDir);
                        portalFiles++;
                    }
                }
                Console.WriteLine($"Exported {portalFiles} total files from client_portal.dat to {exportDir}.");

                if (DatManager.HighResDat != null)
                {
                    foreach (KeyValuePair<uint, DatFile> entry in DatManager.HighResDat.AllFiles)
                    {
                        if (entry.Value.GetFileType(DatDatabaseType.Portal) == DatFileType.Texture)
                        {
                            var image = DatManager.HighResDat.ReadFromDat<Texture>(entry.Value.ObjectId);
                            image.ExportTexture(exportDir);
                            highresFiles++;
                        }
                    }
                    Console.WriteLine($"Exported {highresFiles} total files from client_highres.dat to {exportDir}.");
                }
                int totalFiles = portalFiles + highresFiles;
                Console.WriteLine($"Exported {totalFiles} total files to {exportDir}.");
            }
        }
    }
}
