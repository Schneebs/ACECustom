using System;
using System.Collections.Generic;
using System.Linq;

using Microsoft.EntityFrameworkCore;

using ACE.Database;
using ACE.Database.Models.Shard;
using ACE.Entity.Enum;
using ACE.Server.Command.Handlers.Processors;
using ACE.Server.Managers;
using ACE.Server.Network;

using log4net;

namespace ACE.Server.Command.Handlers
{
    public static class DeveloperDatabaseCommands
    {
        private static readonly ILog log = LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        [CommandHandler("databasequeueinfo", AccessLevel.Developer, CommandHandlerFlag.None, 0, "Show database queue information.")]
        public static void HandleDatabaseQueueInfo(Session session, params string[] parameters)
        {
            CommandHandlerHelper.WriteOutputInfo(session, $"Current database queue count: {DatabaseManager.Shard.QueueCount}");

            DatabaseManager.Shard.GetCurrentQueueWaitTime(result =>
            {
                CommandHandlerHelper.WriteOutputInfo(session, $"Current database queue wait time: {result.TotalMilliseconds:N0} ms");
            });
        }

        [CommandHandler("databaseperftest", AccessLevel.Developer, CommandHandlerFlag.None, 0, "Test server/database performance.", "biotasPerTest\n" + "optional parameter biotasPerTest if omitted 1000")]
        public static void HandleDatabasePerfTest(Session session, params string[] parameters)
        {
            int biotasPerTest = DatabasePerfTest.DefaultBiotasTestCount;

            if (parameters?.Length > 0)
                int.TryParse(parameters[0], out biotasPerTest);

            var processor = new DatabasePerfTest();
            processor.RunAsync(session, biotasPerTest);
        }

        [CommandHandler("databasetest-singlethread", AccessLevel.Developer, CommandHandlerFlag.None, 0, "Test single-threaded vs multi-threaded database performance.", "testCount\n" + "optional parameter testCount if omitted 100")]
        public static void HandleDatabaseTestSingleThread(Session session, params string[] parameters)
        {
            int testCount = 100;

            if (parameters?.Length > 0)
                int.TryParse(parameters[0], out testCount);

            CommandHandlerHelper.WriteOutputInfo(session, $"Starting single-threaded performance test with {testCount} operations...");
            
            // Run the test in a background thread to avoid blocking
            System.Threading.Tasks.Task.Run(() =>
            {
                try
                {
                    var serializedDb = DatabaseManager.Shard as SerializedShardDatabase;
                    if (serializedDb != null)
                    {
                        serializedDb.TestSingleThreadedPerformance(testCount);
                        CommandHandlerHelper.WriteOutputInfo(session, "Single-threaded performance test completed. Check console for detailed results.");
                    }
                    else
                    {
                        CommandHandlerHelper.WriteOutputInfo(session, "Error: SerializedShardDatabase not available for testing.");
                    }
                }
                catch (Exception ex)
                {
                    CommandHandlerHelper.WriteOutputInfo(session, $"Error during performance test: {ex.Message}");
                }
            });
        }

        [CommandHandler("databasequeue-analysis", AccessLevel.Developer, CommandHandlerFlag.None, 0, "Get detailed database queue analysis to identify bottlenecks.")]
        public static void HandleDatabaseQueueAnalysis(Session session, params string[] parameters)
        {
            var serializedDb = DatabaseManager.Shard as SerializedShardDatabase;
            if (serializedDb != null)
            {
                var analysis = serializedDb.GetDetailedQueueAnalysis();
                CommandHandlerHelper.WriteOutputInfo(session, "=== DATABASE QUEUE ANALYSIS ===");
                
                // Split the analysis into chunks for better readability
                var lines = analysis.Split('\n');
                foreach (var line in lines)
                {
                    if (!string.IsNullOrWhiteSpace(line))
                    {
                        CommandHandlerHelper.WriteOutputInfo(session, line.Trim());
                    }
                }
            }
            else
            {
                CommandHandlerHelper.WriteOutputInfo(session, "Error: SerializedShardDatabase not available for analysis.");
            }
        }

        [CommandHandler("databasequeue-stress", AccessLevel.Developer, CommandHandlerFlag.None, 0, "Stress test the database queue to identify bottlenecks.", "operationCount\n" + "optional parameter operationCount if omitted 1000")]
        public static void HandleDatabaseQueueStress(Session session, params string[] parameters)
        {
            int operationCount = 1000;

            if (parameters?.Length > 0)
                int.TryParse(parameters[0], out operationCount);

            CommandHandlerHelper.WriteOutputInfo(session, $"Starting database queue stress test with {operationCount} operations...");
            
            // Run the stress test in a background thread
            System.Threading.Tasks.Task.Run(() =>
            {
                try
                {
                    var serializedDb = DatabaseManager.Shard as SerializedShardDatabase;
                    if (serializedDb != null)
                    {
                        serializedDb.StressTestQueue(operationCount);
                        CommandHandlerHelper.WriteOutputInfo(session, "Stress test completed. Monitor console for detailed performance metrics.");
                    }
                    else
                    {
                        CommandHandlerHelper.WriteOutputInfo(session, "Error: SerializedShardDatabase not available for stress testing.");
                    }
                }
                catch (Exception ex)
                {
                    CommandHandlerHelper.WriteOutputInfo(session, $"Error during stress test: {ex.Message}");
                }
            });
        }

        [CommandHandler("databasequeue-quickcompare", AccessLevel.Developer, CommandHandlerFlag.None, 0, "Quick performance comparison between single-threaded and multi-threaded execution.", "testCount\n" + "optional parameter testCount if omitted 50")]
        public static void HandleDatabaseQuickCompare(Session session, params string[] parameters)
        {
            int testCount = 50;

            if (parameters?.Length > 0)
                int.TryParse(parameters[0], out testCount);

            CommandHandlerHelper.WriteOutputInfo(session, $"Starting quick performance comparison test with {testCount} operations...");
            
            // Run the test in a background thread to avoid blocking
            System.Threading.Tasks.Task.Run(() =>
            {
                try
                {
                    var serializedDb = DatabaseManager.Shard as SerializedShardDatabase;
                    if (serializedDb != null)
                    {
                        serializedDb.QuickPerformanceComparison(testCount);
                        CommandHandlerHelper.WriteOutputInfo(session, "Quick performance comparison completed. Check console for results.");
                    }
                    else
                    {
                        CommandHandlerHelper.WriteOutputInfo(session, "Error: SerializedShardDatabase not available for testing.");
                    }
                }
                catch (Exception ex)
                {
                    CommandHandlerHelper.WriteOutputInfo(session, $"Error during performance comparison: {ex.Message}");
                }
            });
        }

        [CommandHandler("databasequeue-cancel", AccessLevel.Developer, CommandHandlerFlag.None, 0, "Cancel any running database performance tests.")]
        public static void HandleDatabaseCancelTests(Session session, params string[] parameters)
        {
            var serializedDb = DatabaseManager.Shard as SerializedShardDatabase;
            if (serializedDb != null)
            {
                serializedDb.CancelTests();
                CommandHandlerHelper.WriteOutputInfo(session, "Test cancellation requested. Tests will stop at next check.");
            }
            else
            {
                CommandHandlerHelper.WriteOutputInfo(session, "Error: SerializedShardDatabase not available.");
            }
        }

        [CommandHandler("fix-shortcut-bars", AccessLevel.Admin, CommandHandlerFlag.ConsoleInvoke, "Fixes the players with duplicate items on their shortcut bars.", "<execute>")]
        public static void HandleFixShortcutBars(Session session, params string[] parameters)
        {
            Console.WriteLine();

            Console.WriteLine("This command will attempt to fix duplicate shortcuts found in player shortcut bars. Unless explictly indicated, command will dry run only");
            Console.WriteLine("If the command outputs nothing or errors, you are ready to proceed with updating your shard db with 2019-04-17-00-Character_Shortcut_Changes.sql script");

            Console.WriteLine();

            var execute = false;

            if (parameters.Length < 1)
                Console.WriteLine("This will be a dry run and show which characters that would be affected. To perform fix, please use command: fix-shortcut-bars execute");
            else if (parameters[0].ToLower() == "execute")
                execute = true;
            else
                Console.WriteLine("Please use command fix-shortcut-bars execute");

            using (var ctx = new ShardDbContext())
            {
                var results = ctx.CharacterPropertiesShortcutBar
                    .FromSqlRaw("SELECT * FROM character_properties_shortcut_bar ORDER BY character_Id, shortcut_Bar_Index, id")
                    .ToList();

                var sqlCommands = new List<string>();

                uint characterId = 0;
                string playerName = null;
                var idxToObj = new Dictionary<uint, uint>();
                var objToIdx = new Dictionary<uint, uint>();
                var buggedChar = false;
                var buggedPlayerCount = 0;

                foreach (var result in results)
                {
                    if (characterId != result.CharacterId)
                    {
                        if (buggedChar)
                        {
                            buggedPlayerCount++;
                            Console.WriteLine($"Player {playerName} ({characterId}) was found to have errors in their shortcuts.");
                            sqlCommands.AddRange(OutputShortcutSQLCommand(playerName, characterId, idxToObj));
                            buggedChar = false;
                        }

                        // begin parsing new character
                        characterId = result.CharacterId;
                        var player = PlayerManager.FindByGuid(characterId);
                        playerName = player != null ? player.Name : $"{characterId:X8}";
                        idxToObj = new Dictionary<uint, uint>();
                        objToIdx = new Dictionary<uint, uint>();
                    }

                    var dupeIdx = idxToObj.ContainsKey(result.ShortcutBarIndex);
                    var dupeObj = objToIdx.ContainsKey(result.ShortcutObjectId);

                    if (dupeIdx || dupeObj)
                    {
                        //Console.WriteLine($"Player: {playerName}, Idx: {result.ShortcutBarIndex}, Obj: {result.ShortcutObjectId:X8} ({result.Id})");
                        buggedChar = true;
                    }

                    objToIdx[result.ShortcutObjectId] = result.ShortcutBarIndex;

                    if (!dupeObj)
                        idxToObj[result.ShortcutBarIndex] = result.ShortcutObjectId;
                }

                if (buggedChar)
                {
                    Console.WriteLine($"Player {playerName} ({characterId}) was found to have errors in their shortcuts.");
                    buggedPlayerCount++;
                    sqlCommands.AddRange(OutputShortcutSQLCommand(playerName, characterId, idxToObj));
                }

                Console.WriteLine($"Total players found with bugged shortcuts: {buggedPlayerCount}");

                if (execute)
                {
                    Console.WriteLine("Executing changes...");

                    foreach (var cmd in sqlCommands)
                        ctx.Database.ExecuteSqlRaw(cmd);
                }
                else
                    Console.WriteLine("dry run completed. Use fix-shortcut-bars execute to actually run command");
            }
        }

        public static List<string> OutputShortcutSQLCommand(string playerName, uint characterID, Dictionary<uint, uint> idxToObj)
        {
            var strings = new List<string>();

            strings.Add($"DELETE FROM `character_properties_shortcut_bar` WHERE `character_Id`={characterID};");

            foreach (var shortcut in idxToObj)
                strings.Add($"INSERT INTO `character_properties_shortcut_bar` SET `character_Id`={characterID}, `shortcut_Bar_Index`={shortcut.Key}, `shortcut_Object_Id`={shortcut.Value};");

            return strings;
        }

        [CommandHandler("database-shard-cache-pbrt", AccessLevel.Developer, CommandHandlerFlag.None, 0, "Shard Database, Player Biota Cache - Retention Time (in minutes)")]
        public static void HandleDatabaseShardCachePBRT(Session session, params string[] parameters)
        {
            if (!(DatabaseManager.Shard.BaseDatabase is ShardDatabaseWithCaching shardDatabaseWithCaching))
            {
                CommandHandlerHelper.WriteOutputInfo(session, "DatabaseManager is not using ShardDatabaseWithCaching");

                return;
            }

            if (parameters == null || parameters.Length == 0)
            {
                CommandHandlerHelper.WriteOutputInfo(session, $"Shard Database, Player Biota Cache - Retention Time {shardDatabaseWithCaching.PlayerBiotaRetentionTime.TotalMinutes:N0} m");

                return;
            }

            if (!int.TryParse(parameters[0], out var value) || value < 0)
            {
                CommandHandlerHelper.WriteOutputInfo(session, "Unable to parse argument. Specify retention time in integer minutes.");

                return;
            }

            shardDatabaseWithCaching.PlayerBiotaRetentionTime = TimeSpan.FromMinutes(value);

            CommandHandlerHelper.WriteOutputInfo(session, $"Shard Database, Player Biota Cache - Retention Time {shardDatabaseWithCaching.PlayerBiotaRetentionTime.TotalMinutes:N0} m");
        }

        [CommandHandler("database-shard-cache-npbrt", AccessLevel.Developer, CommandHandlerFlag.None, 0, "Shard Database, Non-Player Biota Cache - Retention Time (in minutes)")]
        public static void HandleDatabaseShardCacheNPBRT(Session session, params string[] parameters)
        {
            if (!(DatabaseManager.Shard.BaseDatabase is ShardDatabaseWithCaching shardDatabaseWithCaching))
            {
                CommandHandlerHelper.WriteOutputInfo(session, "DatabaseManager is not using ShardDatabaseWithCaching");

                return;
            }

            if (parameters == null || parameters.Length == 0)
            {
                CommandHandlerHelper.WriteOutputInfo(session, $"Shard Database, Non-Player Biota Cache - Retention Time {shardDatabaseWithCaching.NonPlayerBiotaRetentionTime.TotalMinutes:N0} m");

                return;
            }

            if (!int.TryParse(parameters[0], out var value) || value < 0)
            {
                CommandHandlerHelper.WriteOutputInfo(session, "Unable to parse argument. Specify retention time in integer minutes.");

                return;
            }

            shardDatabaseWithCaching.NonPlayerBiotaRetentionTime = TimeSpan.FromMinutes(value);

            CommandHandlerHelper.WriteOutputInfo(session, $"Shard Database, Non-Player Biota Cache - Retention Time {shardDatabaseWithCaching.NonPlayerBiotaRetentionTime.TotalMinutes:N0} m");
        }

        [CommandHandler("fix-spell-bars", AccessLevel.Admin, CommandHandlerFlag.ConsoleInvoke, "Fixes the players spell bars.", "<execute>")]
        public static void HandleFixSpellBars(Session session, params string[] parameters)
        {
            Console.WriteLine();

            Console.WriteLine("This command will attempt to fix player spell bars. Unless explictly indicated, command will dry run only");
            Console.WriteLine("You must have executed 2020-04-11-00-Update-Character-SpellBars.sql script first before running this command");

            Console.WriteLine();

            var execute = false;

            if (parameters.Length < 1)
                Console.WriteLine("This will be a dry run and show which characters that would be affected. To perform fix, please use command: fix-spell-bars execute");
            else if (parameters[0].ToLower() == "execute")
                execute = true;
            else
                Console.WriteLine("Please use command fix-spell-bars execute");


            if (!execute)
            {
                Console.WriteLine();
                Console.WriteLine("Press enter to start.");
                Console.ReadLine();
            }

            var numberOfRecordsFixed = 0;

            log.Info($"Starting FixSpellBarsPR2918 process. This could take a while...");

            using (var context = new ShardDbContext())
            {
                var characterSpellBarsNotFixed = context.CharacterPropertiesSpellBar.Where(c => c.SpellBarNumber == 0).ToList();

                if (characterSpellBarsNotFixed.Count > 0)
                {
                    log.Warn("2020-04-11-00-Update-Character-SpellBars.sql patch not yet applied. Please apply this patch ASAP! Skipping FixSpellBarsPR2918 for now...");
                    log.Fatal("2020-04-11-00-Update-Character-SpellBars.sql patch not yet applied. You must apply this patch before proceeding further...");
                    return;
                }

                var characterSpellBars = context.CharacterPropertiesSpellBar.OrderBy(c => c.CharacterId).ThenBy(c => c.SpellBarNumber).ThenBy(c => c.SpellBarIndex).ToList();

                uint characterId = 0;
                uint spellBarNumber = 0;
                uint spellBarIndex = 0;

                foreach (var entry in characterSpellBars)
                {
                    if (entry.CharacterId != characterId)
                    {
                        characterId = entry.CharacterId;
                        spellBarIndex = 0;
                    }

                    if (entry.SpellBarNumber != spellBarNumber)
                    {
                        spellBarNumber = entry.SpellBarNumber;
                        spellBarIndex = 0;
                    }

                    spellBarIndex++;

                    if (entry.SpellBarIndex != spellBarIndex)
                    {
                        Console.WriteLine($"FixSpellBarsPR2918: Character 0x{entry.CharacterId:X8}, SpellBarNumber = {entry.SpellBarNumber} | SpellBarIndex = {entry.SpellBarIndex:000}; Fixed - {spellBarIndex:000}");
                        entry.SpellBarIndex = spellBarIndex;
                        numberOfRecordsFixed++;
                    }
                    else
                    {
                        Console.WriteLine($"FixSpellBarsPR2918: Character 0x{entry.CharacterId:X8}, SpellBarNumber = {entry.SpellBarNumber} | SpellBarIndex = {entry.SpellBarIndex:000}; OK");
                    }
                }

                // Save
                if (execute)
                {
                    Console.WriteLine("Saving changes...");
                    context.SaveChanges();
                    log.Info($"Fixed {numberOfRecordsFixed:N0} CharacterPropertiesSpellBar records.");
                }
                else
                {
                    Console.WriteLine($"{numberOfRecordsFixed:N0} CharacterPropertiesSpellBar records need to be fixed!");
                    Console.WriteLine("dry run completed. Use fix-spell-bars execute to actually run command");
                }
            }
        }

        [CommandHandler("databasequeue-inventoryscan", AccessLevel.Developer, CommandHandlerFlag.None, 0, "Test REAL inventory scan performance by querying multiple players' inventories in parallel.", "playerCount\n" + "optional parameter playerCount if omitted 10")]
        public static void HandleDatabaseInventoryScanTest(Session session, params string[] parameters)
        {
            int playerCount = 10;
            if (parameters?.Length > 0)
                int.TryParse(parameters[0], out playerCount);

            // Limit the test to reasonable bounds
            if (playerCount < 1)
            {
                CommandHandlerHelper.WriteOutputInfo(session, "Player count must be at least 1. Using default of 10.");
                playerCount = 10;
            }
            else if (playerCount > 100)
            {
                CommandHandlerHelper.WriteOutputInfo(session, "Player count limited to 100 for performance reasons. Using 100.");
                playerCount = 100;
            }

            CommandHandlerHelper.WriteOutputInfo(session, $"Starting REAL inventory scan performance test with {playerCount} players...");
            CommandHandlerHelper.WriteOutputInfo(session, "This will perform actual database queries to simulate real inventory scans.");
            CommandHandlerHelper.WriteOutputInfo(session, "Monitor the console for detailed progress and results.");
            
            // Run the test in a background thread to avoid blocking
            System.Threading.Tasks.Task.Run(() =>
            {
                try
                {
                    var serializedDb = DatabaseManager.Shard as SerializedShardDatabase;
                    if (serializedDb != null)
                    {
                        serializedDb.TestInventoryScanPerformance(playerCount);
                        CommandHandlerHelper.WriteOutputInfo(session, "Inventory scan performance test completed. Check console for detailed results.");
                    }
                    else
                    {
                        CommandHandlerHelper.WriteOutputInfo(session, "Error: SerializedShardDatabase not available for testing.");
                    }
                }
                catch (Exception ex)
                {
                    CommandHandlerHelper.WriteOutputInfo(session, $"Error during inventory scan test: {ex.Message}");
                }
            });
        }
    }
}
