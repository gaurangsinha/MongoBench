using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using System.Configuration;

namespace MongoBench {

    /// <summary>
    /// 
    /// </summary>
    class Program {

        /// <summary>
        /// Database connection string
        /// </summary>
        private static string CONNECTION_STRING = ConfigurationManager.ConnectionStrings["MongoConnectionString"].ConnectionString;
        
        /// <summary>
        /// Mutext name to synchronize threads
        /// </summary>
        private static string MUTEX_NAME = ConfigurationManager.AppSettings["MUTEX_NAME"];

        /// <summary>
        /// Database name on which the benchmarks will be performed
        /// </summary>
        private static string DATABASE_NAME = ConfigurationManager.AppSettings["DATABASE_NAME"];

        /// <summary>
        /// Name of the collection on which records will be inserted and queries executed
        /// </summary>
        private static string COLLECTION_NAME = ConfigurationManager.AppSettings["COLLECTION_NAME"];

        /// <summary>
        /// Specifies if inserts are to be done in safe mode
        /// </summary>
        private static bool INSERT_SAFE_MODE = bool.Parse(ConfigurationManager.AppSettings["INSERT_SAFE_MODE"]);

        /// <summary>
        /// The fields on which indexes are supposed to be created
        /// </summary>
        private static string[] INDEX_FIELDS = ConfigurationManager.AppSettings["INDEX_FIELDS"].Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries);

        /// <summary>
        /// Program execution begins here.
        /// </summary>
        /// <param name="args">The args.</param>
        static void Main(string[] args) {
            string input = null;

            //Number of runs
            int numOfRuns = 3;
            Console.Write("Number of runs [3] : ");
            input = Console.ReadLine();
            if (!int.TryParse(input, out numOfRuns))
                numOfRuns = 3;

            //Number of threads per run
            int numOfThreads = 1;
            Console.Write("Number of simultaneous threads [1] : ");
            input = Console.ReadLine();
            if (!int.TryParse(input, out numOfThreads))
                numOfThreads = 1;

            //Number of records to insert per thread
            int numOfRecords = 1000;
            Console.Write("Records to insert per thread [1000] : ");
            input = Console.ReadLine();
            if (!int.TryParse(input, out numOfRecords))
                numOfRecords = 1000;

            //Test connection string and database connection
            bool connectionStatus = TestDatabaseConnection();
            Console.WriteLine("Testing database connection... {0}", connectionStatus ? "OK" : "ERROR");

            //Initialize temporary data
            bool dataInitialized = Data.Initialize();
            Console.WriteLine("Initializing Temporary Data... {0}", dataInitialized ? "OK" : "ERROR");

            Benchmark[] allBenchmarks = new Benchmark[numOfRuns * numOfThreads];
            if (connectionStatus & dataInitialized) {
                Console.WriteLine("\nRun\tThread\tName\t\t\tValue");

                for (int run = 0; run < numOfRuns; run++) {
                    //Clear database
                    bool cleanStatus = CleanDatabase();
                    Debug.WriteLine("Cleaning database, removing existing entries... {0}", cleanStatus ? "OK" : "ERROR");

                    //Create threads
                    Debug.WriteLine("Creating Threads");
                    Mutex mutex = new Mutex(true, MUTEX_NAME);
                    Benchmark[] benchmarks = new Benchmark[numOfThreads];
                    Task[] tasks = new Task[numOfThreads];
                    for (int i = 0; i < numOfThreads; i++) {
                        benchmarks[i] = new Benchmark(numOfRecords,
                                            CONNECTION_STRING,
                                            MUTEX_NAME,
                                            DATABASE_NAME,
                                            COLLECTION_NAME,
                                            INDEX_FIELDS,
                                            INSERT_SAFE_MODE);
                        tasks[i] = new Task(new Action(benchmarks[i].WaitForSignal));
                        tasks[i].Start();
                    }

                    //Start nechmark execution
                    Debug.WriteLine("Run {0}, Starting to execute {1} thread", run + 1, numOfThreads);
                    mutex.ReleaseMutex();
                    mutex.Dispose();

                    //Wait for threads to complete execution
                    while (tasks.Any(t => !t.IsCompleted)) { }

                    //Copy to global array, required to calculate statistics
                    Array.Copy(benchmarks, 0, allBenchmarks, run * benchmarks.Length, benchmarks.Length);

                    //Print results
                    PrintResults(run, benchmarks);
                }
                Console.WriteLine("\nStatistics:");
                CalculateStatistics(numOfRecords, allBenchmarks);
                Console.WriteLine("\nCompleted.");
            }
        }

        /// <summary>
        /// Tests the database connection.
        /// </summary>
        /// <returns></returns>
        private static bool TestDatabaseConnection() {
            bool status = false;
            try {
                var db = MongoDB.Driver.MongoServer.Create(CONNECTION_STRING);
                db.Connect();
                status = MongoDB.Driver.MongoServerState.Connected == db.State;
                db.Disconnect();
            }
            catch (MongoDB.Driver.MongoException ex) {
                Debug.Write(ex.ToString());
                status = false;
            }
            return status;
        }

        /// <summary>
        /// Cleans the database.
        /// </summary>
        /// <returns></returns>
        private static bool CleanDatabase() {
            bool status = false;
            try {
                var db = MongoDB.Driver.MongoServer.Create(CONNECTION_STRING)[ConfigurationManager.AppSettings["DATABASE_NAME"]];
                var posts = db[ConfigurationManager.AppSettings["COLLECTION_NAME"]];
                var result = posts.RemoveAll();
                db.DropCollection(ConfigurationManager.AppSettings["COLLECTION_NAME"]);
                status = (null != result) ? result.Ok : false;
            }
            catch (MongoDB.Driver.MongoException ex) {
                Debug.Write(ex.ToString());
                status = false;
            }
            return status;
        }

        /// <summary>
        /// Prints the results.
        /// </summary>
        /// <param name="bench">The bench.</param>
        private static void PrintResults(int run, int thread, Benchmark bench) {
            if (null != bench.InsertStats)
                Console.WriteLine("{0}\t{1}\tInserts\t\t\t{2:0.0000}", run + 1, thread + 1, bench.InsertStats.ElapsedMilliseconds / 1000.0);
            if (null != bench.SimpleSelectStats)
                Console.WriteLine("{0}\t{1}\tSelect\t\t\t{2:0.0000}", run + 1, thread + 1, bench.SimpleSelectStats.ElapsedMilliseconds / 1000.0);
            if (null != bench.SimpleFilterStats)
                Console.WriteLine("{0}\t{1}\tFilter\t\t\t{2:0.0000}", run + 1, thread + 1, bench.SimpleFilterStats.ElapsedMilliseconds / 1000.0);
            if (null != bench.MapReduceTagStats)
                Console.WriteLine("{0}\t{1}\tMap-Reduce Tags\t\t{2:0.0000}", run + 1, thread + 1, bench.MapReduceTagStats.ElapsedMilliseconds / 1000.0);
            if (null != bench.MapReduceCommentStats)
                Console.WriteLine("{0}\t{1}\tMap-Reduce Comments\t{2:0.0000}", run + 1, thread + 1, bench.MapReduceCommentStats.ElapsedMilliseconds / 1000.0);
            if (null != bench.CreateIndexStats)
                Console.WriteLine("{0}\t{1}\tIndex Fields\t\t{2:0.0000}", run + 1, thread + 1, bench.CreateIndexStats.ElapsedMilliseconds / 1000.0);
            if (null != bench.IndexedSimpleSelectStats)
                Console.WriteLine("{0}\t{1}\tSelect with Index\t{2:0.0000}", run + 1, thread + 1, bench.IndexedSimpleSelectStats.ElapsedMilliseconds / 1000.0);
            if (null != bench.IndexedSimpleFilterStats)
                Console.WriteLine("{0}\t{1}\tFilter on Index\t\t{2:0.0000}", run + 1, thread + 1, bench.IndexedSimpleFilterStats.ElapsedMilliseconds / 1000.0);
            if (null != bench.IndexedMapReduceTagStats)
                Console.WriteLine("{0}\t{1}\tMap-Reduce Indexed\t{2:0.0000}", run + 1, thread + 1, bench.IndexedMapReduceTagStats.ElapsedMilliseconds / 1000.0);
        }

        /// <summary>
        /// Prints the results.
        /// </summary>
        /// <param name="run">The run.</param>
        /// <param name="benchmarks">The benchmarks.</param>
        private static void PrintResults(int run, Benchmark[] benchmarks) {
            for (int i = 0; i < benchmarks.Length; i++) {
                PrintResults(run, i, benchmarks[i]);
            }
        }

        /// <summary>
        /// Calculates the statistics.
        /// </summary>
        /// <param name="benchmarks">The benchmarks.</param>
        private static void CalculateStatistics(int insertCount, Benchmark[] benchmarks) {
            //Calculate average inserts per second
            var avgInsertsPerSecond = benchmarks.Average(b => (double)insertCount / (double)b.InsertStats.ElapsedMilliseconds);
            Console.WriteLine("{0:0.0000} : Average inserts/second", avgInsertsPerSecond * 1000.0);

            var avgSelect = benchmarks.Average(b => b.SimpleSelectStats.ElapsedMilliseconds);
            Console.WriteLine("{0:0.0000} : Average simple select", avgSelect / 1000.0);

            var avgFilter = benchmarks.Average(b => b.SimpleFilterStats.ElapsedMilliseconds);
            Console.WriteLine("{0:0.0000} : Average simple filter", avgFilter / 1000.0);

            var avgMapReduceTags = benchmarks.Average(b => b.MapReduceTagStats.ElapsedMilliseconds);
            Console.WriteLine("{0:0.0000} : Average Map-Reduce tags", avgMapReduceTags / 1000.0);

            var avgMapReduceComments = benchmarks.Average(b => b.MapReduceCommentStats.ElapsedMilliseconds);
            Console.WriteLine("{0:0.0000} : Average Map-Reduce comments", avgMapReduceComments / 1000.0);

            var avgIndex = benchmarks.Average(b => b.CreateIndexStats.ElapsedMilliseconds);
            Console.WriteLine("{0:0.0000} : Average Create Index", avgIndex / 1000.0);

            var avgSelectIndexed = benchmarks.Average(b => b.IndexedSimpleSelectStats.ElapsedMilliseconds);
            Console.WriteLine("{0:0.0000} : Average simple select indexed", avgSelectIndexed / 1000.0);

            var avgFilterIndexed = benchmarks.Average(b => b.IndexedSimpleFilterStats.ElapsedMilliseconds);
            Console.WriteLine("{0:0.0000} : Average simple filter indexed", avgFilterIndexed / 1000.0);

            var avgMapReduceTagIndexed = benchmarks.Average(b => b.IndexedMapReduceTagStats.ElapsedMilliseconds);
            Console.WriteLine("{0:0.0000} : Average Map-Reduce tags indexed", avgMapReduceTagIndexed / 1000.0);
        }
    }
}
