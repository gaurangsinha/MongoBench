#define THREADS

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using System.Configuration;
using MongoBench.Benchmarks;

namespace MongoBench {

    /// <summary>
    /// MongoDB Benchmark
    /// </summary>
    class Program {

        /// <summary>
        /// Program execution begins here.
        /// </summary>
        /// <param name="args">The args.</param>
        [MTAThread()]
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
            Console.Write("Records to insert per thread [1000] : ");
            input = Console.ReadLine();
            if (!int.TryParse(input, out Settings.NUM_OF_RECORDS))
                Settings.NUM_OF_RECORDS = 1000;

            //Test connection string and database connection
            bool connectionStatus = TestDatabaseConnection();
            Console.WriteLine("Testing database connection... {0}", connectionStatus ? "OK" : "ERROR");

            //Initialize temporary data
            bool dataInitialized = Data.Initialize();
            Console.WriteLine("Initializing Temporary Data... {0}", dataInitialized ? "OK" : "ERROR");

            BenchmarkBase[] benchmarks = new BenchmarkBase[numOfRuns * ((numOfThreads * 3) + 1)];
            if (connectionStatus & dataInitialized) {
                Console.WriteLine("\nRun\tThread\tTimee\tBenchmark");

                for (int run = 0; run < numOfRuns; run++) {
                    //Clean database
                    CleanDatabase();

                    int index = run * ((numOfThreads * 3) + 1);

                    //Insert benchmarks
                    BenchmarkBase[] results = ExecuteBenchmarks(numOfThreads, typeof(Insert), null);
                    PrintResults(run, results);
                    Array.Copy(results, 0, benchmarks, index, results.Length);

                    //Query benchmarks
                    results = ExecuteBenchmarks(numOfThreads, typeof(CompositeQueries), string.Empty, false);
                    PrintResults(run, results);
                    index += results.Length;
                    Array.Copy(results, 0, benchmarks, index, results.Length);

                    //Create Index
                    index += results.Length;
                    benchmarks[index] = new CreateIndex();
                    benchmarks[index].ExecuteBenchmark();
                    Console.WriteLine("{0}\t-\t{1}", run + 1, benchmarks[index].ToString());

                    //Query benchmarks with indexes
                    results = ExecuteBenchmarks(numOfThreads, typeof(CompositeQueries), "Indexed ", false);
                    PrintResults(run, results);
                    index += 1;
                    Array.Copy(results, 0, benchmarks, index, results.Length);
                }
                Console.WriteLine("\nStatistics:");
                CalculateStatistics(Settings.NUM_OF_RECORDS, benchmarks);
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
                var db = MongoDB.Driver.MongoServer.Create(Settings.CONNECTION_STRING);
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
                var db = MongoDB.Driver.MongoServer.Create(Settings.CONNECTION_STRING)[ConfigurationManager.AppSettings["DATABASE_NAME"]];
                var posts = db[ConfigurationManager.AppSettings["COLLECTION_NAME"]];
                var result = posts.RemoveAll();
                db.DropCollection(ConfigurationManager.AppSettings["COLLECTION_NAME"]);
                db.Drop();
                status = (null != result) ? result.Ok : false;
            }
            catch (MongoDB.Driver.MongoException ex) {
                Debug.Write(ex.ToString());
                status = false;
            }
            return status;
        }

        /// <summary>
        /// Executes the benchmarks.
        /// </summary>
        /// <param name="numOfThreads">The num of threads.</param>
        /// <param name="benchmarkType">Type of the benchmark.</param>
        /// <param name="constructorParameters">The constructor parameters.</param>
        /// <returns></returns>
        private static BenchmarkBase[] ExecuteBenchmarks(int numOfThreads, Type benchmarkType, params object[] constructorParameters) {
            BenchmarkBase[] benchmarks = new BenchmarkBase[numOfThreads];

            //Create threads
            Mutex mutex = new Mutex(true, Settings.MUTEX_NAME);

            #if THREADS
                Thread[] tasks = new Thread[numOfThreads];
            #else
                Task[] tasks = new Task[numOfThreads];
            #endif
                    
            for (int i = 0; i < numOfThreads; i++) {
                benchmarks[i] = Activator.CreateInstance(benchmarkType, constructorParameters) as BenchmarkBase;

                #if THREADS
                    tasks[i] = new Thread(new ThreadStart(benchmarks[i].ExecuteBenchmark));
                #else
                    tasks[i] = new Task(new Action(benchmarks[i].ExecuteBenchmark));
                #endif

                tasks[i].Start();
            }

            //Start nechmark execution
            mutex.ReleaseMutex();
            mutex.Dispose();

            //Wait for threads to complete execution
            #if THREADS
                while (tasks.Any(t => t.IsAlive)) { }
            #else
                while (tasks.Any(t => !t.IsCompleted)) { }
            #endif

            return benchmarks;
        }

        /// <summary>
        /// Prints the results.
        /// </summary>
        /// <param name="run">The run.</param>
        /// <param name="benchmarks">The benchmarks.</param>
        private static void PrintResults(int run, BenchmarkBase[] benchmarks) {
            for (int i = 0; i < benchmarks.Length; i++) {
                if (null != benchmarks[i]) {
                    Console.WriteLine("{0}\t{1}\t{2}", run + 1, i + 1, benchmarks[i].ToString());
                }
            }
        }

        /// <summary>
        /// Calculates the statistics.
        /// </summary>
        /// <param name="benchmarks">The benchmarks.</param>
        private static void CalculateStatistics(int insertCount, BenchmarkBase[] benchmarks) {
            //Calculate average inserts per second
            var avgInsertsPerSecond = benchmarks
                                        .Where(b => b.GetType() == typeof(Insert))
                                        .Average(b => (double)insertCount / (double)b.Time.ElapsedMilliseconds);
            Console.WriteLine("{0:0.0000} : Average inserts/second", avgInsertsPerSecond * 1000.0);

            CalculateStatistics(
                benchmarks.Where(
                    b => b.GetType() == typeof(CompositeQueries) && !b.Type.StartsWith("Indexed")));

            var avgIndex = benchmarks
                                .Where(b => b.GetType() == typeof(CreateIndex))
                                .Average(b => b.Time.ElapsedMilliseconds);
            Console.WriteLine("{0:0.0000} : Average Create Index", avgIndex / 1000.0);

            CalculateStatistics(
                benchmarks.Where(
                    b => b.GetType() == typeof(CompositeQueries) && b.Type.StartsWith("Indexed")));
        }

        /// <summary>
        /// Calculates the statistics.
        /// </summary>
        /// <param name="benchmarks">The composite query benchmarks.</param>
        private static void CalculateStatistics(IEnumerable<BenchmarkBase> benchmarks) {
            var avgSelect = benchmarks.Average(b => ((CompositeQueries)b).SimpleQuery.Time.ElapsedMilliseconds);
            Console.WriteLine("{0:0.0000} : Average simple select", avgSelect / 1000.0);

            var avgFilter = benchmarks.Average(b => ((CompositeQueries)b).FilterQuery.Time.ElapsedMilliseconds);
            Console.WriteLine("{0:0.0000} : Average simple filter", avgFilter / 1000.0);

            var avgMapReduceTags = benchmarks.Average(b => ((CompositeQueries)b).TagCountMapReduce.Time.ElapsedMilliseconds);
            Console.WriteLine("{0:0.0000} : Average Map-Reduce tags", avgMapReduceTags / 1000.0);

            var avgMapReduceComments = benchmarks.Average(b => ((CompositeQueries)b).CommentAuthorMapReduce.Time.ElapsedMilliseconds);
            Console.WriteLine("{0:0.0000} : Average Map-Reduce comments", avgMapReduceComments / 1000.0);            
        }
    }
}
