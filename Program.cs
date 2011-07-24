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
            //Setup global exception handler
            AppDomain.CurrentDomain.UnhandledException += new UnhandledExceptionEventHandler(CurrentDomain_UnhandledException);

            //Gather user input
            string input = null;

            //Select server
            int server = 0;
            Console.WriteLine("MongoDB Servers");
            Console.WriteLine("0. All");
            for (int i = 0; i < Settings.CONNECTION_STRING.Count; i++) {
                Console.WriteLine("{0}. {1}", i + 1, Settings.CONNECTION_STRING[i].Key);
            }
            Console.Write("Select server [0] : ");
            input = Console.ReadLine();
            if (!int.TryParse(input, out server))
                server = 0;
            
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
            int numOfRecords = 0;
            Console.Write("Records to insert per thread [1000] : ");
            input = Console.ReadLine();
            if (!int.TryParse(input, out numOfRecords))
                numOfRecords = 1000;

            //Initialize temporary data
            Log.Info("Initializing temporary data");
            bool dataInitialized = Data.Initialize();
            if (dataInitialized) {
                Log.Info("Temporary data initialized successfully");
                MultiRunBenchmark[] benchmarks;
                if (0 == server) {
                    benchmarks = new MultiRunBenchmark[Settings.CONNECTION_STRING.Count];
                    for (int i = 0; i < Settings.CONNECTION_STRING.Count; i++) {
                        benchmarks[i] = new MultiRunBenchmark(numOfRuns,
                                    Settings.CONNECTION_STRING[i].Key,
                                    Settings.CONNECTION_STRING[i].Value,
                                    numOfThreads,
                                    numOfRecords,
                                    Settings.DATABASE_NAME,
                                    Settings.COLLECTION_NAME,
                                    Settings.MUTEX_NAME,
                                    Settings.INDEX_FIELDS);
                        benchmarks[i].Run();
                        //CalculateStatistics(b.Results);
                    }
                }
                else {
                    benchmarks = new MultiRunBenchmark[1];
                    benchmarks[0] = new MultiRunBenchmark(numOfRuns,
                                Settings.CONNECTION_STRING[server - 1].Key,
                                Settings.CONNECTION_STRING[server - 1].Value,
                                numOfThreads,
                                numOfRecords,
                                Settings.DATABASE_NAME,
                                Settings.COLLECTION_NAME,
                                Settings.MUTEX_NAME,
                                Settings.INDEX_FIELDS);
                    benchmarks[0].Run();
                    //CalculateStatistics(b.Results);
                }

                //Print reports
                Report report = new Report(Settings.TEMPLATE_REPORT_FILE, Settings.OUTPUT_REPORT_FILE);
                report.PrintReport(benchmarks);
            }
            Log.Info("Benchmarks completed");
        }

        /// <summary>
        /// Handles the UnhandledException event of the CurrentDomain control.
        /// </summary>
        /// <param name="sender">The source of the event.</param>
        /// <param name="e">The <see cref="System.UnhandledExceptionEventArgs"/> instance containing the event data.</param>
        static void CurrentDomain_UnhandledException(object sender, UnhandledExceptionEventArgs e) {
            Log.Error(e.ExceptionObject.ToString());
        }


        /// <summary>
        /// Calculates the statistics.
        /// </summary>
        /// <param name="insertCount">The insert count.</param>
        /// <param name="benchmarks">The benchmarks.</param>
        private static void CalculateStatistics(int insertCount, IEnumerable<BenchmarkBase> benchmarks) {
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
            if (null != benchmarks) {
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
}
