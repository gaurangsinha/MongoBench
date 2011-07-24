#define THREADS

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MongoBench.Benchmarks;
using System.Threading;
using System.Threading.Tasks;

namespace MongoBench {
    /// <summary>
    /// Benchmark Server
    /// </summary>
    public class BenchmarkServer {

        /// <summary>
        /// Gets or sets the name of the server.
        /// </summary>
        /// <value>The name of the server.</value>
        public string ServerName { get; set; }

        /// <summary>
        /// Gets or sets the connection string.
        /// </summary>
        /// <value>The connection string.</value>
        public string ConnectionString { get; set; }

        /// <summary>
        /// Gets or sets the num of threads.
        /// </summary>
        /// <value>The num of threads.</value>
        public int NumOfThreads { get; set; }

        /// <summary>
        /// Gets or sets the num of records.
        /// </summary>
        /// <value>The num of records.</value>
        public int NumOfRecords { get; set; }

        /// <summary>
        /// Gets or sets the name of the database.
        /// </summary>
        /// <value>The name of the database.</value>
        public string DatabaseName { get; set; }

        /// <summary>
        /// Gets or sets the name of the collection.
        /// </summary>
        /// <value>The name of the collection.</value>
        public string CollectionName { get; set; }

        /// <summary>
        /// Gets or sets the name of the mutex.
        /// </summary>
        /// <value>The name of the mutex.</value>
        public string MutexName { get; set; }

        /// <summary>
        /// Gets or sets the index fields.
        /// </summary>
        /// <value>The index fields.</value>
        public string[] IndexFields { get; set; }

        /// <summary>
        /// Gets or sets the results.
        /// </summary>
        /// <value>The results.</value>
        public BenchmarkBase[] Results { get; set; }

        /// <summary>
        /// Gets the avg inserts per thread second.
        /// </summary>
        /// <value>The avg inserts per thread second.</value>
        public double AvgInsertsPerThreadSecond {
            get {
                return this.Results
                            .Where(b => b.GetType() == typeof(Insert))
                            .Average(b => (double)this.NumOfRecords / (double)b.Time.ElapsedMilliseconds);
            }
        }

        /// <summary>
        /// Gets the avg inserts per second.
        /// </summary>
        /// <value>The avg inserts per second.</value>
        public double AvgInsertsPerSecond {
            get {
                return this.AvgInsertsPerThreadSecond * this.NumOfThreads;
            }
        }

        /// <summary>
        /// Gets the duration of the avg simple select.
        /// </summary>
        /// <value>The duration of the avg simple select.</value>
        public double AvgSimpleSelectDuration {
            get {
                return this.Results
                            .Where(b => b.GetType() == typeof(CompositeQueries))
                            .Average(b => ((CompositeQueries)b).SimpleQuery.Time.ElapsedMilliseconds);
            }
        }

        /// <summary>
        /// Gets the duration of the avg simple filter.
        /// </summary>
        /// <value>The duration of the avg simple filter.</value>
        public double AvgSimpleFilterDuration {
            get {
                return this.Results
                            .Where(b => b.GetType() == typeof(CompositeQueries))
                            .Average(b => ((CompositeQueries)b).FilterQuery.Time.ElapsedMilliseconds);
            }
        }

        /// <summary>
        /// Gets the duration of the avg map reduce tags.
        /// </summary>
        /// <value>The duration of the avg map reduce tags.</value>
        public double AvgMapReduceTagsDuration {
            get {
                return this.Results
                            .Where(b => b.GetType() == typeof(CompositeQueries))
                            .Average(b => ((CompositeQueries)b).TagCountMapReduce.Time.ElapsedMilliseconds);
            }
        }

        /// <summary>
        /// Gets the duration of the avg map reduce comments.
        /// </summary>
        /// <value>The duration of the avg map reduce comments.</value>
        public double AvgMapReduceCommentsDuration {
            get {
                return this.Results
                            .Where(b => b.GetType() == typeof(CompositeQueries))
                            .Average(b => ((CompositeQueries)b).CommentAuthorMapReduce.Time.ElapsedMilliseconds);
            }
        }

        /// <summary>
        /// Gets the duration of the create index.
        /// </summary>
        /// <value>The duration of the create index.</value>
        public double AvgCreateIndexDuration {
            get {
                return this.Results
                            .Where(b => b.GetType() == typeof(CreateIndex))
                            .Average(b => b.Time.ElapsedMilliseconds);
            }
        }

        /// <summary>
        /// Gets the avg duration of the indexed simple select.
        /// </summary>
        /// <value>The avg duration of the indexed simple select.</value>
        public double AvgIndexedSimpleSelectDuration {
            get {
                return this.Results
                            .Where(b => b.GetType() == typeof(CompositeQueries) && b.Type.StartsWith("Indexed"))
                            .Average(b => ((CompositeQueries)b).SimpleQuery.Time.ElapsedMilliseconds);
            }
        }


        /// <summary>
        /// Gets the avg duration of the indexed simple filter.
        /// </summary>
        /// <value>The avg duration of the indexed simple filter.</value>
        public double AvgIndexedSimpleFilterDuration {
            get {
                return this.Results
                            .Where(b => b.GetType() == typeof(CompositeQueries) && b.Type.StartsWith("Indexed"))
                            .Average(b => ((CompositeQueries)b).FilterQuery.Time.ElapsedMilliseconds);
            }
        }

        /// <summary>
        /// Gets the avg duration of the indexed map reduce tags.
        /// </summary>
        /// <value>The avg duration of the indexed map reduce tags.</value>
        public double AvgIndexedMapReduceTagsDuration {
            get {
                return this.Results
                            .Where(b => b.GetType() == typeof(CompositeQueries) && b.Type.StartsWith("Indexed"))
                            .Average(b => ((CompositeQueries)b).TagCountMapReduce.Time.ElapsedMilliseconds);
            }
        }

        /// <summary>
        /// Gets the avg duration of the indexed map reduce comments.
        /// </summary>
        /// <value>The avg duration of the indexed map reduce comments.</value>
        public double AvgIndexedMapReduceCommentsDuration {
            get {
                return this.Results
                            .Where(b => b.GetType() == typeof(CompositeQueries) && b.Type.StartsWith("Indexed"))
                            .Average(b => ((CompositeQueries)b).CommentAuthorMapReduce.Time.ElapsedMilliseconds);
            }
        }


        /// <summary>
        /// Initializes a new instance of the <see cref="BenchmarkServer"/> class.
        /// </summary>
        /// <param name="serverName">Name of the server.</param>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="numOfThreads">The num of threads.</param>
        /// <param name="numOfRecrods">The num of recrods.</param>
        /// <param name="databaseName">Name of the database.</param>
        /// <param name="collectionName">Name of the collection.</param>
        /// <param name="mutexName">Name of the mutex.</param>
        /// <param name="indexFields">The index fields.</param>
        public BenchmarkServer(string serverName, string connectionString, int numOfThreads, int numOfRecrods, string databaseName, string collectionName, string mutexName, string[] indexFields) {
            this.ServerName = serverName;
            this.ConnectionString = connectionString;
            this.NumOfThreads = numOfThreads;
            this.NumOfRecords = numOfRecrods;
            this.DatabaseName = databaseName;
            this.CollectionName = collectionName;
            this.MutexName = mutexName;
            this.IndexFields = indexFields;
            this.Results = new BenchmarkBase[(this.NumOfThreads * 3) + 1];
        }

        /// <summary>
        /// Runs benchmarks for the specified connection string.
        /// </summary>
        /// <returns></returns>
        public bool Run() {
            bool status = false;

            Log.Info("Started server [{0}] benchmark: threads:{1} records:{2}", this.ServerName, this.NumOfThreads, this.NumOfRecords);

            //Test connection string and database connection
            bool connectionStatus = TestDatabaseConnection(this.ConnectionString);

            if (connectionStatus) {
                Log.Info("Database connection to [{0}] successful", this.ServerName);

                //Clean database
                Log.Info("Cleaning database [{0}:{1}]", this.ServerName, this.DatabaseName);                
                bool cleanStatus = CleanDatabase(this.ConnectionString, this.DatabaseName, this.CollectionName);

                if (cleanStatus) {
                    Log.Info("Cleaning database [{0}:{1}] successful", this.ServerName, this.DatabaseName);

                    int index = 0;

                    //Insert benchmarks
                    BenchmarkBase[] results = ExecuteBenchmarks(this.NumOfThreads,
                                                typeof(Insert),
                                                this.ConnectionString,
                                                this.DatabaseName,
                                                this.CollectionName,
                                                this.NumOfThreads,
                                                true,
                                                this.MutexName);
                    Array.Copy(results, 0, this.Results, index, results.Length);

                    //Query benchmarks
                    results = ExecuteBenchmarks(this.NumOfThreads,
                                typeof(CompositeQueries),
                                this.ConnectionString,
                                this.DatabaseName,
                                this.CollectionName,
                                true,
                                this.MutexName);
                    index += results.Length;
                    Array.Copy(results, 0, this.Results, index, results.Length);

                    //Create Index
                    index += results.Length;
                    this.Results[index] = new CreateIndex(this.ConnectionString,
                                            this.DatabaseName,
                                            this.CollectionName,
                                            this.IndexFields,
                                            false,
                                            null);
                    this.Results[index].ExecuteBenchmark();

                    //Query benchmarks with indexes
                    results = ExecuteBenchmarks(this.NumOfThreads,
                                typeof(CompositeQueries),
                                "Indexed ",
                                this.ConnectionString,
                                this.DatabaseName,
                                this.CollectionName,
                                true,
                                this.MutexName);
                    index += 1;
                    Array.Copy(results, 0, this.Results, index, results.Length);

                    Log.Info("Completed benchmark for server [{0}]", this.ServerName);

                    status = true;
                }
                else {
                    Log.Error("Cleaning database [{0}:{1}] unsuccessful", this.ServerName, this.DatabaseName);
                }
            }
            else {
                Log.Error("Database connection to [{0}] unsuccessful", this.ServerName);
            }
            return status;
        }

        /// <summary>
        /// Tests the database connection.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <returns></returns>
        private static bool TestDatabaseConnection(string connectionString) {
            bool status = false;
            try {
                var db = MongoDB.Driver.MongoServer.Create(connectionString);
                db.Connect();
                status = MongoDB.Driver.MongoServerState.Connected == db.State;
                db.Disconnect();
            }
            catch (MongoDB.Driver.MongoException ex) {
                status = false;
                Log.Error("Cannot connect to {0} - {1}", connectionString, ex.Message);
            }
            return status;
        }

        /// <summary>
        /// Cleans the database.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="databaseName">Name of the database.</param>
        /// <param name="collectionName">Name of the collection.</param>
        /// <returns></returns>
        private static bool CleanDatabase(string connectionString, string databaseName, string collectionName) {
            bool status = false;
            try {
                var db = MongoDB.Driver.MongoServer.Create(connectionString)[databaseName];
                var posts = db[collectionName];
                var result = posts.RemoveAll();
                //db.DropCollection(collectionName);
                //db.Drop();
                status = (null != result) ? result.Ok : true;
            }
            catch (MongoDB.Driver.MongoException ex) {
                status = false;
                Log.Error("Cannot clean database/collection {0}/{1}/{2} - {3}", connectionString, databaseName, collectionName, ex.Message);
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
    }
}
