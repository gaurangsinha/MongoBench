using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MongoBench.Benchmarks;

namespace MongoBench {

    /// <summary>
    /// 
    /// </summary>
    public class MultiRunBenchmark {

        /// <summary>
        /// Gets or sets the num of runs.
        /// </summary>
        /// <value>The num of runs.</value>
        public int NumOfRuns { get; private set; }

        /// <summary>
        /// Gets the benchmarks.
        /// </summary>
        /// <value>The benchmarks.</value>
        public BenchmarkServer[] ServerBenchmark { get; private set; }

        private List<BenchmarkBase> _Results;
        /// <summary>
        /// Gets the results.
        /// </summary>
        /// <value>The results.</value>
        public List<BenchmarkBase> Results {
            get {
                if (null == _Results) {
                    _Results = new List<BenchmarkBase>();
                    foreach (var b in this.ServerBenchmark) {
                        _Results.AddRange(b.Results);
                    }
                }
                return _Results;
            }
        }

        #region Statistics
        /// <summary>
        /// Gets the avg inserts per thread second.
        /// </summary>
        /// <value>The avg inserts per thread second.</value>
        public double AvgInsertsPerThreadSecond {
            get {
                return this.Results
                            .Where(b => null != b && b.GetType() == typeof(Insert))
                            .Average(b => (double)((Insert)b).NumOfRecords / ((double)b.Time.ElapsedMilliseconds / 1000.0));
            }
        }

        /// <summary>
        /// Gets the avg inserts per second.
        /// </summary>
        /// <value>The avg inserts per second.</value>
        public double AvgInsertsPerSecond {
            get {
                return this.AvgInsertsPerThreadSecond * this.ServerBenchmark[0].NumOfThreads;
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
                            .Average(b => ((CompositeQueries)b).SimpleQuery.Time.ElapsedMilliseconds) / 1000.0;
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
                            .Average(b => ((CompositeQueries)b).FilterQuery.Time.ElapsedMilliseconds) / 1000.0;
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
                            .Average(b => ((CompositeQueries)b).TagCountMapReduce.Time.ElapsedMilliseconds) / 1000.0;
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
                            .Average(b => ((CompositeQueries)b).CommentAuthorMapReduce.Time.ElapsedMilliseconds) / 1000.0;
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
                            .Average(b => b.Time.ElapsedMilliseconds) / 1000.0;
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
                            .Average(b => ((CompositeQueries)b).SimpleQuery.Time.ElapsedMilliseconds) / 1000.0;
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
                            .Average(b => ((CompositeQueries)b).FilterQuery.Time.ElapsedMilliseconds) / 1000.0;
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
                            .Average(b => ((CompositeQueries)b).TagCountMapReduce.Time.ElapsedMilliseconds) / 1000.0;
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
                            .Average(b => ((CompositeQueries)b).CommentAuthorMapReduce.Time.ElapsedMilliseconds) / 1000.0;
            }
        }
        #endregion

        /// <summary>
        /// Initializes a new instance of the <see cref="MultiRunBenchmark"/> class.
        /// </summary>
        /// <param name="numOfRuns">The num of runs.</param>
        /// <param name="serverName">Name of the server.</param>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="numOfThreads">The num of threads.</param>
        /// <param name="numOfRecrods">The num of recrods.</param>
        /// <param name="databaseName">Name of the database.</param>
        /// <param name="collectionName">Name of the collection.</param>
        /// <param name="mutexName">Name of the mutex.</param>
        /// <param name="indexFields">The index fields.</param>
        public MultiRunBenchmark(int numOfRuns, string serverName, string connectionString, int numOfThreads, int numOfRecrods, string databaseName, string collectionName, string mutexName, string[] indexFields) {
            this.NumOfRuns = numOfRuns;
            this.ServerBenchmark = new BenchmarkServer[numOfRuns];
            for (int i = 0; i < numOfRuns; i++) {
                this.ServerBenchmark[i] = new BenchmarkServer(serverName,
                                                connectionString,
                                                numOfThreads,
                                                numOfRecrods,
                                                databaseName,
                                                collectionName,
                                                mutexName,
                                                indexFields);
            }
        }

        /// <summary>
        /// Runs this instance.
        /// </summary>
        /// <returns></returns>
        public bool Run() {
            bool status = true;
            Log.Info("Starting multi-run [{0}] benchmarks", this.NumOfRuns);
            for (int i = 0; i < this.NumOfRuns; i++) {
                status &= this.ServerBenchmark[i].Run();
            }
            Log.Info("Completed multi-run benchmarks");
            return status;
        }
    }
}
