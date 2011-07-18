using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;
using System.Threading;

using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;
using MongoDB.Driver.Builders;
using System.Configuration;

namespace MongoBench {

    /// <summary>
    /// Benchmark
    /// </summary>
    public class Benchmark {

        private string MUTEXT_NAME = null;
        private string CONNECTION_STRING = null;
        private string DATABASE_NAME = null;
        private string COLLECTION_NAME = null;
        private int NUM_OF_RECORDS = 0;
        private SafeMode INSERT_SAFE_MODE = SafeMode.Create(bool.Parse(ConfigurationManager.AppSettings["INSERT_SAFE_MODE"]));
        private string[] INDEX_FIELDS = ConfigurationManager.AppSettings["INDEX_FIELDS"].Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries);

        /// <summary>
        /// Gets or sets the inserts.
        /// </summary>
        /// <value>The inserts.</value>
        public Stopwatch InsertStats;

        /// <summary>
        /// The simple query stats.
        /// </summary>
        public Stopwatch SimpleSelectStats;

        /// <summary>
        /// The simple filter stats.
        /// </summary>
        public Stopwatch SimpleFilterStats;

        /// <summary>
        /// The map reduce stats.
        /// </summary>
        public Stopwatch MapReduceTagStats;

        /// <summary>
        /// The create index stats.
        /// </summary>
        public Stopwatch CreateIndexStats;

        /// <summary>
        /// The indexed simple query stats.
        /// </summary>
        public Stopwatch IndexedSimpleSelectStats;

        /// <summary>
        /// The indexed simple filter stats.
        /// </summary>
        public Stopwatch IndexedSimpleFilterStats;

        /// <summary>
        /// The indexed map reduce stats.
        /// </summary>
        public Stopwatch IndexedMapReduceTagStats;

        /// <summary>
        /// The map reduce stats for comment authors
        /// </summary>
        public Stopwatch MapReduceCommentStats;
        
        /// <summary>
        /// Initializes a new instance of the <see cref="Benchmark"/> class.
        /// </summary>
        /// <param name="numOfRecordsToInsert">The num of records to insert.</param>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="mutexName">Name of the mutex.</param>
        /// <param name="databaseName">Name of the database.</param>
        /// <param name="collectionName">Name of the collection.</param>
        /// <param name="insertSafeMode">if set to <c>true</c> [insert safe mode].</param>
        /// <param name="indexFields">The index fields.</param>
        public Benchmark(int numOfRecordsToInsert, string connectionString, string mutexName, string databaseName, string collectionName, string[] indexFields, bool insertSafeMode) {
            this.NUM_OF_RECORDS = numOfRecordsToInsert;
            this.CONNECTION_STRING = connectionString;
            this.MUTEXT_NAME = mutexName;
            this.DATABASE_NAME = databaseName;
            this.COLLECTION_NAME = collectionName;
            this.INSERT_SAFE_MODE = SafeMode.Create(insertSafeMode);
            this.INDEX_FIELDS = indexFields;
        }

        /// <summary>
        /// Waits for signal.
        /// </summary>
        public void WaitForSignal() {
            if (!string.IsNullOrEmpty(MUTEXT_NAME)) {
                using (Mutex mutex = new Mutex(false, MUTEXT_NAME)) {
                    mutex.WaitOne();
                    mutex.ReleaseMutex();
                }
            }
            ExecuteBenchmarks();
        }

        /// <summary>
        /// Executes the and measure.
        /// </summary>
        /// <param name="method">The method.</param>
        /// <param name="stopwatch">The stopwatch.</param>
        public void ExecuteAndMeasure(Action method, ref Stopwatch stopwatch) {
            if (null == stopwatch)
                stopwatch = new Stopwatch();
            stopwatch.Start();
            method();
            stopwatch.Stop();
        }

        /// <summary>
        /// Executes the benchmarks.
        /// </summary>
        public void ExecuteBenchmarks() {
            //Insert Benchmarks
            ExecuteAndMeasure(InsertBenchmarks, ref this.InsertStats);

            //Simple Select
            ExecuteAndMeasure(SimpleSelect, ref this.SimpleSelectStats);

            //Simple Filter
            ExecuteAndMeasure(SimpleFilter, ref this.SimpleFilterStats);

            //Map Reduce
            ExecuteAndMeasure(MapReduceTags, ref this.MapReduceTagStats);

            //Map Reduce Comments
            ExecuteAndMeasure(MapReduceComments, ref this.MapReduceCommentStats);

            //Create Index
            ExecuteAndMeasure(CreateIndex, ref this.CreateIndexStats);

            //Simple Select
            ExecuteAndMeasure(SimpleSelect, ref this.IndexedSimpleSelectStats);

            //Simple Filter
            ExecuteAndMeasure(SimpleFilter, ref this.IndexedSimpleFilterStats);

            //Map Reduce
            ExecuteAndMeasure(MapReduceTags, ref this.IndexedMapReduceTagStats);
        }

        /// <summary>
        /// Gets the connection.
        /// </summary>
        /// <returns></returns>
        private MongoDatabase GetDatabase() {
            return MongoServer.Create(this.CONNECTION_STRING)[this.DATABASE_NAME];
        }

        /// <summary>
        /// Inserts benchmarks.
        /// </summary>
        public void InsertBenchmarks() {
            var posts = GetDatabase()[COLLECTION_NAME];
            for (int i = 0; i < this.NUM_OF_RECORDS; i++) {
                var result = posts.Insert(Data.GetDocument(i), INSERT_SAFE_MODE);
            }
        }

        /// <summary>
        /// Simples the select.
        /// </summary>
        public void SimpleSelect() {
            var post = GetDatabase()[COLLECTION_NAME].FindOne(Query.EQ("author", "Caryn"));
        }

        /// <summary>
        /// Simple filter query.
        /// </summary>
        public void SimpleFilter() {
            //Find posts where author names starts with 'a'
            var posts = GetDatabase()[COLLECTION_NAME]
                            .Find(Query.EQ("author", new BsonRegularExpression("A.*")))
                            .ToList();
        }

        /// <summary>
        /// Creates the index.
        /// </summary>
        public void CreateIndex() {
            var posts = GetDatabase()[COLLECTION_NAME];
            foreach (var f in INDEX_FIELDS)
                posts.CreateIndex(f);
        }

        /// <summary>
        /// Get tag count
        /// </summary>
        public void MapReduceTags() {
            var db = GetDatabase();
            var result = db[COLLECTION_NAME]
                        .MapReduce(new BsonJavaScript(
                            @"function() {
                                this.tags.forEach(
                                    function(t) {
                                        emit(t, { count : 1 });
                                    }
                                );
                            };"),
                            new BsonJavaScript(
                            @"function(key, values) {
                                var total = 0;
                                for(var i=0; i<values.length; i++) {
                                    total += values[i].count;
                                }
                                return { count : total };
                            };"));
            var tags = db[result.CollectionName].FindAll().ToList();
        }

        /// <summary>
        /// Get comment count by author
        /// </summary>
        public void MapReduceComments() {
            var db = GetDatabase();
            var result = db[COLLECTION_NAME]
                        .MapReduce(new BsonJavaScript(
                            @"function() {
                                this.comments.forEach(
                                    function(c) {
                                        emit(c.author, { count : 1 });
                                    }
                                );
                            };"),
                            new BsonJavaScript(
                            @"function(key, values) {
                                var total = 0;
                                for(var i=0; i<values.length; i++) {
                                    total += values[i].count;
                                }
                                return { count : total };
                            };"));
            var tags = db[result.CollectionName].FindAll().ToList();
        }
    }
}
