using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MongoDB.Bson;
using MongoDB.Driver.Builders;
using System.Threading;

namespace MongoBench.Benchmarks {

    /// <summary>
    /// Tag count benchmark
    /// </summary>
    public class TagCountMapReduce : BenchmarkBase {

        /// <summary>
        /// Initializes a new instance of the <see cref="TagCountMapReduce"/> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="databaseName">Name of the database.</param>
        /// <param name="collectionName">Name of the collection.</param>
        /// <param name="waitForMutex">if set to <c>true</c> [wait for mutex].</param>
        /// <param name="mutextName">Name of the mutext.</param>
        public TagCountMapReduce(string connectionString, string databaseName, string collectionName, bool waitForMutex, string mutextName)
            : base("Tag Count Map Reduce", connectionString, databaseName, collectionName, waitForMutex, mutextName) { }

        /// <summary>
        /// Benchmarks.
        /// </summary>
        public override void Benchmark() {
            var db = GetDatabase();
            var collectionName = "MongoBenchTemp" + Thread.CurrentThread.ManagedThreadId.ToString();
            var result = db[Settings.COLLECTION_NAME]
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
                            };"), 
                            MapReduceOptions.SetOutput(MapReduceOutput.Replace(collectionName)));
            var tags = db[collectionName].FindAll().ToList();
        }
    }
}
