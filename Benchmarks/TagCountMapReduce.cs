using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MongoDB.Bson;

namespace MongoBench.Benchmarks {

    /// <summary>
    /// Tag count benchmark
    /// </summary>
    public class TagCountMapReduce : BenchmarkBase {

        /// <summary>
        /// Initializes a new instance of the <see cref="TagCountMapReduce"/> class.
        /// </summary>
        public TagCountMapReduce() : this(false) { }

        /// <summary>
        /// Initializes a new instance of the <see cref="TagCountMapReduce"/> class.
        /// </summary>
        /// <param name="waitForMutex">if set to <c>true</c> [wait for mutex].</param>
        public TagCountMapReduce(bool waitForMutex) : base("Tag Count Map Reduce", waitForMutex) { }

        /// <summary>
        /// Benchmarks.
        /// </summary>
        public override void Benchmark() {
            var db = GetDatabase();
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
                            };"));
            var tags = db[result.CollectionName].FindAll().ToList();
        }
    }
}
