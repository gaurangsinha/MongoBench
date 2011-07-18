using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MongoDB.Bson;
using MongoDB.Driver.Builders;

namespace MongoBench.Benchmarks {

    /// <summary>
    /// Comment author map reduce
    /// </summary>
    public class CommentAuthorMapReduce : BenchmarkBase {

        /// <summary>
        /// Initializes a new instance of the <see cref="CommentAuthorMapReduce"/> class.
        /// </summary>
        public CommentAuthorMapReduce() : this(false) { }

        /// <summary>
        /// Initializes a new instance of the <see cref="CommentAuthorMapReduce"/> class.
        /// </summary>
        /// <param name="waitForMutex">if set to <c>true</c> [wait for mutex].</param>
        public CommentAuthorMapReduce(bool waitForMutex) : base("Comment Map Reduce", waitForMutex) { }

        /// <summary>
        /// Find the count of comments by author.
        /// </summary>
        public override void Benchmark() {
            var db = GetDatabase();
            var collectionName = "MongoBenchTemp";
            var result = db[Settings.COLLECTION_NAME]
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
                            };"),
                            MapReduceOptions.SetOutput(MapReduceOutput.Replace(collectionName)));
            var tags = db[collectionName].FindAll().ToList();
        }
    }
}
