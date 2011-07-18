using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MongoDB.Driver.Builders;
using MongoDB.Bson;

namespace MongoBench.Benchmarks {
    /// <summary>
    /// Filter query benchmark
    /// </summary>
    public class FilterQuery : BenchmarkBase {

        /// <summary>
        /// Initializes a new instance of the <see cref="FilterQuery"/> class.
        /// </summary>
        public FilterQuery() : this(false) { }

        /// <summary>
        /// Initializes a new instance of the <see cref="FilterQuery"/> class.
        /// </summary>
        /// <param name="waitForMutex">if set to <c>true</c> [wait for mutex].</param>
        public FilterQuery(bool waitForMutex) : base("Filter Query", waitForMutex) { }

        /// <summary>
        /// Fetch posts where author name starts with 'A'
        /// </summary>
        public override void Benchmark() {
            var posts = GetDatabase()[Settings.COLLECTION_NAME]
                            .Find(Query.EQ("author", new BsonRegularExpression("A.*")))
                            .ToList();
        }
    }
}
