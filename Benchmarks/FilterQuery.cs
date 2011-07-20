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
        /// <param name="connectionString">The connection string.</param>
        /// <param name="databaseName">Name of the database.</param>
        /// <param name="collectionName">Name of the collection.</param>
        /// <param name="waitForMutex">if set to <c>true</c> [wait for mutex].</param>
        /// <param name="mutextName">Name of the mutext.</param>
        public FilterQuery(string connectionString, string databaseName, string collectionName, bool waitForMutex, string mutextName)
            : base("Filter Query", connectionString, databaseName, collectionName, waitForMutex, mutextName) { }

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
