using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MongoDB.Driver.Builders;

namespace MongoBench.Benchmarks {

    /// <summary>
    /// Simple query benchmark
    /// </summary>
    public class SimpleQuery : BenchmarkBase {

        /// <summary>
        /// Initializes a new instance of the <see cref="SimpleQuery"/> class.
        /// </summary>
        public SimpleQuery() : this(false) { }

        /// <summary>
        /// Initializes a new instance of the <see cref="SimpleQuery"/> class.
        /// </summary>
        /// <param name="waitForMutex">if set to <c>true</c> [wait for mutex].</param>
        public SimpleQuery(bool waitForMutex) : base("Simple Query", waitForMutex) { }

        /// <summary>
        /// Simple query, get by author name
        /// </summary>
        public override void Benchmark() {
            var post = GetDatabase()[Settings.COLLECTION_NAME].FindOne(Query.EQ("author", "Caryn"));
        }
    }
}
