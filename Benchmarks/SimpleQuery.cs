﻿using System;
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
        /// <param name="connectionString">The connection string.</param>
        /// <param name="databaseName">Name of the database.</param>
        /// <param name="collectionName">Name of the collection.</param>
        /// <param name="waitForMutex">if set to <c>true</c> [wait for mutex].</param>
        /// <param name="mutextName">Name of the mutext.</param>
        public SimpleQuery(string connectionString, string databaseName, string collectionName, bool waitForMutex, string mutextName) 
            : base("Simple Query", connectionString, databaseName, collectionName, waitForMutex, mutextName) { }

        /// <summary>
        /// Simple query, get by author name
        /// </summary>
        public override void Benchmark() {
            var post = GetDatabase()[Settings.COLLECTION_NAME].FindOne(Query.EQ("author", "Caryn"));
        }
    }
}
