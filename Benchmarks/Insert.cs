using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MongoDB.Driver;

namespace MongoBench.Benchmarks {

    /// <summary>
    /// Insert Benchmark
    /// </summary>
    public class Insert : BenchmarkBase {

        /// <summary>
        /// Gets or sets the num of records.
        /// </summary>
        /// <value>The num of records.</value>
        public int NumOfRecords { get; private set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="Insert"/> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="databaseName">Name of the database.</param>
        /// <param name="collectionName">Name of the collection.</param>
        /// <param name="numOfRecords">The num of records.</param>
        /// <param name="waitForMutex">if set to <c>true</c> [wait for mutex].</param>
        /// <param name="mutextName">Name of the mutext.</param>
        public Insert(string connectionString, string databaseName, string collectionName, int numOfRecords, bool waitForMutex, string mutextName)
            : base("Insert", connectionString, databaseName, collectionName, waitForMutex, mutextName) {
                this.NumOfRecords = numOfRecords;
        }

        /// <summary>
        /// Insert benchmarks method
        /// </summary>
        public override void Benchmark() {
            var posts = GetDatabase()[Settings.COLLECTION_NAME];
            for (int i = 0; i < NumOfRecords; i++) {
                var result = posts.Insert(Data.GetDocument(i), Settings.INSERT_SAFE_MODE ? SafeMode.True : SafeMode.False);
            }
        }
    }
}