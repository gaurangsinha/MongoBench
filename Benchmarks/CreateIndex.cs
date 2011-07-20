using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MongoBench.Benchmarks {

    /// <summary>
    /// Create Index benchmark
    /// </summary>
    public class CreateIndex : BenchmarkBase {

        /// <summary>
        /// Gets or sets the index fields.
        /// </summary>
        /// <value>The index fields.</value>
        public string[] IndexFields { get; private set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="CreateIndex"/> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="databaseName">Name of the database.</param>
        /// <param name="collectionName">Name of the collection.</param>
        /// <param name="indexFields">The index fields.</param>
        /// <param name="waitForMutex">if set to <c>true</c> [wait for mutex].</param>
        /// <param name="mutextName">Name of the mutext.</param>
        public CreateIndex(string connectionString, string databaseName, string collectionName, string[] indexFields, bool waitForMutex, string mutextName)
            : base("Created Indexes", connectionString, databaseName, collectionName, waitForMutex, mutextName) {
                this.IndexFields = indexFields;
        }

        /// <summary>
        /// Create index on fields specified
        /// </summary>
        public override void Benchmark() {
            var posts = GetDatabase()[Settings.COLLECTION_NAME];
            foreach (var f in Settings.INDEX_FIELDS)
                posts.CreateIndex(f);            
        }
    }
}
