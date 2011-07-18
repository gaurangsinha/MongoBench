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
        /// Initializes a new instance of the <see cref="Insert"/> class.
        /// </summary>
        public Insert() : this(true) { }

        /// <summary>
        /// Initializes a new instance of the <see cref="Insert"/> class.
        /// </summary>
        /// <param name="waitForMutex">if set to <c>true</c> [wait for mutex].</param>
        public Insert(bool waitForMutex) : base("Insert", waitForMutex) { }

        /// <summary>
        /// Insert benchmarks method
        /// </summary>
        public override void Benchmark() {
            var posts = GetDatabase()[Settings.COLLECTION_NAME];
            for (int i = 0; i < Settings.NUM_OF_RECORDS; i++) {
                var result = posts.Insert(Data.GetDocument(i), Settings.INSERT_SAFE_MODE ? SafeMode.True : SafeMode.False);
            }
        }
    }
}
