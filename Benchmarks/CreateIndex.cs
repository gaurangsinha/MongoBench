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
        /// Initializes a new instance of the <see cref="CreateIndex"/> class.
        /// </summary>
        public CreateIndex() : base("Created Indexes", false) { }

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
