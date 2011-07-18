using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MongoBench.Benchmarks {

    /// <summary>
    /// Composite Queries - Simple, Filter, Tags & Comments
    /// </summary>
    public class CompositeQueries : BenchmarkBase {

        /// <summary>
        /// Gets or sets the simple query.
        /// </summary>
        /// <value>The simple query.</value>
        public SimpleQuery SimpleQuery;

        /// <summary>
        /// Gets or sets the filter query.
        /// </summary>
        /// <value>The filter query.</value>
        public FilterQuery FilterQuery;

        /// <summary>
        /// Gets or sets the tag count map reduce.
        /// </summary>
        /// <value>The tag count map reduce.</value>
        public TagCountMapReduce TagCountMapReduce;

        /// <summary>
        /// Gets or sets the comment author map reduce.
        /// </summary>
        /// <value>The comment author map reduce.</value>
        public CommentAuthorMapReduce CommentAuthorMapReduce;

        /// <summary>
        /// Initializes a new instance of the <see cref="CompositeQueries"/> class.
        /// </summary>
        /// <param name="name">The name.</param>
        public CompositeQueries(string name) : this(name, false) { }

        /// <summary>
        /// Initializes a new instance of the <see cref="CompositeQueries"/> class.
        /// </summary>
        /// <param name="name">The name.</param>
        /// <param name="waitForMutex">if set to <c>true</c> [wait for mutex].</param>
        public CompositeQueries(string name, bool waitForMutex) : base(name + "Composite Queries", waitForMutex) {
            this.SimpleQuery = new SimpleQuery(waitForMutex);
            this.FilterQuery = new FilterQuery(waitForMutex);
            this.TagCountMapReduce = new TagCountMapReduce(waitForMutex);
            this.CommentAuthorMapReduce = new CommentAuthorMapReduce(waitForMutex);
        }

        /// <summary>
        /// Run Benchmarks
        /// </summary>
        public override void Benchmark() {
            SimpleQuery.ExecuteBenchmark();
            FilterQuery.ExecuteBenchmark();
            TagCountMapReduce.ExecuteBenchmark();
            CommentAuthorMapReduce.ExecuteBenchmark();
        }

        /// <summary>
        /// Returns a <see cref="System.String"/> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="System.String"/> that represents this instance.
        /// </returns>
        public override string ToString() {
            return string.Format("{0}\n\t\t{1}\n\t\t{2}\n\t\t{3}",
                SimpleQuery.ToString(),
                FilterQuery.ToString(),
                TagCountMapReduce.ToString(),
                CommentAuthorMapReduce.ToString());
        }
    }
}
