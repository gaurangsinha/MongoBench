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
        /// <param name="connectionString">The connection string.</param>
        /// <param name="databaseName">Name of the database.</param>
        /// <param name="collectionName">Name of the collection.</param>
        /// <param name="waitForMutex">if set to <c>true</c> [wait for mutex].</param>
        /// <param name="mutextName">Name of the mutext.</param>
        public CompositeQueries(string connectionString, string databaseName, string collectionName, bool waitForMutex, string mutextName)
            : this(string.Empty, connectionString, databaseName, collectionName, waitForMutex, mutextName) { }


        /// <summary>
        /// Initializes a new instance of the <see cref="CompositeQueries"/> class.
        /// </summary>
        /// <param name="name">The name.</param>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="databaseName">Name of the database.</param>
        /// <param name="collectionName">Name of the collection.</param>
        /// <param name="waitForMutex">if set to <c>true</c> [wait for mutex].</param>
        /// <param name="mutextName">Name of the mutext.</param>
        public CompositeQueries(string name, string connectionString, string databaseName, string collectionName, bool waitForMutex, string mutextName)
            : base(name + "Composite Queries", connectionString, databaseName, collectionName, waitForMutex, mutextName) {
                this.SimpleQuery = new SimpleQuery(connectionString, databaseName, collectionName, waitForMutex, mutextName);
                this.FilterQuery = new FilterQuery(connectionString, databaseName, collectionName, waitForMutex, mutextName);
                this.TagCountMapReduce = new TagCountMapReduce(connectionString, databaseName, collectionName, waitForMutex, mutextName);
                this.CommentAuthorMapReduce = new CommentAuthorMapReduce(connectionString, databaseName, collectionName, waitForMutex, mutextName);
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
