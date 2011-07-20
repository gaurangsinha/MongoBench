using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;
using MongoDB.Driver;
using System.Threading;

namespace MongoBench.Benchmarks {

    /// <summary>
    /// Base class for Benchmark
    /// </summary>
    public abstract class BenchmarkBase {

        /// <summary>
        /// Gets or sets the connection string.
        /// </summary>
        /// <value>The connection string.</value>
        public string ConnectionString { get; private set; }

        /// <summary>
        /// Gets or sets the name of the database.
        /// </summary>
        /// <value>The name of the database.</value>
        public string DatabaseName { get; private set; }

        /// <summary>
        /// Gets or sets the name of the collection.
        /// </summary>
        /// <value>The name of the collection.</value>
        public string CollectionName { get; private set; }

        /// <summary>
        /// Gets or sets the type.
        /// </summary>
        /// <value>The type.</value>
        public string Type { get; private set; }

        /// <summary>
        /// Gets or sets a value indicating whether [wait foir mutex].
        /// </summary>
        /// <value><c>true</c> if [wait foir mutex]; otherwise, <c>false</c>.</value>
        public bool WaitForMutex { get; private set; }

        /// <summary>
        /// Gets or sets the name of the mutext.
        /// </summary>
        /// <value>The name of the mutext.</value>
        public string MutextName { get; private set; }

        /// <summary>
        /// Gets or sets the time.
        /// </summary>
        /// <value>The time.</value>
        public Stopwatch Time { get; private set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="BenchmarkBase"/> class.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="databaseName">Name of the database.</param>
        /// <param name="collectionName">Name of the collection.</param>
        /// <param name="waitForMutex">if set to <c>true</c> [wait for mutex].</param>
        /// <param name="mutextName">Name of the mutext.</param>
        protected BenchmarkBase(string type, string connectionString, string databaseName, string collectionName, bool waitForMutex, string mutextName) {
            this.Type = type;
            this.ConnectionString = connectionString;
            this.DatabaseName = databaseName;
            this.CollectionName = collectionName;
            this.WaitForMutex = waitForMutex;
            this.MutextName = mutextName;
        }

        /// <summary>
        /// Executes the benchmark.
        /// </summary>
        public virtual void ExecuteBenchmark() {
            //Create Stopwatch instance if required
            if (null == Time)
                Time = new Stopwatch();

            //Wait for signal
            if (WaitForMutex && !string.IsNullOrEmpty(MutextName)) {
                using (Mutex mutex = new Mutex(false, MutextName)) {
                    mutex.WaitOne();
                    mutex.ReleaseMutex();
                }
            }

            Time.Start();
            Benchmark();
            Time.Stop();
        }
        
        /// <summary>
        /// Gets the connection.
        /// </summary>
        /// <returns></returns>
        protected virtual MongoDatabase GetDatabase() {
            return MongoServer.Create(this.ConnectionString)[this.DatabaseName];
        }

        /// <summary>
        /// Benchmarks.
        /// </summary>
        public abstract void Benchmark();

        /// <summary>
        /// Returns a <see cref="System.String"/> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="System.String"/> that represents this instance.
        /// </returns>
        public override string ToString() {
            return string.Format("{0:0.0000} - {1}", 
                (null != Time) ? Time.ElapsedMilliseconds / 1000.0 : -1,
                this.Type);
        }
    }
}
