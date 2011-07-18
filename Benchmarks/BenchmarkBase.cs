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
        /// Gets or sets the type.
        /// </summary>
        /// <value>The type.</value>
        public string Type { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether [wait foir mutex].
        /// </summary>
        /// <value><c>true</c> if [wait foir mutex]; otherwise, <c>false</c>.</value>
        public bool WaitForMutex { get; set; }

        /// <summary>
        /// Gets or sets the time.
        /// </summary>
        /// <value>The time.</value>
        public Stopwatch Time { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="BenchmarkBase"/> class.
        /// </summary>
        /// <param name="type">The type.</param>
        protected BenchmarkBase(string type) : this(type, true) { }

        /// <summary>
        /// Initializes a new instance of the <see cref="BenchmarkBase"/> class.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <param name="waitForMutex">if set to <c>true</c> [wait for mutex].</param>
        protected BenchmarkBase(string type, bool waitForMutex) {
            this.Type = type;
            this.WaitForMutex = waitForMutex;
        }

        /// <summary>
        /// Executes the benchmark.
        /// </summary>
        public virtual void ExecuteBenchmark() {
            //Create Stopwatch instance if required
            if (null == Time)
                Time = new Stopwatch();

            //Wait for signal
            if (WaitForMutex && !string.IsNullOrEmpty(Settings.MUTEX_NAME)) {
                using (Mutex mutex = new Mutex(false, Settings.MUTEX_NAME)) {
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
            return MongoServer.Create(Settings.CONNECTION_STRING)[Settings.DATABASE_NAME];
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
            return string.Format("{0:0.0000}\t{1}", 
                (null != Time) ? Time.ElapsedMilliseconds / 1000.0 : -1,
                this.Type);
        }
    }
}
