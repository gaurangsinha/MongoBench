using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration;

namespace MongoBench {
    /// <summary>
    /// Application Settings
    /// </summary>
    public static class Settings {
        /// <summary>
        /// Database connection string
        /// </summary>
        public static string CONNECTION_STRING = ConfigurationManager.ConnectionStrings["MongoConnectionString"].ConnectionString;

        /// <summary>
        /// Mutext name to synchronize threads
        /// </summary>
        public static string MUTEX_NAME = ConfigurationManager.AppSettings["MUTEX_NAME"];

        /// <summary>
        /// Database name on which the benchmarks will be performed
        /// </summary>
        public static string DATABASE_NAME = ConfigurationManager.AppSettings["DATABASE_NAME"];

        /// <summary>
        /// Name of the collection on which records will be inserted and queries executed
        /// </summary>
        public static string COLLECTION_NAME = ConfigurationManager.AppSettings["COLLECTION_NAME"];

        /// <summary>
        /// Specifies if inserts are to be done in safe mode
        /// </summary>
        public static bool INSERT_SAFE_MODE = bool.Parse(ConfigurationManager.AppSettings["INSERT_SAFE_MODE"]);

        /// <summary>
        /// The fields on which indexes are supposed to be created
        /// </summary>
        public static string[] INDEX_FIELDS = ConfigurationManager.AppSettings["INDEX_FIELDS"].Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries);

        /// <summary>
        /// The number of records to be inserted per thread
        /// </summary>
        public static int NUM_OF_RECORDS = 1000;
    }
}
