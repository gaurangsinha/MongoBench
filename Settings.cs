using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Configuration;
using System.Collections;

namespace MongoBench {
    /// <summary>
    /// Application Settings
    /// </summary>
    public static class Settings {

        private static List<KeyValuePair<string, string>> _CONNECTION_STRING = null;
        /// <summary>
        /// Gets the Connection String.
        /// </summary>
        /// <value>The Connection String.</value>
        public static List<KeyValuePair<string, string>> CONNECTION_STRING {
            get {
                if (null == _CONNECTION_STRING) {
                    _CONNECTION_STRING = new List<KeyValuePair<string, string>>();
                    for(int i=0 ; i<ConfigurationManager.ConnectionStrings.Count; i++) {
                        _CONNECTION_STRING.Add(new KeyValuePair<string, string>(
                            ConfigurationManager.ConnectionStrings[i].Name,
                            ConfigurationManager.ConnectionStrings[i].ConnectionString));
                    }
                }
                return _CONNECTION_STRING;
            }
        }

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
    }
}
