using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MongoBench {

    /// <summary>
    /// Log Level
    /// </summary>
    public enum LogLevel {
        /// <summary>
        /// 
        /// </summary>
        INFO,
        
        /// <summary>
        /// 
        /// </summary>
        ERROR
    }

    /// <summary>
    /// Log
    /// </summary>
    public static class Log {

        /// <summary>
        /// Logs
        /// </summary>
        private static StringBuilder LOG = new StringBuilder();
        
        /// <summary>
        /// Log info
        /// </summary>
        /// <param name="format"></param>
        /// <param name="objects"></param>
        public static void Info(string format, params object[] objects) {
            LogEntry(LogLevel.INFO, format, objects);
        }

        /// <summary>
        /// Log error
        /// </summary>
        /// <param name="format">The format.</param>
        /// <param name="objects">The objects.</param>
        public static void Error(string format, params object[] objects) {
            LogEntry(LogLevel.ERROR, format, objects);
        }

        /// <summary>
        /// Logs the specified level.
        /// </summary>
        /// <param name="level">The level.</param>
        /// <param name="format">The format.</param>
        /// <param name="objects">The objects.</param>
        public static void LogEntry(LogLevel level, string format, params object[] objects) {
            lock (LOG) {
                LOG.AppendFormat("{0:HH:mm:ss.ffff} [{1}] - ", DateTime.Now, level.ToString())
                    .AppendFormat(format, objects)
                    .AppendLine();
            }
            Console.Write("[{0}] ", level.ToString());
            Console.WriteLine(format, objects);
        }
    }
}
