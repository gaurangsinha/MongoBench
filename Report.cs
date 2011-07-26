using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.IO;
using System.Web.UI.WebControls;
using System.Web.UI;
using MongoBench.Benchmarks;
using GoogleChartSharp;

namespace MongoBench {

    /// <summary>
    /// Report
    /// </summary>
    public class Report {       

        /// <summary>
        /// Output Filename
        /// </summary>
        public string OutputFileName { get; set; }

        /// <summary>
        /// Template Filename
        /// </summary>
        public string TemplateFileName { get; set; }

        /// <summary>
        /// Report data
        /// </summary>
        private StringBuilder ReportData;

        /// <summary>
        /// Initializes a new instance of the <see cref="Report"/> class.
        /// </summary>
        /// <param name="templateFileName">Name of the template file.</param>
        /// <param name="outputFileName">Name of the output file.</param>
        public Report(string templateFileName, string outputFileName) {
            this.TemplateFileName = templateFileName;
            this.OutputFileName = outputFileName;
        }

        /// <summary>
        /// Print Report
        /// </summary>
        /// <param name="benchmarks"></param>
        public void PrintReport(params MultiRunBenchmark[] benchmarks) {
            //Initialize output file
            this.ReportData = new StringBuilder().Append(File.ReadAllText(this.TemplateFileName));

            //Print Servers
            PrintServers(benchmarks);

            //Print Stats
            PrintStats(benchmarks);

            //Print Data
            PrintData(benchmarks);

            //Print Log
            PrintLog();

            //Write report data
            File.WriteAllText(this.OutputFileName, this.ReportData.ToString());
        }

        /// <summary>
        /// Prints the servers.
        /// </summary>
        /// <param name="benchmarks">The benchmarks.</param>
        private void PrintServers(params MultiRunBenchmark[] benchmarks) {
            StringBuilder str = new StringBuilder();
            str.Append("<table><tr><th>Name</th><th>Connection String</th></tr>");
            foreach (var b in benchmarks) {
                str.AppendFormat("<tr><td>{0}</td><td><code>{1}</code></td></tr>", 
                    b.ServerBenchmark[0].ServerName, 
                    b.ServerBenchmark[0].ConnectionString);
            }
            str.Append("</table><br/>");
            //Print benchmark options
            str.Append("<table><tr><th>Type</th><th>Value</th></tr>");
            str.AppendFormat("<tr><td>{0}</td><td>{1}</td></tr>", "Database Name", Settings.DATABASE_NAME);
            str.AppendFormat("<tr><td>{0}</td><td>{1}</td></tr>", "Collection Name", Settings.COLLECTION_NAME);
            str.AppendFormat("<tr><td>{0}</td><td>{1}</td></tr>", "Safe Mode", Settings.INSERT_SAFE_MODE);
            str.AppendFormat("<tr><td>{0}</td><td>{1}</td></tr>", "Index Fields", string.Join(",", Settings.INDEX_FIELDS));
            str.AppendFormat("<tr><td>{0}</td><td>{1}</td></tr>", "Number Of Runs", benchmarks[0].NumOfRuns);
            str.AppendFormat("<tr><td>{0}</td><td>{1}</td></tr>", "Number Of Threads", benchmarks[0].ServerBenchmark[0].NumOfThreads);
            str.AppendFormat("<tr><td>{0}</td><td>{1}</td></tr>", "Number Of Reecords/Thread", benchmarks[0].ServerBenchmark[0].NumOfRecords);
            str.AppendLine("</table>");
            this.ReportData.Replace("<%SERVERS%>", str.ToString());
        }

        /// <summary>
        /// Prints the stats.
        /// </summary>
        /// <param name="benchmarks">The benchmarks.</param>
        private void PrintStats(params MultiRunBenchmark[] benchmarks) {
            StringBuilder str = new StringBuilder();
            //Global Average
            str.Append("<table><tr><th></th><th>")
                .Append(string.Join("</th><th>", 
                    Array.ConvertAll<MultiRunBenchmark, string>(benchmarks,
                        b => b.ServerBenchmark[0].ServerName)))
                .Append("</th></tr>");

            //AvgInsertsPerThreadSecond
            str.Append("<tr><td>AvgInsertsPerThreadSecond</td><td>")
                .Append(string.Join("</td><td>", 
                    Array.ConvertAll<MultiRunBenchmark, string>(benchmarks,
                        b => b.AvgInsertsPerThreadSecond.ToString())))
                .Append("</td></tr>");

            //AvgInsertsPerSecond
            str.Append("<tr><td>AvgInsertsPerSecond</td><td>")
                .Append(string.Join("</td><td>", 
                    Array.ConvertAll<MultiRunBenchmark, string>(benchmarks,
                        b => b.AvgInsertsPerSecond.ToString())))
                .Append("</td></tr>");

            //AvgSimpleSelectDuration
            str.Append("<tr><td>AvgSimpleSelectDuration</td><td>")
                .Append(string.Join("</td><td>",
                    Array.ConvertAll<MultiRunBenchmark, string>(benchmarks,
                        b => b.AvgSimpleSelectDuration.ToString())))
                .Append("</td></tr>");
            
            //AvgSimpleFilterDuration
            str.Append("<tr><td>AvgSimpleFilterDuration</td><td>")
                .Append(string.Join("</td><td>",
                    Array.ConvertAll<MultiRunBenchmark, string>(benchmarks,
                        b => b.AvgSimpleFilterDuration.ToString())))
                .Append("</td></tr>");

            //AvgMapReduceTagsDuration
            str.Append("<tr><td>AvgMapReduceTagsDuration</td><td>")
                .Append(string.Join("</td><td>",
                    Array.ConvertAll<MultiRunBenchmark, string>(benchmarks,
                        b => b.AvgMapReduceTagsDuration.ToString())))
                .Append("</td></tr>");

            //AvgMapReduceCommentsDuration
            str.Append("<tr><td>AvgMapReduceCommentsDuration</td><td>")
                .Append(string.Join("</td><td>",
                    Array.ConvertAll<MultiRunBenchmark, string>(benchmarks,
                        b => b.AvgMapReduceCommentsDuration.ToString())))
                .Append("</td></tr>");

            //AvgCreateIndexDuration
            str.Append("<tr><td>AvgCreateIndexDuration</td><td>")
                .Append(string.Join("</td><td>",
                    Array.ConvertAll<MultiRunBenchmark, string>(benchmarks,
                        b => b.AvgCreateIndexDuration.ToString())))
                .Append("</td></tr>");

            //AvgIndexedSimpleSelectDuration
            str.Append("<tr><td>AvgIndexedSimpleSelectDuration</td><td>")
                .Append(string.Join("</td><td>",
                    Array.ConvertAll<MultiRunBenchmark, string>(benchmarks,
                        b => b.AvgIndexedSimpleSelectDuration.ToString() )))
                .Append("</td></tr>");

            //AvgIndexedSimpleFilterDuration
            str.Append("<tr><td>AvgIndexedSimpleFilterDuration</td><td>")
                .Append(string.Join("</td><td>",
                    Array.ConvertAll<MultiRunBenchmark, string>(benchmarks,
                        b => b.AvgIndexedSimpleFilterDuration.ToString())))
                .Append("</td></tr>");

            //AvgIndexedMapReduceTagsDuration
            str.Append("<tr><td>AvgIndexedMapReduceTagsDuration</td><td>")
                .Append(string.Join("</td><td>",
                    Array.ConvertAll<MultiRunBenchmark, string>(benchmarks,
                        b => b.AvgIndexedMapReduceTagsDuration.ToString())))
                .Append("</td></tr>");

            //AvgIndexedMapReduceCommentsDuration
            str.Append("<tr><td>AvgIndexedMapReduceCommentsDuration</td><td>")
                .Append(string.Join("</td><td>",
                    Array.ConvertAll<MultiRunBenchmark, string>(benchmarks,
                        b => b.AvgIndexedMapReduceCommentsDuration.ToString())))
                .Append("</td></tr>");

            str.Append("</table>");

            this.ReportData.Replace("<%STATS%>", str.ToString());
        }

        /// <summary>
        /// Prints the data.
        /// </summary>
        private void PrintData(params MultiRunBenchmark[] benchmarks) {
            DataTable data = new DataTable();
            data.Columns.Add("Server");
            data.Columns.Add("Run");
            data.Columns.Add("Type");
            data.Columns.Add("Time (seconds)");

            //Populate data table
            foreach (var m in benchmarks) {
                Dictionary<string, List<int[]>> graphData = new Dictionary<string, List<int[]>>();
                for (int i = 0; i < m.NumOfRuns; i++) {
                    for (int j = 0; j < m.ServerBenchmark[i].Results.Length; j++) {
                        BenchmarkBase b = m.ServerBenchmark[i].Results[j];
                        AppendDataTable(data, m.ServerBenchmark[i].ServerName, i + 1, b);
                    }
                }
            }

            //Insert Graph
            BarChart insertBarChart = new BarChart(320, 220, BarChartOrientation.Vertical, BarChartStyle.Grouped);
            insertBarChart.SetTitle("Inserts");
            insertBarChart.AddAxis(new ChartAxis(ChartAxisType.Bottom));
            var c = new ChartAxis(ChartAxisType.Left);
            c.SetRange(0, 60000);
            insertBarChart.AddAxis(c);
            insertBarChart.SetData(benchmarks[0].Results
                .Where(b => b.Type == "Insert")
                .ToList()
                .ConvertAll<float>(b => b.Time.ElapsedMilliseconds)
                .ToArray());

            //Print table
            using (GridView table = new GridView()) {
                table.CellPadding = 5;
                table.CellSpacing = 5;
                table.DataSource = data;
                table.DataBind();
                
                StringBuilder text = new StringBuilder();
                text.AppendFormat("<img src=\"{0}\" /><br/>", insertBarChart.GetUrl());
                using (StringWriter wr = new StringWriter(text))
                using (HtmlTextWriter htmlWr = new HtmlTextWriter(wr)) {
                    table.RenderControl(htmlWr);
                    this.ReportData.Replace("<%DATA%>", text.ToString());
                }
            }
        }

        /// <summary>
        /// Appends the data table.
        /// </summary>
        /// <param name="table">The table.</param>
        /// <param name="benchmark">The benchmark.</param>
        private void AppendDataTable(DataTable data, string serverName, int run, BenchmarkBase benchmark) {
            if (benchmark.GetType() == typeof(CompositeQueries)) {
                AppendDataTable(data, serverName, run, ((CompositeQueries)benchmark).SimpleQuery);
                AppendDataTable(data, serverName, run, ((CompositeQueries)benchmark).FilterQuery);
                AppendDataTable(data, serverName, run, ((CompositeQueries)benchmark).TagCountMapReduce);
                AppendDataTable(data, serverName, run, ((CompositeQueries)benchmark).CommentAuthorMapReduce);
            }
            else {
                DataRow row = data.NewRow();
                row["Server"] = serverName;
                row["Run"] = run;
                row["Type"] = benchmark.Type;
                row["Time (seconds)"] = benchmark.Time.ElapsedMilliseconds / 1000.0;
                data.Rows.Add(row);
            }
        }

        /// <summary>
        /// Prints the log.
        /// </summary>
        private void PrintLog() {
            this.ReportData.Replace("<%LOG%>", Log.LOG.Replace("\n", "<br/>").ToString());
        }
    }
}
