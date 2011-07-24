using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.IO;
using System.Web.UI.WebControls;
using System.Web.UI;
using MongoBench.Benchmarks;

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

            //Print Log
            PrintLog();

            //Print Data
            PrintData(benchmarks);

            //Write report data
            File.WriteAllText(this.OutputFileName, this.ReportData.ToString());
        }

        /// <summary>
        /// Prints the servers.
        /// </summary>
        /// <param name="benchmarks">The benchmarks.</param>
        private void PrintServers(params MultiRunBenchmark[] benchmarks) {
            StringBuilder str = new StringBuilder();
            str.Append("<table><thead><td>Name</td><td>Connection String</td></thread>");
            foreach (var b in benchmarks) {
                str.AppendFormat("<tr><td>{0}</td><td><code>{1}</code></td></tr>", 
                    b.ServerBenchmark[0].ServerName, 
                    b.ServerBenchmark[0].ConnectionString);
            }
            str.Append("</table>");
            this.ReportData.Replace("<%SERVERS%>", str.ToString());
        }

        /// <summary>
        /// Prints the data.
        /// </summary>
        private void PrintData(params MultiRunBenchmark[] benchmarks) {
            DataTable data = new DataTable();
            data.Columns.Add("Server");
            data.Columns.Add("Run");
            data.Columns.Add("Type");
            data.Columns.Add("Time");

            //Populate data table
            foreach (var m in benchmarks) {
                for (int i = 0; i < m.NumOfRuns; i++) {                    
                    foreach (var r in m.ServerBenchmark[i].Results) {
                        AppendDataTable(data, m.ServerBenchmark[i].ServerName, i + 1, r);
                    }
                }
            }

            //Print table
            using (GridView table = new GridView()) {
                table.CellPadding = 5;
                table.CellSpacing = 5;
                table.DataSource = data;
                table.DataBind();
                
                StringBuilder text = new StringBuilder();
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
                row["Time"] = benchmark.Time.ElapsedMilliseconds / 1000.0;
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
