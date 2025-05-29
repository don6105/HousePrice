using MySqlConnector;
using Newtonsoft.Json.Linq;
using System.Collections.Concurrent;
using System.Data;
using System.Runtime.Intrinsics.X86;
using System.Text.RegularExpressions;
using System.Threading.Tasks.Dataflow;

namespace HousePrice.Services
{
    //匯入 MySQL 
    public class DataImporter : IDisposable
    {
        private const int MAX_BLOCK_NUM = 10;
        private const int MAX_BLOCK_CAPACITY = 100;
        private const int MAX_RETRY_QUEUE = 3;
        private const int BATCH_SIZE = 2000; //寫入MySQL的筆數上限
        private readonly string _connStr; //資料庫連線字串
        private bool _disposed = false; // 避免重複 Dispose
        private ActionBlock<string> _myBlock;
        private ConcurrentQueue<MySqlConnection> _connPool;
        private static DataTable _batchHistory = null;

        public DataImporter()
        {
            _connStr = $"Server={MySettings.DbHost};Port={MySettings.DbPort};" +
                       $"Database={MySettings.DbName};Username={MySettings.DbUser};Password={MySettings.DbPswd};" +
                       "Allow User Variables=true;AllowLoadLocalInfile=true;";

            //初始化「連接池」
            _connPool = new ConcurrentQueue<MySqlConnection>(); //init queue
            for (int i = 0; i < MAX_BLOCK_NUM; i++)
            {
                MySqlConnection conn = new MySqlConnection(_connStr);
                conn.Open();
                _connPool.Enqueue(conn);
            }

            //定義ActionBlock：輸入CSV檔案路徑，解析並寫入資料庫
            _myBlock = new ActionBlock<string>(
                async (csvfile) => {
                    //從「連接池」取得一個連接(connection re-use)
                    MySqlConnection conn = null;
                    for (int i = 1; i <= MAX_RETRY_QUEUE; i++)
                    {
                        if (_connPool.TryDequeue(out conn)) break;
                        if (i == MAX_RETRY_QUEUE) throw new InvalidOperationException("無法從連線池取得連線");
                    }
                    //未曾匯入過資料庫
                    if (!IsDataExistDb(csvfile)) await CsvToDbAsync(conn, csvfile);
                    //丟回去給「連接池」
                    _connPool.Enqueue(conn);
                },
                new ExecutionDataflowBlockOptions
                {
                    MaxDegreeOfParallelism = MAX_BLOCK_NUM,   // 限制同時只處理 N 筆
                    BoundedCapacity = MAX_BLOCK_CAPACITY      // 設定最大緩衝區為 M
                }
            );

            Truncate(); //debug
        }

        public void Dispose() //釋放資源
        {
            if (_disposed) return; //避免重複執行

            while (_connPool.TryDequeue(out var conn))
            {
                try
                {
                    conn?.Close();
                    conn?.Dispose();
                }
                catch { }
            }
            Console.WriteLine("<MySQL連接池>已安全釋放");
            GC.SuppressFinalize(this); // 告訴 GC 不用再呼叫 finalizer
        }

        ~DataImporter()
        {
            Dispose();
        }

        public void Run()
        {
            //開始下載
            FileDownloader dl = new();
            List<Task> pendingTasks = dl.GenerateDownloadTasks(_myBlock);
            Task.WaitAll(pendingTasks);
            _myBlock.Complete(); //告訴block不會再送了
            _myBlock.Completion.Wait(); //等待ActionBlock完成
        }

        private async Task CsvToDbAsync(MySqlConnection conn, string fileName)
        {
            //紀錄MySqlBulkCopy的異常訊息
            conn.InfoMessage += (s, e) => MysqlErrorMessage(e, fileName);

            //因MySQL.max_allowed_packet限制，分批寫入資料庫
            int rows = 0;
            bool isMapping = false;
            MySqlBulkCopy bulkCopy = new MySqlBulkCopy(conn);
            foreach (DataTable dt in MyCsvParser.CsvToDataTable(fileName, BATCH_SIZE))
            {
                //建立 Column Mappings
                if (!isMapping)
                {
                    List<MySqlBulkCopyColumnMapping> colMappings = new List<MySqlBulkCopyColumnMapping>();
                    for (int i = 0; i < dt.Columns.Count; i++)
                    {
                        colMappings.Add(new MySqlBulkCopyColumnMapping(i, dt.Columns[i].ColumnName));
                    }
                    bulkCopy.DestinationTableName = dt.TableName; //目標Table名稱
                    bulkCopy.ColumnMappings.AddRange(colMappings);
                    isMapping = true;
                }
                // dataTable複製到資料庫
                MySqlBulkCopyResult result = await bulkCopy.WriteToServerAsync(dt);
                rows += result.RowsInserted;
            }
            InsertBatchHistory(conn, fileName, rows);
            Console.WriteLine($" [{fileName} >> {bulkCopy.DestinationTableName}] {rows} rows inserted.");
        }

        private static void MysqlErrorMessage(MySqlInfoMessageEventArgs e, string fileName)
        {
            foreach (var error in e.Errors)
            {
                if (error.Level != "Note")
                    Console.WriteLine($"[{error.Level}] <{fileName}> {error.Message}");
            }
        }

        private void Truncate()
        {
            using (MySqlConnection conn = new MySqlConnection(_connStr))
            {
                conn.Open();
                string tables = "batch_history,old_house,new_house";
                foreach (string t in tables.Split(","))
                {
                    using (MySqlCommand mc = new MySqlCommand($"truncate table {t}", conn))
                    {
                        try { mc.ExecuteNonQuery(); }
                        catch (Exception ex) { Console.WriteLine(ex.Message); }
                    }
                }
            }
        }

        private bool IsDataExistDb(string filePath)
        {
            if (_batchHistory == null)
            {
                string sql = "SELECT csv_path FROM batch_history order by 1";
                using (DataTable dt = new DataTable())
                using (MySqlConnection conn = new MySqlConnection(_connStr))
                using (MySqlDataAdapter da = new MySqlDataAdapter(sql, conn))
                {
                    MySqlCommandBuilder bd = new MySqlCommandBuilder(da);
                    da.Fill(dt);
                    _batchHistory = dt;
                }
            }
            return _batchHistory.AsEnumerable()
                                .Any(x => x.Field<string>("csv_path") == filePath);
        }

        private static void InsertBatchHistory(MySqlConnection conn, string fileName, int rowCount)
        {
            string yearAndSeason = null;
            Match m = Regex.Match(fileName, @"[0-9]{3}S[1-4]", RegexOptions.IgnoreCase);
            if (m.Success) { yearAndSeason = m.Value.ToString(); }

            string sql = "INSERT INTO batch_history(year_season, csv_path, row_count) VALUES (@v1, @v2, @v3)";
            using (MySqlCommand mc = new MySqlCommand(sql, conn))
            {
                try {
                    mc.Parameters.AddWithValue("@v1", yearAndSeason);
                    mc.Parameters.AddWithValue("@v2", fileName);
                    mc.Parameters.AddWithValue("@v3", rowCount);
                    mc.ExecuteNonQuery();
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
            }
        }
    }
}
