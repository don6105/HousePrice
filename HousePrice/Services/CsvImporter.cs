using MySqlConnector;
using System.Threading.Tasks.Dataflow;

namespace HousePrice.Services
{
    //匯入 MySQL 
    public class CsvImporter
    {
        private readonly string _connStr; //資料庫連線字串
        private ActionBlock<string> _myBlock;

        public CsvImporter()
        {
            this._connStr = $"Server={MySettings.DbHost};Port={MySettings.DbPort};" +
                            $"Database={MySettings.DbName};Username={MySettings.DbUser};Password={MySettings.DbPswd};" +
                            "Allow User Variables=true;AllowLoadLocalInfile=true;";

            truncate(); //debug
        }

        public void Run()
        {
            //定義ActionBlock：輸入CSV檔案路徑，解析並寫入資料庫
            _myBlock = new ActionBlock<string>(
                async csvfile => await CsvToDB(csvfile),
                new ExecutionDataflowBlockOptions
                {
                    MaxDegreeOfParallelism = 10,   // 限制同時只處理10筆
                    BoundedCapacity = 20          // 設定最大緩衝區為100
                }
            );
            
            //開始下載
            Downloader dl = new();
            dl.Run(_myBlock);
        }

        private async Task CsvToDB(string fileName)
        {
            using MySqlConnection conn = new MySqlConnection(_connStr);
            {
                //紀錄MySqlBulkCopy的異常訊息
                conn.InfoMessage += (s, e) => MysqlErrorMessage(e, fileName);

                //因MySQL.max_allowed_packet限制，分批寫入資料庫
                int rows = 0;
                bool isMapping = false;
                const int BATCH_SIZE = 2000;
                await conn.OpenAsync();
                MySqlBulkCopy bulkCopy = new MySqlBulkCopy(conn);
                foreach (var dt in MyCsvParser.CsvToDataTable(fileName, BATCH_SIZE))
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

                    MySqlBulkCopyResult result = await bulkCopy.WriteToServerAsync(dt); // dataTable複製到資料庫
                    rows += result.RowsInserted;
                }
                Console.WriteLine($" [{fileName} >> {bulkCopy.DestinationTableName}] {rows} rows inserted.");
            }
        }

        private static void MysqlErrorMessage(MySqlInfoMessageEventArgs e, string fileName)
        {
            foreach (var error in e.Errors)
            {
                if (error.Level != "Note")
                    Console.WriteLine($"[{error.Level}] <{fileName}> {error.Message}");
            }
        }

        private void truncate()
        {
            using (MySqlConnection conn = new MySqlConnection(_connStr))
            {
                conn.Open();
                string tables = "old_house,new_house,rent_house";
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
    }
}
