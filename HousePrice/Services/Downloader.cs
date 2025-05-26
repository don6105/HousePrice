using System.Data;
using System.Globalization;
using System.Text.RegularExpressions;
using System.Threading.Tasks.Dataflow;
using CsvHelper;
using HousePrice.Services;
using MySqlConnector;
using Newtonsoft.Json;

namespace HousePrice
{
    public class Downloader
    {
        //[政府實價登錄網站] https://plvr.land.moi.gov.tw/DownloadOpenData
        private MySettings mySettings; //讀取appsettings.json的設定資料
        private HttpClient httpClient = new();
        private const string downloadUrl = "https://plvr.land.moi.gov.tw//DownloadSeason?type=zip&fileName=lvr_landcsv.zip";
        private const string csvPattern = @"(?>^|[\\\/])([a-z])_lvr_land_([a-b])(.csv?)$";
        //[縣市代號]_lvr_land_[交易類型]，交易類型：a-房屋買賣交易, b-新成屋交易, c-租房交易
        private readonly string connString; //資料庫連線字串
        private string cityJsonStr; //緩存用的「縣市代號-縣市名稱」對照資料

        public Downloader(MySettings ms)
        {
            this.mySettings = ms;
            this.connString = $"Server={mySettings.DbHost};Port={mySettings.DbPort};" +
                              $"Database={mySettings.DbName};Username={mySettings.DbUser};Password={mySettings.DbPswd};" +
                              "Allow User Variables=true;AllowLoadLocalInfile=true;";
            truncate(); //debug
        }

        public void Run()
        {
            //定義ActionBlock：輸入csv檔案路徑，轉入資料庫
            var block = new ActionBlock<string>(
                async csvfile => await CsvToDB(csvfile),
                new ExecutionDataflowBlockOptions
                {
                    MaxDegreeOfParallelism = 10,   // 限制同時只處理10筆
                    BoundedCapacity = 20          // 設定最大緩衝區為100
                }
            );

            string saveToPath = mySettings.StoredFilesPath ?? "C:\\temp";
            int currTwYear = GetTaiwanYear(DateTime.Now);
            int currSeason = GetSeason(DateTime.Now);
            List<Task> tasks = [];
            for (int year = 112; year <= currTwYear; year++)
            {
                foreach (var season in Enumerable.Range(1, 4))
                {
                    if (year == currTwYear && season >= currSeason)
                    {
                        break;
                    }
                    //這裡寫成一支函式 DownloadAndScanCsv()
                    //若要使用CcontinueWith，中間要使用Unwrap()等待「所有」任務完成，而非只有最外層任務完成就結束。
                    Task t = DownloadAndScanCsv(year, season, saveToPath, block);
                    tasks.Add(t);
                }
            }
            Task.WaitAll(tasks);
            block.Complete(); //告訴block不會再送了
            block.Completion.Wait(); //等待ActionBlock完成
        }

        private async Task DownloadAndScanCsv(int year, int season, string saveToPath, ActionBlock<string> block)
        {
            string zipFile = await DownloadFile(year, season, saveToPath);
            if (string.IsNullOrWhiteSpace(zipFile)) return; //下載失敗，跳離

            string extractPath = UnZip(zipFile, saveToPath);
            List<string> csvFiles = ScanFolder(extractPath);
            foreach (var f in csvFiles)
            {
                // 使用SendAsync，如果 block 滿了會等候(背壓,Backpressure）
                await block.SendAsync(f);
            }
        }

        private async Task<string> DownloadFile(int year, int season, string saveToPath)
        {
            if (!File.Exists($"{saveToPath}\\{year}S{season}.zip"))
            {
                string url = $"{downloadUrl}&season={year}S{season}";
                const int maxRetry = 3;
                HttpResponseMessage response;
                for (int i = 0; i < maxRetry; i++)
                {
                    response = await httpClient.GetAsync(url);
                    if (response.StatusCode == System.Net.HttpStatusCode.OK)
                    {
                        using (Stream stream = await response.Content.ReadAsStreamAsync())
                        using (var fileStream = File.Create($"{saveToPath}\\{year}S{season}.zip"))
                        {
                            await stream.CopyToAsync(fileStream);
                            Console.WriteLine($"{year}S{season}.zip is downloaded.");
                            break; //成功
                        }
                    }
                    else
                    { 
                        Console.WriteLine($"Error: {url} download failure.");
                        return null;
                    }
                }
            }            
            return $"{year}S{season}.zip";
        }

        private string UnZip(string filename, string saveToPath)
        {
            string zipPath = Path.Combine(saveToPath, filename);
            string extractPath = Regex.Replace(zipPath, @"\.zip", "", RegexOptions.IgnoreCase);
            if (Directory.Exists(extractPath))
            {
                Directory.Delete(extractPath, true); //刪除資料夾及子目錄的檔案
            }
            System.IO.Compression.ZipFile.ExtractToDirectory(zipPath, extractPath);
            Console.WriteLine($"(unzip) {zipPath} => {extractPath}");
            return extractPath;
        }

        private List<string> ScanFolder(string folderPath)
        {
            object sync = new();
            List<string> csvFiles = [];
            Parallel.ForEach(Directory.GetFiles(folderPath, "*.csv"), (filePath) =>
            {
                string file = Path.GetFileName(filePath);
                if (Regex.IsMatch(file, csvPattern, RegexOptions.IgnoreCase))
                {
                    lock (sync) { csvFiles.Add(filePath); } //塞入檔案完整路徑
                }
            });
            return csvFiles;
        }

        private async Task CsvToDB(string filename)
        {
            string tableName;
            using DataTable dt = CsvToDataTable(filename, out tableName);
            using MySqlConnection conn = new MySqlConnection(connString);
            {
                //紀錄MySqlBulkCopy的異常訊息
                conn.InfoMessage += (s, e) =>
                {
                    foreach (var error in e.Errors)
                    {
                        if (error.Level != "Note")
                            Console.WriteLine($"[{error.Level}] <{filename}> {error.Message}");
                    }
                };

                //建立 Column Mappings
                List<MySqlBulkCopyColumnMapping> colMappings = new List<MySqlBulkCopyColumnMapping>();
                for (int i = 0; i < dt.Columns.Count; i++)
                {
                    colMappings.Add(new MySqlBulkCopyColumnMapping(i, dt.Columns[i].ColumnName));
                }

                //因MSQL.max_allowed_packet限制，分批寫入資料庫
                int rows = 0;
                int batchSize = 2000;
                await conn.OpenAsync();
                MySqlBulkCopy bulkCopy = new MySqlBulkCopy(conn);
                bulkCopy.DestinationTableName = tableName; //目標Table名稱
                bulkCopy.ColumnMappings.AddRange(colMappings);
                foreach (var batchDt in GetDataBatch(dt, batchSize))
                {
                    MySqlBulkCopyResult result = await bulkCopy.WriteToServerAsync(batchDt); // dataTable複製到資料庫
                    rows += result.RowsInserted;
                }
                Console.WriteLine($" [{filename} >> {tableName}] {rows} / {dt.Rows.Count} rows inserted.");
            }
        }

        private IEnumerable<DataTable> GetDataBatch(DataTable dt, int batchSize)
        {
            DataTable batchDt = dt.Clone();
            foreach (DataRow row in dt.Rows)
            {
                batchDt.ImportRow(row);
                if (batchDt.Rows.Count == batchSize)
                {
                    yield return batchDt;
                    batchDt = dt.Clone();
                }
            }
            if (batchDt.Rows.Count > 0)
            {
                yield return batchDt;
            }
        }

        private DataTable CsvToDataTable(string filename, out string tableName)
        {
            string? cityCode = null, cityName = null, dealType = null;
            //[縣市代號]_lvr_land_[交易類型]，交易類型：A-房屋買賣交易, B-新成屋交易, C-租房交易
            Match m = Regex.Match(filename, csvPattern, RegexOptions.IgnoreCase);
            if (m.Success)
            {
                cityCode = m.Groups[1].ToString().ToUpper(); //大寫
                cityName = getCityName(cityCode);
                dealType = m.Groups[2].ToString().ToUpper(); //大寫
            }
            //Null checking
            if (string.IsNullOrWhiteSpace(cityCode) || 
                string.IsNullOrWhiteSpace(cityName) ||
                string.IsNullOrWhiteSpace(dealType))
                throw new ArgumentException($"cityCode/cityName/dealType cannot be null. (filename: {filename})");

            //透過out參數回傳
            tableName = dealType switch
            {
                "A" => "old_house",
                "B" => "new_house",
                "C" => "rent_house",
                _ => throw new ArgumentException($"Unknown dealType: {dealType}")
            };
            
            //宣告
            DataTable dt;

            //讀取CSV內容並轉入DataTable
            var csvConfig = new CsvHelper.Configuration.CsvConfiguration(CultureInfo.CreateSpecificCulture("zh-TW"))
            {
                Delimiter = ",",
                MissingFieldFound = null,
                BadDataFound = null //跳過異常資料列
            };
            using (var reader = new StreamReader(filename))
            using (var csv = new CsvReader(reader, csvConfig))
            {
                BaseMapping houseData = dealType switch
                {
                    "A" => new OldHouse(cityName),
                    "B" => new NewHouse(cityName),
                    "C" => new RentHouse(cityName),
                    _ => throw new ArgumentException($"Unknown dealType: {dealType}")
                };
                dt = houseData.InitDatatable();

                int rowIdx = 0;
                DataRow row;
                while (csv.Read())
                {
                    rowIdx++;
                    if (rowIdx <= 2) continue; //CSV前兩列為中英文標題

                    try { row = houseData.Mapping(dt.NewRow(), csv); }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"{filename} mapping error.");
                        throw ex;
                    }
                    
                    dt.Rows.Add(row);
                }
            }

            return dt;
        }

        private string getCityName(string cityCode)
        {
            //由Http Request(Get)取得城市名稱及代號對照JSON資料，有緩存
            if (cityJsonStr == null)
            {
                const string cityUrl = "https://lvr.land.moi.gov.tw/SERVICE/CITY";
                using (HttpClientHandler handler = new HttpClientHandler())
                using (HttpClient client = new HttpClient(handler))
                {
                    Task.Run(async () =>
                    {
                        HttpResponseMessage response = await client.GetAsync(cityUrl);
                        response.EnsureSuccessStatusCode();
                        cityJsonStr = await response.Content.ReadAsStringAsync();
                        //Console.WriteLine(cityJsonStr);
                    }).Wait();
                }
            }
            //JSON反序列化，查詢代號對應的城市名稱
            dynamic cities = JsonConvert.DeserializeObject(cityJsonStr);
            foreach (var city in cities)
            {
                if (city.code.ToString().ToUpper() == cityCode.ToUpper() && city.use == true)
                {
                    return city.title;
                }
            }
            return ""; //查不到
        }

        private int GetTaiwanYear(DateTime dt)
        {
            TaiwanCalendar taiwanCalendar = new();
            return taiwanCalendar.GetYear(dt);
        }

        private int GetSeason(DateTime dt)
        {
            double season = Convert.ToDouble(dt.Month) / 3.0;
            return Convert.ToInt32(Math.Ceiling(season));
        }

        private void truncate()
        {
            using (MySqlConnection conn = new MySqlConnection(connString))
            {
                conn.Open();
                string tables = "old_house,new_house,rent_house";
                foreach (string t in tables.Split(","))
                {
                    using (MySqlCommand mc = new MySqlCommand($"truncate table {t}", conn))
                    {
                        try { mc.ExecuteNonQuery(); }
                        catch (Exception ex){ Console.WriteLine(ex.Message); }
                    }
                } 
            }
        }
    }
}
