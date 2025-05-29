using System.Globalization;
using System.Threading.Tasks.Dataflow;
using HousePrice.Services;

namespace HousePrice
{
    //下載房屋實價登錄資料(zip檔)
    //[政府實價登錄網站] https://plvr.land.moi.gov.tw/DownloadOpenData
    public class FileDownloader
    {
        private const string BASE_URL = @"https://plvr.land.moi.gov.tw//DownloadSeason?type=zip&fileName=lvr_landcsv.zip";
        private readonly string _saveToPath = MySettings.StoredFilesPath;
        private readonly HttpClient _httpClient = new();
        private static readonly TaiwanCalendar _taiwanCalendar = new();

        public List<Task> GenerateDownloadTasks(ActionBlock<string> block)
        {
            List<Task> tasks = [];
            int currYear = GetTaiwanYear(DateTime.Now);
            int fromYear = MySettings.DataFromYear;
            int toYear = (MySettings.DataToYear < 0) ? currYear : MySettings.DataToYear;
            int currSeason = GetSeason(DateTime.Now);
            for (int year = fromYear; year <= toYear; year++)
            {
                for (int season = 1; season <= 4; season++)
                {
                    if (year == currYear && season >= currSeason) break;

                    //這裡寫成一支方法 Process()
                    //若要使用ContinueWith，中間要使用Unwrap()等待「所有」任務完成，而非只有最外層任務完成就結束。
                    Task t = ProcessAsync(year, season, _saveToPath, block);
                    tasks.Add(t);
                }
            }
            return tasks;
        }

        private async Task ProcessAsync(int year, int season, string saveToPath, ActionBlock<string> block)
        {
            string zipPath = await DownloadFileAsync(year, season, saveToPath);
            if (string.IsNullOrWhiteSpace(zipPath)) return; //下載失敗，跳離

            string extractPath = FileExtractor.UnZip(zipPath);
            List<string> csvFiles = FileExtractor.ScanFolder(extractPath);
            foreach (var csv in csvFiles)
            {
                // 使用SendAsync，如果 block 滿了會等候(背壓,Backpressure）
                await block.SendAsync(csv);
            }
        }

        private async Task<string> DownloadFileAsync(int year, int season, string saveToPath)
        { 
            string fileName = $"{year}S{season}.zip";
            string filePath = Path.Combine(saveToPath, fileName);

            //本地已有此ZIP檔，免再下載
            if (File.Exists(filePath)) return filePath; 

            //本地無此ZIP檔，開始下載
            const int maxRetry = 3;
            string url = $"{BASE_URL}&season={year}S{season}";
            HttpResponseMessage response;
            for (int i = 1; i <= maxRetry; i++)
            {
                response = await _httpClient.GetAsync(url);
                if (response.StatusCode == System.Net.HttpStatusCode.OK)
                {
                    using (Stream stream = await response.Content.ReadAsStreamAsync())
                    using (var fileStream = File.Create(filePath))
                    {
                        await stream.CopyToAsync(fileStream);
                        Console.WriteLine($"{fileName} is downloaded.");
                        return filePath;
                    }
                }
                Console.WriteLine($"Attempt {i} failed for {fileName} - Status: {response.StatusCode}");
                await Task.Delay(1000); // delay before retry
            }

            //下載N次失敗
            Console.WriteLine($"Error: {url} download failure.");
            return null;
        }

        private static int GetTaiwanYear(DateTime dt) => _taiwanCalendar.GetYear(dt);
        
        private static int GetSeason(DateTime dt) => (dt.Month + 2) / 3;
        //月份+2：避免Ceil等額外的計算及轉型
    }
}
