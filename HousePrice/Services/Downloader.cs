using System.Globalization;
using System.Threading.Tasks.Dataflow;
using HousePrice.Services;

namespace HousePrice
{
    //下載房屋實價登錄資料(zip檔)
    //[政府實價登錄網站] https://plvr.land.moi.gov.tw/DownloadOpenData
    public class Downloader
    {
        private const string DOWNLOAD_URL = "https://plvr.land.moi.gov.tw//DownloadSeason?type=zip&fileName=lvr_landcsv.zip";
        private readonly string _saveToPath = MySettings.StoredFilesPath;
        private HttpClient _httpClient = new();

        public void Run(ActionBlock<string> block)
        {
            int currYear = GetTaiwanYear(DateTime.Now);
            int toYear = (MySettings.DataToYear < 0) ? currYear : MySettings.DataToYear;
            int currSeason = GetSeason(DateTime.Now);
            List<Task> tasks = [];
            for (int year = MySettings.DataFromYear; year <= toYear; year++)
            {
                foreach (var season in Enumerable.Range(1, 4))
                {
                    if (year == currYear && season >= currSeason)
                    {
                        break;
                    }
                    //這裡寫成一支方法 Process()
                    //若要使用ContinueWith，中間要使用Unwrap()等待「所有」任務完成，而非只有最外層任務完成就結束。
                    Task t = Process(year, season, _saveToPath, block);
                    tasks.Add(t);
                }
            }
            Task.WaitAll(tasks);
            block.Complete(); //告訴block不會再送了
            block.Completion.Wait(); //等待ActionBlock完成
        }

        private async Task Process(int year, int season, string saveToPath, ActionBlock<string> block)
        {
            string zipFile = await DownloadFile(year, season, saveToPath);
            if (string.IsNullOrWhiteSpace(zipFile)) return; //下載失敗，跳離

            string extractPath = FileExtractor.UnZip(zipFile, saveToPath);
            List<string> csvFiles = FileExtractor.ScanFolder(extractPath);
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
                string url = $"{DOWNLOAD_URL}&season={year}S{season}";
                const int maxRetry = 3;
                HttpResponseMessage response;
                for (int i = 0; i < maxRetry; i++)
                {
                    response = await _httpClient.GetAsync(url);
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
    }
}
