using System.Collections.Concurrent;
using System.Text.RegularExpressions;

namespace HousePrice.Services
{
    //解壓縮與篩選檔案
    public class FileExtractor
    {
        private static string CSV_PATTERN = MySettings.CsvPattern;
        //[縣市代號]_lvr_land_[交易類型]，交易類型：a-房屋買賣交易, b-新成屋交易, c-租房交易

        public static string UnZip(string zipPath)
        {
            string extractPath = Regex.Replace(zipPath, @"\.zip", "", RegexOptions.IgnoreCase);
            //刪除資料夾及子目錄的檔案
            if (Directory.Exists(extractPath)) Directory.Delete(extractPath, true); 
            //開始解壓縮
            System.IO.Compression.ZipFile.ExtractToDirectory(zipPath, extractPath);
            Console.WriteLine($"(unzip) {zipPath} => {extractPath}");
            return extractPath;
        }

        public static List<string> ScanFolder(string folderPath)
        {
            //ConcurrentBag是thread-safe版本的List，不用手動Lock
            var csvFiles = new ConcurrentBag<string>();

            Parallel.ForEach(Directory.GetFiles(folderPath, "*.csv"), (filePath) =>
            {
                string file = Path.GetFileName(filePath);
                if (Regex.IsMatch(file, CSV_PATTERN, RegexOptions.IgnoreCase))
                    csvFiles.Add(filePath); //塞入檔案完整路徑
            });

            return csvFiles.ToList();
        }
    }
}
