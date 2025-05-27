using CsvHelper;
using Newtonsoft.Json;
using System.Data;
using System.Globalization;
using System.Text.RegularExpressions;

namespace HousePrice.Services
{
    //使用 CsvHelper 及 CsvMapping 產生 DataTable
    public class MyCsvParser
    {
        private const string CITY_URL = "https://lvr.land.moi.gov.tw/SERVICE/CITY";
        private static string CSV_PATTERN = MySettings.CsvPattern;
        private static string _cityJsonStr;

        public static IEnumerable<DataTable> CsvToDataTable(string fileName, int batchSize)
        {
            (string cityCode, string cityName, string dealType) = ParseFileName(fileName);

            CsvMapping houseData = dealType switch
            {
                "A" => new OldHouse(cityName),
                "B" => new NewHouse(cityName),
                "C" => new RentHouse(cityName),
                _ => throw new ArgumentException($"Unknown dealType: {dealType}")
            };
            DataTable sampleDt = houseData.InitDatatable(); //設定DataTable的結構
            DataTable dt = sampleDt.Clone(); //複製空的結構

            //讀取CSV內容並轉入DataTable
            var csvConfig = new CsvHelper.Configuration.CsvConfiguration(CultureInfo.CreateSpecificCulture("zh-TW"))
            {
                Delimiter = ",",
                MissingFieldFound = null,
                BadDataFound = null //跳過異常資料列
            };
            using (var reader = new StreamReader(fileName))
            using (var csv = new CsvReader(reader, csvConfig))
            {
                int rowIdx = 0;
                DataRow row;
                while (csv.Read())
                {
                    rowIdx++;
                    if (rowIdx <= 2) continue; //CSV前兩列為中英文標題

                    try { row = houseData.Mapping(dt.NewRow(), csv); }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"{fileName} mapping error.");
                        throw ex;
                    }

                    dt.Rows.Add(row);
                    if (dt.Rows.Count == batchSize)
                    {
                        yield return dt; //分批回傳
                        dt = sampleDt.Clone(); //重新複製空的結構
                    }
                }
                if (dt.Rows.Count > 0)
                {
                    yield return dt; //回傳剩餘資料
                }
            }
        }

        private static (string, string, string) ParseFileName(string fileName)
        {
            string cityCode = null, cityName = null, dealType = null;
            //[縣市代號]_lvr_land_[交易類型]，交易類型：A-房屋買賣交易, B-新成屋交易, C-租房交易
            Match m = Regex.Match(fileName, CSV_PATTERN, RegexOptions.IgnoreCase);
            if (m.Success)
            {
                cityCode = m.Groups[1].ToString().ToUpper(); //大寫
                cityName = GetCityName(cityCode);
                dealType = m.Groups[2].ToString().ToUpper(); //大寫
            }
            //Null checking
            if (string.IsNullOrWhiteSpace(cityCode) ||
                string.IsNullOrWhiteSpace(cityName) ||
                string.IsNullOrWhiteSpace(dealType))
                throw new ArgumentNullException($"cityCode/cityName/dealType cannot be null. (filename: {fileName})");

            return (cityCode, cityName, dealType);
        }

        private static string GetCityName(string cityCode)
        {
            if (string.IsNullOrWhiteSpace(_cityJsonStr))
                _cityJsonStr = GetCityFromRequest(); //緩存(效能考量)

            try
            {
                //JSON反序列化，查詢代號對應的城市名稱
                dynamic cities = JsonConvert.DeserializeObject(_cityJsonStr);
                foreach (var city in cities)
                {
                    if (city.code.ToString().ToUpper() == cityCode.ToUpper() && city.use == true)
                    {
                        return city.title;
                    }
                }
            }
            catch (Exception ex) { Console.WriteLine(ex); }

            return null;//查不到
        }

        //由Http Request(Get)取得城市名稱及代號對照JSON資料
        private static string GetCityFromRequest()
        {
            const int maxRetry = 3;
            using (HttpClient httpClient = new HttpClient())
            {
                for(int i = 0; i < maxRetry; i++)
                {
                    string jsonStr = Task.Run(async () => await SendRequest(httpClient)).Result;
                    if (!string.IsNullOrWhiteSpace(jsonStr)) return jsonStr;
                }
            }
            return null; //重試N次仍失敗
        }

        private static async Task<string> SendRequest(HttpClient httpClient)
        {
            HttpResponseMessage response = await httpClient.GetAsync(CITY_URL);
            return response.IsSuccessStatusCode ? await response.Content.ReadAsStringAsync() : "";
        }
    }
}
