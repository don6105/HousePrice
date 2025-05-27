namespace HousePrice.Services
{
    //讀取JSON設定檔及提供全域變數
    public class MySettings
    {
        private static IConfigurationRoot _config = null;
        public static string StoredFilesPath {
            get => getConfigValue<string>("StoredFilesPath");
        }
        public static string DbHost {
            get => getConfigValue<string>("DbHost");
        }
        public static int DbPort {
            get => getConfigValue<int>("DbPort");
        }
        public static string DbName {
            get => getConfigValue<string>("DbName");
        }
        public static string DbUser {
            get => getConfigValue<string>("DbUser");
        }
        public static string DbPswd {
            get => getConfigValue<string>("DbPswd");
        }
        public static int DataFromYear
        {
            get => getConfigValue<int>("DataFromYear");
        }
        public static int DataToYear
        {
            get => getConfigValue<int>("DataToYear");
        }

        //[縣市代號]_lvr_land_[交易類型]，交易類型：a-房屋買賣交易, b-新成屋交易, c-租房交易
        public static string CsvPattern = @"(?>^|[\\\/])([a-z])_lvr_land_([a-b])(.csv?)$";

        private static T getConfigValue<T>(string attrName)
        {
            if (_config == null)
            {
                _config = new ConfigurationBuilder()
                        .AddJsonFile("appsettings.json")
                        .AddEnvironmentVariables()
                        .Build();
            }
            return _config.GetValue<T>($"MySettings:{attrName}");
        }
    }
}
