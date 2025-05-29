# HousePrice
政府房屋實價登錄資料分析網站(不含租屋)
開發環境：C# ASP.NET Core MVC (.net 9) + MySQL

Services  
　├── MySettings：讀取JSON設定檔及提供全域變數  
　└── DataImporter：下載檔案->解壓縮及過濾->寫入資料庫  
　　　　├── FileDownloader：下載實價登錄資料ZIP檔  
　　　　│　　　 └── FileExtractor：解壓縮及篩選檔案  
　　　　└── MyCsvParser：解析CSV檔  
　　　　　　　　└── CsvMapping：記錄DB和CSV對應關係及轉換資料  
