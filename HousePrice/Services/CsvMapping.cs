using System.Data;
using System.Reflection;
using CsvHelper;

namespace HousePrice.Services
{
    public class BaseMapping
    {
        protected string city;
        private Dictionary<string, int> columnMapping; //class反射效率差，優化

        public BaseMapping(string cityName)
        {
            city = cityName;
            columnMapping = [];
        }

        //透過「繼承+Reflection」產生DataRow的結構
        public DataTable InitDatatable()
        {
            DataTable dt = new();
            dt.Columns.Add("city", typeof(string));

            Type type = this.GetType(); //透過繼承，取得實際子類別
            FieldInfo[] fields = type.GetFields(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public);
            foreach (var field in fields)
            {
                var value = field.GetValue(this);
                if (value is ValueTuple<Type, int> tuple) //有透過Tuple設定DB和CSV的對應關係
                {
                    Type DbColumnType = tuple.Item1;
                    int CsvColumnIdx = tuple.Item2;
                    string DbColumnName = field.Name;
                    //設定DataTable的欄位
                    dt.Columns.Add(DbColumnName, DbColumnType);
                    //優化：Reflection效率差，紀錄「DB欄位名稱:CSV欄位順序」
                    columnMapping.Add(DbColumnName, CsvColumnIdx); 
                }
            }
            return dt;
        }

        //透過「繼承+Reflection」塞入DataRow的資料
        public DataRow Mapping(DataRow row, CsvReader csv)
        {
            row["city"] = city;
            
            foreach (var field in columnMapping)
            {
                Type columnType = row.Table.Columns[field.Key].DataType;
                row[field.Key] = convertType(csv.GetField<string>(field.Value), columnType);
            }
            return row;
        }

        // CSV欄位資料之型態轉換
        protected object convertType(string value, Type targetType)
        {
            if (targetType == typeof(int) || targetType == typeof(uint))
            {
                int r = String.IsNullOrEmpty(value) ? 0 : Convert.ToInt32(value);
                return (object)r;
            }
            else if (targetType == typeof(byte))
            {
                byte r = String.IsNullOrEmpty(value) ? (byte)0 : Convert.ToByte(value);
                return (object)r;
            }
            else if (targetType == typeof(decimal))
            {
                decimal r = String.IsNullOrEmpty(value) ? 0 : Convert.ToDecimal(value);
                return (object)r;
            }
            else
            {
                return (object)Convert.ToString(value);
            }
        }
    }

    public class OldHouse : BaseMapping
    {
        // DB欄位型態、名稱、CSV欄位次序
        private (Type type, int csvIdx) town = (typeof(string), 0);
        private (Type type, int csvIdx) deal_type = (typeof(string), 1);
        private (Type type, int csvIdx) location = (typeof(string), 2);
        private (Type type, int csvIdx) area = (typeof(decimal), 3);

        private (Type type, int csvIdx) land_type = (typeof(string), 4);
        private (Type type, int csvIdx) deal_date = (typeof(string), 7);
        private (Type type, int csvIdx) deal_count = (typeof(string), 8);
        private (Type type, int csvIdx) deal_floor = (typeof(string), 9);
        private (Type type, int csvIdx) total_floor = (typeof(string), 10);

        private (Type type, int csvIdx) building_type = (typeof(string), 11);
        private (Type type, int csvIdx) main_use = (typeof(string), 12);
        private (Type type, int csvIdx) building_material = (typeof(string), 13);
        private (Type type, int csvIdx) building_construct_date = (typeof(string), 14);
        private (Type type, int csvIdx) building_area = (typeof(decimal), 15);

        private (Type type, int csvIdx) bed_room_num = (typeof(uint), 16);
        private (Type type, int csvIdx) living_room_num = (typeof(uint), 17);
        private (Type type, int csvIdx) bath_room_num = (typeof(uint), 18);
        private (Type type, int csvIdx) management_center = (typeof(string), 20);
        private (Type type, int csvIdx) total_price = (typeof(decimal), 21);

        private (Type type, int csvIdx) avg_price = (typeof(decimal), 22);
        private (Type type, int csvIdx) parking_type = (typeof(string), 23);
        private (Type type, int csvIdx) parking_area = (typeof(decimal), 24);
        private (Type type, int csvIdx) parking_price = (typeof(decimal), 25);
        private (Type type, int csvIdx) comment = (typeof(string), 26);

        private (Type type, int csvIdx) serial_number = (typeof(string), 27);
        private (Type type, int csvIdx) main_building_area = (typeof(decimal), 28);
        private (Type type, int csvIdx) auxiliary_building_area = (typeof(decimal), 29);
        private (Type type, int csvIdx) balcony_area = (typeof(decimal), 30);
        private (Type type, int csvIdx) elevator = (typeof(string), 31);

        public OldHouse(string cityName) : base(cityName) { }
    }

    public class NewHouse : BaseMapping
    {
        // DB欄位型態、名稱、CSV欄位次序
        private (Type type, int csvIdx) town = (typeof(string), 0);
        private (Type type, int csvIdx) deal_type = (typeof(string), 1);
        private (Type type, int csvIdx) location = (typeof(string), 2);
        private (Type type, int csvIdx) area = (typeof(decimal), 3);

        private (Type type, int csvIdx) land_type = (typeof(string), 4);
        private (Type type, int csvIdx) deal_date = (typeof(string), 7);
        private (Type type, int csvIdx) deal_count = (typeof(string), 8);
        private (Type type, int csvIdx) deal_floor = (typeof(string), 9);
        private (Type type, int csvIdx) total_floor = (typeof(string), 10);

        private (Type type, int csvIdx) building_type = (typeof(string), 11);
        private (Type type, int csvIdx) main_use = (typeof(string), 12);
        private (Type type, int csvIdx) building_material = (typeof(string), 13);
        private (Type type, int csvIdx) building_construct_date = (typeof(string), 14);
        private (Type type, int csvIdx) building_area = (typeof(decimal), 15);

        private (Type type, int csvIdx) bed_room_num = (typeof(uint), 16);
        private (Type type, int csvIdx) living_room_num = (typeof(uint), 17);
        private (Type type, int csvIdx) bath_room_num = (typeof(uint), 18);
        private (Type type, int csvIdx) management_center = (typeof(string), 20);
        private (Type type, int csvIdx) total_price = (typeof(decimal), 21);

        private (Type type, int csvIdx) avg_price = (typeof(decimal), 22);
        private (Type type, int csvIdx) parking_type = (typeof(string), 23);
        private (Type type, int csvIdx) parking_area = (typeof(decimal), 24);
        private (Type type, int csvIdx) parking_price = (typeof(decimal), 25);
        private (Type type, int csvIdx) comment = (typeof(string), 26);

        private (Type type, int csvIdx) serial_number = (typeof(string), 27);
        private (Type type, int csvIdx) village_name = (typeof(string), 28);
        private (Type type, int csvIdx) building_no = (typeof(string), 29);
        public NewHouse(string cityName) : base(cityName) { }
    }

    public class RentHouse : BaseMapping
    {
        // DB欄位型態、名稱、CSV欄位次序

        public RentHouse(string cityName) : base(cityName) { }
    }

}
