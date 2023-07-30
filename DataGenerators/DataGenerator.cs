using System.Data;
using SQLDataGenerator.Helpers;
using SQLDataGenerator.Models;
using SQLDataGenerator.Models.Config;

namespace SQLDataGenerator.DataGenerators
{
    public abstract class DataGenerator
    {
        protected readonly ServerConfiguration ServerConfig;
        protected Dictionary<string, int> RowsInsertedMap;
        private readonly CommonSettings _commonSettings;
        private readonly TableSettings _tableSettings;

        private const int MaxAllowedParams = 2100;
        private const int DesiredBatchSize = 500;
        private DateTime _startTime;

        protected DataGenerator(Configuration config)
        {
            ServerConfig = config.ServerConfiguration;
            RowsInsertedMap = new Dictionary<string, int>();
            _commonSettings = config.CommonSettings;
            _tableSettings = config.TableSettings;
        }

        public void GenerateData()
        {
            try
            {
                _startTime = DateTime.Now;
                using var connection = GetDbConnection();
                connection.Open();
                Console.WriteLine("Connected to the database server.");

                var tableNames = GetTableNames(connection);
                var tableData = GetTableData(connection, tableNames);
                var tableConfigsMap = CreateTableConfigMap();

                GenerateAndInsertData(connection, tableNames, tableData, tableConfigsMap);

                Console.WriteLine("Data generation completed successfully.");
                
                DisplayStats();
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error occurred during data generation:");
                Console.WriteLine(ex.Message);
                Console.WriteLine(ex.StackTrace);
            }
        }
        
        private void DisplayStats()
        {
            var endTime = DateTime.Now;
            var totalTimeTaken = endTime - _startTime;

            Console.WriteLine("----- Data Generation Statistics -----");
            Console.WriteLine($"Total Time Taken: {FormatTimeSpan(totalTimeTaken)}");
            foreach (var table in RowsInsertedMap)
            {
                Console.WriteLine($"Table: {table.Key}, Rows Inserted: {table.Value}");
            }
            Console.WriteLine("--------------------------------------");
        }
        
        private static string FormatTimeSpan(TimeSpan timeSpan)
        {
            // Format the TimeSpan to a user-readable format.
            return timeSpan.ToString(@"hh\:mm\:ss\.fff");
        }

        private Dictionary<string, TableConfig> CreateTableConfigMap()
        {
            var tableConfigsMap = new Dictionary<string, TableConfig>();
            foreach (var config in _tableSettings.Config)
            {
                tableConfigsMap[config.Name] = config;
            }

            return tableConfigsMap;
        }

        private void GenerateAndInsertData(IDbConnection connection, List<string> tableNames,
            Dictionary<string, TableInfo> tableData, Dictionary<string, TableConfig> tableConfigsMap)
        {
            var tableNamesToWorkWith = FilterBasedOnSettings(tableNames);
            var totalTables = tableNamesToWorkWith.Count;
            var currentTable = 0;

            foreach (var tableName in tableNamesToWorkWith)
            {
                currentTable++;
                var tableInfo = tableData[tableName];
                var tableConfig = tableConfigsMap.TryGetValue(tableName, out var config) ? config : null;

                Console.WriteLine($"Generating data for Table {currentTable}/{totalTables}: {tableName}");

                InsertDataIntoTable(connection, tableName, tableInfo, tableConfig);

                Console.WriteLine($"Data generation for Table {currentTable}/{totalTables}: {tableName} completed.");
            }
        }


        private List<string> FilterBasedOnSettings(List<string> tableNames)
        {
            if (_tableSettings.TableFilter == null)
            {
                return tableNames;
            }

            var values = _tableSettings.TableFilter.Values;
            return _tableSettings.TableFilter.FilterMode == FilterMode.Include
                ? tableNames.Where((x) => values.Contains(x)).ToList()
                : tableNames.Where((x) => !values.Contains(x)).ToList();
        }

        protected abstract IDbConnection GetDbConnection();

        protected abstract List<string> GetTableNames(IDbConnection connection);

        protected abstract Dictionary<string, TableInfo>
            GetTableData(IDbConnection connection, List<string> tableNames);

        protected abstract void InsertDataIntoTable(IDbConnection connection, string tableName, TableInfo tableInfo,
            TableConfig? tableConfig);

        protected static string GetParamPlaceholders(IEnumerable<string> columns, int rowIdx)
        {
            var placeholders = columns.Select(t => $"@{t}{rowIdx}").ToList();
            return string.Join(", ", placeholders);
        }

        protected object? GenerateRandomValue(string dataType, string columnName, List<object>? tableConfigValidValues)
        {
            return tableConfigValidValues != null
                ? FakerUtility.Instance.PickRandom(tableConfigValidValues)
                : GenerateRandomValueBasedOnDataType(dataType, columnName);
        }

        protected abstract object? GenerateRandomValueBasedOnDataType(string dataType, string columnName);

        protected int GetNumberOfRowsToInsert(TableConfig? tableSettings)
        {
            if (tableSettings == null || tableSettings.NumberOfRows == 0)
            {
                return _commonSettings.NumberOfRows;
            }

            return tableSettings.NumberOfRows;
        }

        protected static int GetAchievableBatchSize(int columnLength)
        {
            var batchSize = DesiredBatchSize;

            while (batchSize * columnLength >= MaxAllowedParams)
            {
                batchSize -= 50;
            }

            return batchSize;
        }
    }
}