using System.Data;
using SQLDataGenerator.Helpers;
using SQLDataGenerator.Models;
using SQLDataGenerator.Models.Config;

namespace SQLDataGenerator.DataGenerators
{
    public abstract class DataGenerator
    {
        protected readonly ServerConfiguration ServerConfig;
        private readonly CommonSettings _commonSettings;
        private readonly TableSettings _tableSettings;
        
        private const int MaxAllowedParams = 2100;
        private const int DesiredBatchSize = 500;

        protected DataGenerator(Configuration config)
        {
            ServerConfig = config.ServerConfiguration;
            _commonSettings = config.CommonSettings;
            _tableSettings = config.TableSettings;
        }

        public void GenerateData()
        {
            try
            {
                // Connect to the database server using the provided credentials.
                using var connection = GetDbConnection();
                connection.Open();
                Console.WriteLine("Connected to the database server.");

                // Retrieve all table names from the selected schema.
                var tableNames = GetTableNames(connection);

                // Get column information and foreign key relationships for each table.
                var tableData = GetTableData(connection, tableNames);

                var tableConfigsMap = new Dictionary<string, TableConfig>();
                foreach (var c in _tableSettings.Config)
                {
                    tableConfigsMap[c.Name] = c;
                }

                // Generate and insert data into each table.
                var tableNamesToWorkWith = FilterBasedOnSettings(tableNames);
                foreach (var tableName in tableNamesToWorkWith)
                {
                    var tableInfo = tableData[tableName];
                    InsertDataIntoTable(connection, tableName, tableInfo,
                        tableConfigsMap.TryGetValue(tableName, out var config) ? config : null);
                }

                // Show a message indicating successful data generation.
                Console.WriteLine("Data generation completed.");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error occurred during data generation:");
                Console.WriteLine(ex.Message);
                Console.WriteLine(ex.StackTrace);
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

        protected abstract Dictionary<string, TableInfo> GetTableData(IDbConnection connection, List<string> tableNames);

        protected abstract void InsertDataIntoTable(IDbConnection connection, string tableName, TableInfo tableInfo,
            TableConfig? tableConfig);

        protected static string GetParamPlaceholders(IEnumerable<string> columns, int rowIdx)
        {
            var placeholders = columns.Select(t => $"@{t}{rowIdx}").ToList();
            return string.Join(", ", placeholders);
        }
        
        protected object? GenerateRandomValue(string dataType, string columnName,
            List<object>? tableConfigValidValues)
        {
            return tableConfigValidValues != null ? 
                FakerUtility.Instance.PickRandom(tableConfigValidValues) : 
                GenerateRandomValueBasedOnDataType(dataType, columnName);
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