using System.Data;
using SQLDataGenerator.Helpers;
using SQLDataGenerator.Models;
using SQLDataGenerator.Models.Config;

namespace SQLDataGenerator.DataGenerators
{
    public abstract class DataGenerator
    {
        protected readonly ServerConfiguration ServerConfig;
        protected readonly Dictionary<string, int> RowsInsertedMap;
        private readonly CommonSettings _commonSettings;
        private readonly TableSettings _tableSettings;

        private const int GetMaxAllowedParams = 2100;
        private const int GetDesiredBatchSize = 500;
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

        protected static void ReportProgress(int batchSize, int batches, int batchIndex, int totalRows)
        {
            Console.SetCursorPosition(0, Console.CursorTop); // Move the cursor to the start of the line.
            var progress = (float)(batchIndex + 1) / batches * 100;
            var remainingRows = Math.Max(0, totalRows - (batchIndex + 1) * batchSize);

            const int progressBarWidth = 30; // Width of the progress bar (adjust as needed)
            var progressValue = (int)(progress / 100 * progressBarWidth);
            var progressBar = new string('#', progressValue).PadRight(progressBarWidth, '-');

            var progressText =
                $"Progress: [{progressBar}] {progress:F2}% | Remaining Rows: {remainingRows}/{totalRows}";
            Console.SetCursorPosition(0, Console.CursorTop); // Move the cursor to the beginning of the line.
            Console.Write(progressText.PadRight(Console.WindowWidth - 1)); // Pad with spaces to clear previous text.
        }

        private void DisplayStats()
        {
            var endTime = DateTime.Now;
            var totalTimeTaken = endTime - _startTime;

            Console.WriteLine();
            Console.WriteLine();

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
                tableConfigsMap[config.Name.ToLower()] = config;
            }

            return tableConfigsMap;
        }

        private void GenerateAndInsertData(IDbConnection connection, List<string> tableNames,
            IReadOnlyDictionary<string, TableInfo> tableData, IReadOnlyDictionary<string, TableConfig> tableConfigsMap)
        {
            var tableNamesToWorkWith = FilterBasedOnSettings(tableNames);
            var totalTables = tableNamesToWorkWith.Count;
            var currentTable = 0;

            foreach (var tableName in tableNamesToWorkWith)
            {
                currentTable++;
                var tableInfo = tableData[tableName];
                var tableConfig = tableConfigsMap.TryGetValue(tableName.ToLower(), out var config) ? config : null;

                Console.WriteLine("--------------------------------------");
                Console.WriteLine($"Generating data for Table {currentTable}/{totalTables}: {tableName}");

                InsertDataIntoTable(connection, tableName, tableInfo, tableConfig);

                Console.WriteLine($"Data generation for Table {currentTable}/{totalTables}: {tableName} completed.");
                Console.WriteLine("--------------------------------------");
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

        protected object? GenerateRandomValue(string dataType, string columnName, int? maxLength,
            List<object>? tableConfigValidValues)
        {
            return tableConfigValidValues != null
                ? FakerUtility.Instance.PickRandom(tableConfigValidValues)
                : GenerateRandomValueBasedOnDataType(dataType, columnName, maxLength);
        }

        protected abstract object? GenerateRandomValueBasedOnDataType(string dataType, string columnName,
            int? maxLength);

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
            var batchSize = GetDesiredBatchSize;

            while (batchSize * columnLength >= GetMaxAllowedParams)
            {
                batchSize -= 50;
            }

            return batchSize;
        }

        private object? GenerateRandomValueForRegularColumn(string column, string primaryColumn, string dataType,
            int? maxLength, ref int? lastRowId, TableConfig? tableConfig)
        {
            if (column == primaryColumn && dataType.StartsWith("int"))
            {
                return ++lastRowId;
            }

            if (column == primaryColumn && dataType.StartsWith("char"))
            {
                return Guid.NewGuid().ToString();
            }

            return GenerateRandomValue(dataType, column, maxLength,
                tableConfig != null &&
                tableConfig.ValidValues.TryGetValue(column, out var validValues)
                    ? validValues
                    : null);
        }

        private object GenerateDataForReferenceColumn(IDbConnection connection, string referencedColumn,
            IDictionary<string, List<object>> referenceTableValueMap)
        {
            var referencedTable = referencedColumn[..referencedColumn.IndexOf('.')];
            var referencedTableIdColumn =
                referencedColumn[(referencedColumn.IndexOf('.') + 1)..];
            var mapKey = $"{referencedTable}.{referencedTableIdColumn}";
            List<object> possibleValues;
            if (!referenceTableValueMap.ContainsKey(mapKey))
            {
                possibleValues = AllPossibleValuesForReferencingColumn(connection, referencedTable,
                    referencedTableIdColumn);
                referenceTableValueMap[mapKey] = possibleValues;
            }
            else
            {
                possibleValues = referenceTableValueMap[mapKey];
            }

            return possibleValues[FakerUtility.Instance.Random.Int(0, possibleValues.Count - 1)];
        }

        protected object? GenerateRandomValueForColumn(IDbConnection connection, TableInfo tableInfo, string column,
            string dataType,
            string primaryColumn, Dictionary<string, List<object>> referenceTableValueMap, ref int? lastRowId,
            TableConfig? tableConfig, int? maxLength)
        {
            if (tableInfo.ForeignKeyRelationships.TryGetValue(column, out var referencedColumn))
            {
                return GenerateDataForReferenceColumn(connection, referencedColumn,
                    referenceTableValueMap);
            }

            return GenerateRandomValueForRegularColumn(column, primaryColumn, dataType, maxLength, ref lastRowId,
                tableConfig);
        }

        protected void AddParametersForEachBatch(IDbConnection connection, string queryString, int startIndex,
            int endIndex, TableInfo tableInfo,
            string primaryColumn, Dictionary<string, List<object>> referenceTableValueMap, ref int? lastRowId,
            TableConfig? tableConfig)
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandText = queryString;
                // Create a new batch of parameters for each iteration.
                command.Parameters.Clear();

                // Generate and insert data for each row in the batch.
                for (var rowIndex = startIndex; rowIndex < endIndex; rowIndex++)
                {
                    foreach (var column in tableInfo.Columns)
                    {
                        if (!tableInfo.ColumnTypes.TryGetValue(column, out var dataType)) continue;
                        if (!tableInfo.ColumnMaxLengths.TryGetValue(column, out var maxLength)) continue;

                        var randomValue = GenerateRandomValueForColumn(connection, tableInfo, column, dataType,
                            primaryColumn, referenceTableValueMap, ref lastRowId, tableConfig, maxLength);

                        command.Parameters.Add(InsertStatementParameter(command, column, dataType,
                            rowIndex, randomValue));
                    }
                }

                command.ExecuteNonQuery();
            }
        }

        protected virtual IDbDataParameter InsertStatementParameter(IDbCommand command, string column, string dataType,
            int rowIndex, object? value)
        {
            var param = command.CreateParameter();
            param.ParameterName = $"@{column}{rowIndex}";
            param.Value = value;

            return param;
        }

        protected abstract List<object> AllPossibleValuesForReferencingColumn(IDbConnection connection,
            string referencedTable,
            string referencedTableIdColumn);
    }
}