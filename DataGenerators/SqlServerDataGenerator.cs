using System.Data;
using Microsoft.Data.SqlClient;
using SQLDataGenerator.Constants;
using SQLDataGenerator.Helpers;
using SQLDataGenerator.Models;
using SQLDataGenerator.Models.Config;

namespace SQLDataGenerator.DataGenerators
{
    public sealed class SqlServerDataGenerator : DataGenerator
    {
        public SqlServerDataGenerator(Configuration config)
            : base(config)
        {
        }

        protected override IDbConnection GetDbConnection()
        {
            // Create and return a SqlConnection for SQL Server.
            return new SqlConnection(
                $"Data Source={ServerConfig.ServerName};Initial Catalog={ServerConfig.DatabaseName};User ID={ServerConfig.Username};" +
                $"Password={ServerConfig.Password};TrustServerCertificate=True;");
        }

        protected override List<string?> GetTableNames()
        {
            var commandParams = new List<IDbDataParameter> { new SqlParameter("@SchemaName", ServerConfig.SchemaName) };
            var queryResult = ExecuteSqlQuery(SqlServerQueries.TableNamesQuery, commandParams);

            return queryResult.Select(row => GetDataFromRow<string>(row, SqlServerColumnNames.TableName))
                .ToList();
        }

        private Dictionary<string, TableInfo> PopulateBasicTableInfoMap()
        {
            var tableInfoMap = new Dictionary<string, TableInfo>();

            var commandParams = new List<IDbDataParameter> { new SqlParameter("@SchemaName", ServerConfig.SchemaName) };

            var queryResult = ExecuteSqlQuery(SqlServerQueries.TableColumnsQuery, commandParams);

            foreach (var row in queryResult)
            {
                var tableName = GetDataFromRow<string>(row, SqlServerColumnNames.TableName);
                if (tableName == null)
                {
                    continue;
                }

                if (!tableInfoMap.ContainsKey(tableName))
                {
                    tableInfoMap[tableName] = new TableInfo();
                }

                var columnName = GetDataFromRow<string>(row, SqlServerColumnNames.ColumnName);
                var dataType = GetDataFromRow<string>(row, SqlServerColumnNames.DataType);
                int? maxLength = GetDataFromRow<int>(row, SqlServerColumnNames.CharacterMaximumLength);

                tableInfoMap[tableName].Columns.Add(columnName);
                tableInfoMap[tableName].ColumnTypes.Add(columnName, dataType);
                tableInfoMap[tableName].ColumnMaxLengths.Add(columnName, maxLength);
            }

            return tableInfoMap;
        }

        private Dictionary<string, Dictionary<string, string>> PopulateForeignKeyRelationsMap()
        {
            var foreignKeyRelationMap = new Dictionary<string, Dictionary<string, string>>();

            var commandParams = new List<IDbDataParameter> { new SqlParameter("@SchemaName", ServerConfig.SchemaName) };

            var queryResult = ExecuteSqlQuery(SqlServerQueries.ForeignKeyRelationshipsQuery, commandParams);

            foreach (var row in queryResult)
            {
                var tableName = GetDataFromRow<string>(row, SqlServerColumnNames.TableName);
                if (!foreignKeyRelationMap.ContainsKey(tableName))
                {
                    foreignKeyRelationMap[tableName] = new Dictionary<string, string>();
                }

                var columnName = GetDataFromRow<string>(row, SqlServerColumnNames.ColumnName);
                var referencedTableName =
                    GetDataFromRow<string>(row, SqlServerColumnNames.ReferencedTableName);
                var referencedColumnName =
                    GetDataFromRow<string>(row, SqlServerColumnNames.ReferencedColumnName);

                // Save the foreign key relationship information.
                foreignKeyRelationMap[tableName].Add(columnName,
                    $"{referencedTableName}.{referencedColumnName}");
            }

            return foreignKeyRelationMap;
        }

        private Dictionary<string, List<string>> PopulatePrimaryColumnsMap()
        {
            var pkMap = new Dictionary<string, List<string>>();

            var commandParams = new List<IDbDataParameter> { new SqlParameter("@SchemaName", ServerConfig.SchemaName) };
            var queryResult = ExecuteSqlQuery(SqlServerQueries.PrimaryColumnsQuery, commandParams);

            foreach (var row in queryResult)
            {
                var tableName = GetDataFromRow<string>(row, SqlServerColumnNames.TableName);
                if (!pkMap.ContainsKey(tableName))
                {
                    pkMap[tableName] = new List<string>();
                }

                var columnName = GetDataFromRow<string>(row, SqlServerColumnNames.ColumnName);
                ;

                pkMap[tableName].Add(columnName);
            }

            return pkMap;
        }


        protected override Dictionary<string, TableInfo> GetTableData(List<string> tableNames)
        {
            var tableData = new Dictionary<string, TableInfo>();

            var tableInfoMap = PopulateBasicTableInfoMap();
            var foreignKeyRelationMap = PopulateForeignKeyRelationsMap();
            var pkMap = PopulatePrimaryColumnsMap();

            foreach (var tableName in tableNames)
            {
                if (!tableInfoMap.TryGetValue(tableName, out var tableInfo))
                {
                    continue;
                }

                if (foreignKeyRelationMap.TryGetValue(tableName, out var foreignKeyMap))
                {
                    tableInfo.ForeignKeyRelationships = foreignKeyMap;
                }

                if (pkMap.TryGetValue(tableName, out var pkColumns))
                {
                    tableInfo.PrimaryColumns = pkColumns;
                }

                tableData.Add(tableName, tableInfo);
            }

            return tableData;
        }

        protected override void InsertDataIntoTable(string tableName, TableInfo tableInfo,
            TableConfig? tableConfig)
        {
            try
            {
                // Disable foreign key constraints before inserting data.
                DisableForeignKeyCheck();

                var primaryColumn = tableInfo.PrimaryColumns[0]; // Assuming the first column is the primary key column.

                if (!tableInfo.ColumnTypes.TryGetValue(primaryColumn, out var primaryDataType))
                {
                    primaryDataType = "int";
                }

                var lastRowId = primaryDataType.StartsWith("int")
                    ? GetLastIdForIntegerPrimaryColumn(ServerConfig.SchemaName, tableName, primaryColumn)
                    : null;

                // Generate and insert data in batches.
                var batchSize = GetAchievableBatchSize(tableInfo.Columns.Count); // Set the desired batch size.
                var totalRows = GetNumberOfRowsToInsert(tableConfig);
                Console.WriteLine($"Starting to insert {totalRows} rows for {tableName} with batch size {batchSize}");

                var batches = (totalRows + batchSize - 1) / batchSize; // Calculate the number of batches.
                var referenceTableValueMap = new Dictionary<string, List<object>>();

                for (var batchIndex = 0; batchIndex < batches; batchIndex++)
                {
                    var startIndex = batchIndex * batchSize;
                    var endIndex = Math.Min(startIndex + batchSize, totalRows);

                    var queryBuilder = new InsertQueryBuilder();
                    queryBuilder.InsertInto($"{ServerConfig.SchemaName}.{tableName}")
                        .Columns(tableInfo.Columns)
                        .ParamPlaceholders(startIndex, endIndex, tableInfo.Columns);

                    var commandParams = GetParameters<SqlParameter>(startIndex, endIndex,
                        tableInfo, referenceTableValueMap, ref lastRowId, tableConfig);

                    ExecuteNonQueryCommand(queryBuilder.Build(), commandParams);

                    ReportProgress(batchSize, batches, batchIndex, totalRows);
                }

                Console.WriteLine();

                // Re-enable foreign key constraints after data insertion.
                EnableForeignKeyCheck();

                RowsInsertedMap[tableName] = totalRows;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error occurred while generating data for Table {tableName}:");
                Console.WriteLine(ex.Message);
                Console.WriteLine(ex.StackTrace);
            }
        }

        private int? GetLastIdForIntegerPrimaryColumn(string schemaName,
            string tableName, string primaryColumnName)
        {
            var queryBuilder = new SelectQueryBuilder()
                .Limit(1)
                .ColumnsWithAliases(new Dictionary<string, string>
                {
                    [primaryColumnName] = "COLUMN_NAME"
                })
                .From($"{schemaName}.{tableName}")
                .OrderBy(new Dictionary<string, string>
                {
                    [primaryColumnName] = "DESC"
                });

            var queryResult = ExecuteSqlQuery(queryBuilder.Build(), new List<IDbDataParameter>());

            if (queryResult.Count == 1)
            {
                return GetDataFromRow<int>(queryResult[0], SqlServerColumnNames.ColumnName);
            }

            return null;
        }

        protected override List<object?> AllPossibleValuesForReferencingColumn(string referencedTable,
            string referencedIdColumn)
        {
            var queryBuilder = new SelectQueryBuilder()
                .Limit(100)
                .ColumnsWithAliases(new Dictionary<string, string>
                {
                    [referencedIdColumn] = SqlServerColumnNames.ColumnName
                })
                .From($"{ServerConfig.SchemaName}.{referencedTable}")
                .OrderBy(new Dictionary<string, string>
                {
                    ["NEWID()"] = "DESC"
                });

            var queryResult = ExecuteSqlQuery(queryBuilder.Build(), new List<IDbDataParameter>());

            return queryResult.Select(row =>
                GetDataFromRow<object>(row, SqlServerColumnNames.ColumnName)).ToList();
        }

        private void DisableForeignKeyCheck()
        {
            try
            {
                ExecuteNonQueryCommand(SqlServerQueries.DisableForeignKeyCheckQuery, new List<IDbDataParameter>());
                Console.WriteLine("Foreign key check constraint disabled.");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error occurred while disabling foreign key constraints:");
                Console.WriteLine(ex.Message);
                Console.WriteLine(ex.StackTrace);
            }
        }

        private void EnableForeignKeyCheck()
        {
            try
            {
                ExecuteNonQueryCommand(SqlServerQueries.EnableForeignKeyCheckQuery, new List<IDbDataParameter>());
                Console.WriteLine("Foreign key check constraint enabled.");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error occurred while enabling foreign key constraints:");
                Console.WriteLine(ex.Message);
                Console.WriteLine(ex.StackTrace);
            }
        }

        protected override object? GenerateRandomValueBasedOnDataType(string dataType, string columnName,
            int? maxLength)
        {
            try
            {
                dataType = dataType.ToLower();

                switch (dataType)
                {
                    case "nvarchar":
                    case "varchar":
                    case "text":
                        return FakerUtility.GenerateTextValue(columnName, maxLength);

                    case "int":
                    case "bigint":
                    case "smallint":
                    case "tinyint":
                        return FakerUtility.GetRandomInt();
                    case "float":
                    case "real":
                    case "decimal":
                    case "numeric":
                        return FakerUtility.GetRandomDecimal();
                    case "bit":
                        return FakerUtility.GetRandomBool();
                    case "date":
                    case "datetime":
                    case "datetime2":
                        return FakerUtility.GetRandomDate();
                    default:
                        return null;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(
                    $"Error occurred while generating random value for column {columnName} with data type {dataType}:");
                Console.WriteLine(ex.Message);
                Console.WriteLine(ex.StackTrace);
                return null;
            }
        }
    }
}