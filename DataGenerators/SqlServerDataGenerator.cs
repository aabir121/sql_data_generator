using System.Data;
using System.Text;
using Microsoft.Data.SqlClient;
using SQLDataGenerator.Constants;
using SQLDataGenerator.Helpers;
using SQLDataGenerator.Models;
using SQLDataGenerator.Models.Config;

namespace SQLDataGenerator.DataGenerators
{
    public class SqlServerDataGenerator : DataGenerator
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

        protected override List<string> GetTableNames(IDbConnection connection)
        {
            var tableNames = new List<string>();
            using var command = connection.CreateCommand();
            command.CommandText = SqlServerConstants.GetTableNamesQuery;
            command.Parameters.Add(new SqlParameter("@SchemaName", SqlDbType.NVarChar)
                { Value = ServerConfig.SchemaName });

            using var reader = command.ExecuteReader();
            while (reader.Read())
            {
                tableNames.Add(reader.GetString(0));
            }

            return tableNames;
        }

        private Dictionary<string, TableInfo> PopulateBasicTableInfoMap(IDbConnection connection)
        {
            var tableInfoMap = new Dictionary<string, TableInfo>();

            // Retrieve column names and data types for the current table.
            using (var command = connection.CreateCommand())
            {
                command.CommandText = SqlServerConstants.GetTableColumnsQuery;
                command.Parameters.Add(new SqlParameter("@SchemaName", SqlDbType.NVarChar)
                    { Value = ServerConfig.SchemaName });

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var tableName = reader.GetString(0);
                        if (!tableInfoMap.ContainsKey(tableName))
                        {
                            tableInfoMap[tableName] = new TableInfo();
                        }

                        var columnName = reader.GetString(1);
                        var dataType = reader.GetString(2);
                        int? maxLength = reader.GetValue(3) == DBNull.Value ? null : reader.GetInt32(3);

                        tableInfoMap[tableName].Columns.Add(columnName);
                        tableInfoMap[tableName].ColumnTypes.Add(columnName, dataType);
                        tableInfoMap[tableName].ColumnMaxLengths.Add(columnName, maxLength);
                    }
                }
            }

            return tableInfoMap;
        }

        private Dictionary<string, Dictionary<string, string>> PopulateForeignKeyRelationsMap(IDbConnection connection)
        {
            var foreignKeyRelationMap = new Dictionary<string, Dictionary<string, string>>();
            // Retrieve foreign key relationships for the current table.
            using (var command = connection.CreateCommand())
            {
                command.CommandText = SqlServerConstants.GetForeignKeyRelationshipsQuery;
                command.Parameters.Add(new SqlParameter("@SchemaName", SqlDbType.NVarChar)
                    { Value = ServerConfig.SchemaName });

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var tableName = reader.GetString(0);
                        if (!foreignKeyRelationMap.ContainsKey(tableName))
                        {
                            foreignKeyRelationMap[tableName] = new Dictionary<string, string>();
                        }

                        var columnName = reader.GetString(1);
                        var referencedTableName = reader.GetString(2);
                        var referencedColumnName = reader.GetString(3);

                        // Save the foreign key relationship information.
                        foreignKeyRelationMap[tableName].Add(columnName,
                            $"{referencedTableName}.{referencedColumnName}");
                    }
                }
            }

            return foreignKeyRelationMap;
        }

        private Dictionary<string, List<string>> PopulatePrimaryColumnsMap(IDbConnection connection)
        {
            var pkMap = new Dictionary<string, List<string>>();

            using (var command = connection.CreateCommand())
            {
                command.CommandText = SqlServerConstants.GetPrimaryColumnsQuery;
                command.Parameters.Add(new SqlParameter("@SchemaName", SqlDbType.NVarChar)
                    { Value = ServerConfig.SchemaName });

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var tableName = reader.GetString(0);
                        if (!pkMap.ContainsKey(tableName))
                        {
                            pkMap[tableName] = new List<string>();
                        }

                        var columnName = reader.GetString(1);

                        pkMap[tableName].Add(columnName);
                    }
                }
            }

            return pkMap;
        }


        protected override Dictionary<string, TableInfo> GetTableData(IDbConnection connection, List<string> tableNames)
        {
            var tableData = new Dictionary<string, TableInfo>();

            var tableInfoMap = PopulateBasicTableInfoMap(connection);
            var foreignKeyRelationMap = PopulateForeignKeyRelationsMap(connection);
            var pkMap = PopulatePrimaryColumnsMap(connection);

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

        protected override void InsertDataIntoTable(IDbConnection connection, string tableName, TableInfo tableInfo,
            TableConfig? tableConfig)
        {
            try
            {
                // Disable foreign key constraints before inserting data.
                DisableForeignKeyCheck((SqlConnection)connection);

                var primaryColumn = tableInfo.PrimaryColumns[0]; // Assuming the first column is the primary key column.

                if (!tableInfo.ColumnTypes.TryGetValue(primaryColumn, out var primaryDataType))
                {
                    primaryDataType = "int";
                }

                var lastRowId = primaryDataType.StartsWith("int")
                    ? GetLastIdForIntegerPrimaryColumn(connection, ServerConfig.SchemaName, tableName, primaryColumn)
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
                    // Console.WriteLine(
                    // $"Preparing Insert statements for {tableName} and for row number {startIndex} till {endIndex}");

                    var insertSql =
                        new StringBuilder(
                            $"INSERT INTO {ServerConfig.SchemaName}.{tableName} ({string.Join(", ", tableInfo.Columns)}) VALUES ");

                    for (var i = startIndex; i < endIndex; i++)
                    {
                        insertSql.Append($"({GetParamPlaceholders(tableInfo.Columns, i)}),");
                    }

                    insertSql.Length--;

                    AddParametersForEachBatch(connection, insertSql.ToString(), startIndex, endIndex, tableInfo,
                        primaryColumn,
                        referenceTableValueMap, ref lastRowId, tableConfig);

                    ReportProgress(batchSize, batches, batchIndex, totalRows);
                }

                Console.WriteLine();

                // Re-enable foreign key constraints after data insertion.
                EnableForeignKeyCheck((SqlConnection)connection);

                RowsInsertedMap[tableName] = totalRows;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error occurred while generating data for Table {tableName}:");
                Console.WriteLine(ex.Message);
                Console.WriteLine(ex.StackTrace);
            }
        }

        private static int? GetLastIdForIntegerPrimaryColumn(IDbConnection connection, string schemaName,
            string tableName, string primaryColumnName)
        {
            using var command = connection.CreateCommand();
            command.CommandText =
                $"select top(1) {primaryColumnName} from {schemaName}.{tableName} ORDER BY {primaryColumnName} DESC;";

            var result = command.ExecuteScalar();
            return result == DBNull.Value ? 1 : Convert.ToInt32(result);
        }

        protected override List<object> AllPossibleValuesForReferencingColumn(IDbConnection connection,
            string referencedTable, string referencedIdColumn)
        {
            using var command = connection.CreateCommand();
            command.CommandText =
                $"SELECT TOP 100 {referencedIdColumn} FROM {ServerConfig.SchemaName}.{referencedTable} ORDER BY NEWID()";

            var result = new List<object>();

            using var reader = command.ExecuteReader();
            while (reader.Read())
            {
                var value = reader[0];
                if (value != DBNull.Value) // Check for possible null values
                {
                    result.Add(value);
                }
            }

            return result;
        }

        protected virtual void DisableForeignKeyCheck(IDbConnection connection)
        {
            try
            {
                using var command = connection.CreateCommand();
                command.CommandText = SqlServerConstants.DisableForeignKeyCheckQuery;
                command.ExecuteNonQuery();
                Console.WriteLine("Foreign key check constraint disabled.");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error occurred while disabling foreign key constraints:");
                Console.WriteLine(ex.Message);
                Console.WriteLine(ex.StackTrace);
            }
        }

        protected virtual void EnableForeignKeyCheck(IDbConnection connection)
        {
            try
            {
                using var command = connection.CreateCommand();
                command.CommandText = SqlServerConstants.EnableForeignKeyCheckQuery;
                command.ExecuteNonQuery();
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