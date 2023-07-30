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


        protected override Dictionary<string, TableInfo> GetTableData(IDbConnection connection, List<string> tableNames)
        {
            var tableData = new Dictionary<string, TableInfo>();

            foreach (var tableName in tableNames)
            {
                var tableInfo = new TableInfo();

                // Retrieve column names and data types for the current table.
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = SqlServerConstants.GetTableColumnsQuery;
                    command.Parameters.Add(new SqlParameter("@SchemaName", SqlDbType.NVarChar)
                        { Value = ServerConfig.SchemaName });
                    command.Parameters.Add(new SqlParameter("@TableName", SqlDbType.NVarChar) { Value = tableName });

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var columnName = reader.GetString(0);
                            var dataType = reader.GetString(1);
                            tableInfo.Columns.Add(columnName);
                            tableInfo.ColumnTypes.Add(columnName, dataType);
                        }
                    }
                }

                // Retrieve foreign key relationships for the current table.
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = SqlServerConstants.GetForeignKeyRelationshipsQuery;
                    command.Parameters.Add(new SqlParameter("@TableName", SqlDbType.NVarChar) { Value = tableName });

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var columnName = reader.GetString(2);
                            var referencedTableName = reader.GetString(3);
                            var referencedColumnName = reader.GetString(4);

                            // Save the foreign key relationship information.
                            tableInfo.ForeignKeyRelationships.Add(columnName,
                                $"{referencedTableName}.{referencedColumnName}");
                        }
                    }
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
                var rowsInserted = 0;
                // Disable foreign key constraints before inserting data.
                DisableForeignKeyCheck((SqlConnection)connection);

                var primaryColumn = tableInfo.Columns[0]; // Assuming the first column is the primary key column.

                // Generate and insert data in batches.
                var batchSize = GetAchievableBatchSize(tableInfo.Columns.Count); // Set the desired batch size.
                var totalRows = GetNumberOfRowsToInsert(tableConfig);
                Console.WriteLine($"Starting to insert {totalRows} rows for {tableName} with batch size {batchSize}");

                var batches = (totalRows + batchSize - 1) / batchSize; // Calculate the number of batches.
                var lastRowId =
                    GetLastIdForIntegerPrimaryColumn(connection, ServerConfig.SchemaName, tableName, primaryColumn);
                var referenceTableValueMap = new Dictionary<string, List<object>>();

                for (var batchIndex = 0; batchIndex < batches; batchIndex++)
                {
                    var startIndex = batchIndex * batchSize;
                    var endIndex = Math.Min(startIndex + batchSize, totalRows);
                    Console.WriteLine(
                        $"Preparing Insert statements for {tableName} and for row number {startIndex} till {endIndex}");

                    var insertSql =
                        new StringBuilder(
                            $"INSERT INTO {ServerConfig.SchemaName}.{tableName} ({string.Join(", ", tableInfo.Columns)}) VALUES ");

                    for (var i = startIndex; i < endIndex; i++)
                    {
                        insertSql.Append($"({GetParamPlaceholders(tableInfo.Columns, i)}),");
                    }

                    insertSql.Length--;

                    using var command = new SqlCommand(insertSql.ToString(), (SqlConnection)connection);
                    // Create a new batch of parameters for each iteration.
                    command.Parameters.Clear();

                    // Generate and insert data for each row in the batch.
                    for (var rowIndex = startIndex; rowIndex < endIndex; rowIndex++)
                    {
                        foreach (var column in tableInfo.Columns)
                        {
                            if (!tableInfo.ColumnTypes.TryGetValue(column, out var dataType)) continue;
                            object? value;
                            if (tableInfo.ForeignKeyRelationships.TryGetValue(column, out var referencedColumn))
                            {
                                // Generate data for referencing column based on the referenced table.
                                var referencedTable = referencedColumn[..referencedColumn.IndexOf('.')];
                                var referencedTableIdColumn =
                                    referencedColumn[(referencedColumn.IndexOf('.') + 1)..];
                                var mapKey = $"{referencedTable}.{referencedTableIdColumn}";
                                List<object> possibleValues;
                                if (!referenceTableValueMap.ContainsKey(mapKey))
                                {
                                    possibleValues = GetAllPossibleValuesForReferencingColumn(connection,
                                        ServerConfig.SchemaName,
                                        referencedTable,
                                        referencedTableIdColumn);
                                    referenceTableValueMap[mapKey] = possibleValues;
                                }
                                else
                                {
                                    possibleValues = referenceTableValueMap[mapKey];
                                }

                                
                                value = possibleValues[FakerUtility.Instance.Random.Int(0, possibleValues.Count - 1)];
                            }
                            else
                            {
                                if (column == primaryColumn && dataType.Equals("int"))
                                {
                                    value = ++lastRowId;
                                }
                                else
                                {
                                    value = GenerateRandomValue(dataType, column,
                                        tableConfig != null &&
                                        tableConfig.ValidValues.TryGetValue(column, out var validVals)
                                            ? validVals
                                            : null);
                                }
                            }

                            command.Parameters.AddWithValue($"@{column}{rowIndex}", value);
                        }
                    }

                    Console.WriteLine(
                        $"Inserting batch data for {tableName} and for row number {startIndex} till {endIndex}");
                    command.ExecuteNonQuery();

                    rowsInserted += (endIndex - startIndex + 1);
                }

                // Re-enable foreign key constraints after data insertion.
                EnableForeignKeyCheck((SqlConnection)connection);

                RowsInsertedMap[tableName] = rowsInserted;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error occurred while generating data for Table {tableName}:");
                Console.WriteLine(ex.Message);
                Console.WriteLine(ex.StackTrace);
            }
        }

        private static int GetLastIdForIntegerPrimaryColumn(IDbConnection connection, string schemaName,
            string tableName, string primaryColumnName)
        {
            using var command = connection.CreateCommand();
            command.CommandText =
                $"select top(1) {primaryColumnName} from {schemaName}.{tableName} ORDER BY {primaryColumnName} DESC;";

            var result = command.ExecuteScalar();
            return result == DBNull.Value ? 1 : Convert.ToInt32(result);
        }

        private static List<object> GetAllPossibleValuesForReferencingColumn(IDbConnection connection,
            string schemaName,
            string referencedTable, string referencedIdColumn)
        {
            using var command = connection.CreateCommand();
            command.CommandText =
                $"SELECT TOP 100 {referencedIdColumn} FROM {schemaName}.{referencedTable} ORDER BY NEWID()";

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
                command.CommandText = "EXEC sp_MSforeachtable @command1='ALTER TABLE ? NOCHECK CONSTRAINT ALL'";
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
                command.CommandText = "EXEC sp_MSforeachtable @command1='ALTER TABLE ? WITH CHECK CHECK CONSTRAINT ALL'";
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

        protected override object? GenerateRandomValueBasedOnDataType(string dataType, string columnName)
        {
            try
            {
                dataType = dataType.ToLower();

                switch (dataType)
                {
                    case "nvarchar":
                    case "varchar":
                    case "text":
                        return FakerUtility.GenerateTextValue(columnName);

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
                Console.WriteLine($"Error occurred while generating random value for column {columnName} with data type {dataType}:");
                Console.WriteLine(ex.Message);
                Console.WriteLine(ex.StackTrace);
                return null;
            }
        }
    }
}
