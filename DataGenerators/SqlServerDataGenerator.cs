using System.Data;
using System.Text;
using Microsoft.Data.SqlClient;
using SQLDataGenerator.Models;

namespace SQLDataGenerator.DataGenerators
{
    public class SqlServerDataGenerator : DataGenerator
    {
        public SqlServerDataGenerator(DataGeneratorConfiguration config) : base(config)
        {
        }

        protected override IDbConnection GetDbConnection()
        {
            // Create and return a SqlConnection for SQL Server.
            return new SqlConnection(
                $"Data Source={Config.ServerName};Initial Catalog={Config.DatabaseName};User ID={Config.Username};Password={Config.Password};TrustServerCertificate=True;");
        }

        protected override List<string> GetTableNames(IDbConnection connection)
        {
            var tableNames = new List<string>();
            using var command = connection.CreateCommand();
            command.CommandText = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = @SchemaName";
            command.Parameters.Add(new SqlParameter("@SchemaName", SqlDbType.NVarChar) { Value = Config.SchemaName });

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
                    command.CommandText =
                        "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = @SchemaName AND TABLE_NAME = @TableName";
                    command.Parameters.Add(new SqlParameter("@SchemaName", SqlDbType.NVarChar)
                    { Value = Config.SchemaName });
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
                    command.CommandText = @"
                SELECT
                    fk.name AS ForeignKeyName,
                    OBJECT_NAME(fk.parent_object_id) AS TableName,
                    cpa.name AS ColumnName,
                    OBJECT_NAME(fk.referenced_object_id) AS ReferencedTableName,
                    cref.name AS ReferencedColumnName
                FROM
                    sys.foreign_keys fk
                    INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
                    INNER JOIN sys.columns cpa ON fkc.parent_object_id = cpa.object_id AND fkc.parent_column_id = cpa.column_id
                    INNER JOIN sys.columns cref ON fkc.referenced_object_id = cref.object_id AND fkc.referenced_column_id = cref.column_id
                WHERE
                    OBJECT_NAME(fk.parent_object_id) = @TableName";

                    command.Parameters.Add(new SqlParameter("@TableName", SqlDbType.NVarChar) { Value = tableName });

                    using (var reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var foreignKeyName = reader.GetString(0);
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

        protected override void InsertDataIntoTable(IDbConnection connection, string tableName, List<string> columns,
            Dictionary<string, string> columnTypes, Dictionary<string, string> foreignKeyRelationships)
        {
            // Disable foreign key constraints before inserting data.
            DisableForeignKeyCheck((SqlConnection)connection);


            var primaryColumn = columns.FirstOrDefault(); // Assuming the first column is the primary key column.

            // Generate and insert data in batches.
            var batchSize = 100; // Set the desired batch size.
            var totalRows = Config.NumberOfRows;
            var batches = (totalRows + batchSize - 1) / batchSize; // Calculate the number of batches.

            for (var batchIndex = 0; batchIndex < batches; batchIndex++)
            {
                var startIndex = batchIndex * batchSize;
                var endIndex = Math.Min(startIndex + batchSize, totalRows);
                Console.WriteLine($"Preparing Insert statements for {tableName} and for row number {startIndex} till {endIndex}");


                var insertSql = new StringBuilder($"INSERT INTO {Config.SchemaName}.{tableName} ({string.Join(", ", columns)}) VALUES ");

                for (var i = startIndex; i < endIndex; i++)
                {
                    insertSql.Append($"({GetParamPlaceholders(columns, i)}),");
                }


                insertSql.Length--;

                using var command = new SqlCommand(insertSql.ToString(), (SqlConnection)connection);
                // Create a new batch of parameters for each iteration.
                command.Parameters.Clear();

                // Generate and insert data for each row in the batch.
                for (var rowIndex = startIndex; rowIndex < endIndex; rowIndex++)
                {
                    foreach (var column in columns)
                    {
                        if (!columnTypes.TryGetValue(column, out var dataType)) continue;
                        object? value;
                        if (foreignKeyRelationships.TryGetValue(column, out var referencedColumn))
                        {
                            // Generate data for referencing column based on the referenced table.
                            var referencedTable = referencedColumn[..referencedColumn.IndexOf('.')];
                            var referencedTableIdColumn =
                                referencedColumn[(referencedColumn.IndexOf('.') + 1)..];
                            value = GenerateRandomValueForReferencingColumn(connection, Config.SchemaName, referencedTable,
                                referencedTableIdColumn);
                        }
                        else
                        {
                            if (column == primaryColumn && dataType.Equals("int"))
                            {
                                value = (batchIndex * batchSize) + rowIndex + 1;
                            }
                            else
                            {
                                value = GenerateRandomValueForDataType(dataType, column);
                            }
                        }

                        command.Parameters.AddWithValue($"@{column}{rowIndex}", value);
                    }
                }

                Console.WriteLine($"Inserting batch data for {tableName} and for row number {startIndex} till {endIndex}");
                command.ExecuteNonQuery();
            }

            // Re-enable foreign key constraints after data insertion.
            EnableForeignKeyCheck((SqlConnection)connection);
        }



        private static object? GenerateRandomValueForReferencingColumn(IDbConnection connection, string schemaName, string referencedTable,
            string referencedIdColumn)
        {
            // Assuming the primary key of the referenced table is an integer-based type (e.g., int, bigint, smallint, tinyint)
            // or a uniqueidentifier (GUID).
            using var command = connection.CreateCommand();
            command.CommandText = $"SELECT TOP 1 {referencedIdColumn} FROM {schemaName}.{referencedTable} ORDER BY NEWID()";

            var result = command.ExecuteScalar();
            return result;
        }


        protected override void DisableForeignKeyCheck(SqlConnection connection)
        {
            using var command =
                new SqlCommand("EXEC sp_MSforeachtable @command1='ALTER TABLE ? NOCHECK CONSTRAINT ALL'", connection);
            command.ExecuteNonQuery();
            Console.WriteLine("Foreign key check constraint disabled.");
        }

        protected override void EnableForeignKeyCheck(SqlConnection connection)
        {
            using var command =
                new SqlCommand("EXEC sp_MSforeachtable @command1='ALTER TABLE ? WITH CHECK CHECK CONSTRAINT ALL'",
                    connection);
            command.ExecuteNonQuery();
            Console.WriteLine("Foreign key check constraint enabled.");
        }
    }
}