using System.Data;
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
            var insertSql =
                $"INSERT INTO {tableName} ({string.Join(", ", columns)}) VALUES ({GetParamPlaceholders(columns)})";

            // Disable foreign key constraints before inserting data.
            DisableForeignKeyCheck((SqlConnection)connection);

            // Insert data into referenced tables first.
            var referencingColumns = new Dictionary<string, string>();
            foreach (var column in columns)
            {
                if (foreignKeyRelationships.TryGetValue(column, out var referencedColumn))
                {
                    referencingColumns.Add(column, referencedColumn);
                }
            }

            if (referencingColumns.Count > 0)
            {
                var referencedTables = new HashSet<string>();
                foreach (var referencedTable in referencingColumns.Values.Select(referencedColumn => referencedColumn.Substring(0, referencedColumn.IndexOf('.'))))
                {
                    referencedTables.Add(referencedTable);
                }

                foreach (var referencedTable in referencedTables)
                {
                    if (referencedTable == tableName) continue;
                    // Insert data into referenced table first.
                    var referencedTableInfo =
                        GetTableData(connection, new List<string> { referencedTable })[referencedTable];
                    InsertDataIntoTable(connection, referencedTable, referencedTableInfo.Columns,
                        referencedTableInfo.ColumnTypes, referencedTableInfo.ForeignKeyRelationships);
                }
            }

            using (var command = new SqlCommand(insertSql, (SqlConnection)connection))
            {
                // Generate and insert data for each row in the table.
                for (var i = 0; i < Config.NumberOfRows; i++)
                {
                    command.Parameters.Clear();
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
                            value = GenerateRandomValueForReferencingColumn(connection, referencedTable,
                                referencedTableIdColumn);
                        }
                        else
                        {
                            value = GenerateRandomValueForDataType(dataType, column);
                        }

                        command.Parameters.AddWithValue($"@{column}", value);
                    }

                    command.ExecuteNonQuery();
                }
            }

            // Re-enable foreign key constraints after data insertion.
            EnableForeignKeyCheck((SqlConnection)connection);
        }

        private static object? GenerateRandomValueForReferencingColumn(IDbConnection connection, string referencedTable,
            string referencedIdColumn)
        {
            // Assuming the primary key of the referenced table is an integer-based type (e.g., int, bigint, smallint, tinyint)
            // or a uniqueidentifier (GUID).
            using var command = connection.CreateCommand();
            command.CommandText = $"SELECT TOP 1 {referencedIdColumn} FROM {referencedTable} ORDER BY NEWID()";

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