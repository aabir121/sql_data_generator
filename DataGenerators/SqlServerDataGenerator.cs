using System;
using System.Collections.Generic;
using System.Data;
using Microsoft.Data.SqlClient;
using Bogus;
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
            return new SqlConnection($"Data Source={Config.ServerName};Initial Catalog={Config.DatabaseName};User ID={Config.Username};Password={Config.Password};");
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
            // Implement the query to get column information and foreign key relationships for SQL Server.
            // Use the provided connection to execute the query for each table.
            // Return a dictionary with table names as keys and TableInfo as values.
            // TableInfo should contain columns, column types, and foreign key relationships.
            return null;
        }

        protected override void InsertDataIntoTable(IDbConnection connection, string tableName, List<string> columns, Dictionary<string, string> columnTypes, Dictionary<string, string> foreignKeyRelationships)
        {
            var insertSql = $"INSERT INTO {tableName} ({string.Join(", ", columns)}) VALUES ({GetParamPlaceholders(columns)})";

            // Disable foreign key constraints before inserting data.
            DisableForeignKeyCheck((SqlConnection)connection);

            using (var command = new SqlCommand(insertSql, (SqlConnection)connection))
            {
                // Generate and insert data for each row in the table.
                for (var i = 0; i < Config.NumberOfRows; i++)
                {
                    command.Parameters.Clear();
                    foreach (var column in columns)
                    {
                        if (!columnTypes.TryGetValue(column, out var dataType)) continue;
                        var value = GenerateRandomValueForDataType(dataType, column);
                        command.Parameters.AddWithValue($"@{column}", value);
                    }
                    command.ExecuteNonQuery();
                }
            }

            // Re-enable foreign key constraints after data insertion.
            EnableForeignKeyCheck((SqlConnection)connection);
        }

        protected override void DisableForeignKeyCheck(SqlConnection connection)
        {
            using var command = new SqlCommand("EXEC sp_MSforeachtable @command1='ALTER TABLE ? NOCHECK CONSTRAINT ALL'", connection);
            command.ExecuteNonQuery();
            Console.WriteLine("Foreign key check constraint disabled.");
        }

        protected override void EnableForeignKeyCheck(SqlConnection connection)
        {
            using var command = new SqlCommand("EXEC sp_MSforeachtable @command1='ALTER TABLE ? WITH CHECK CHECK CONSTRAINT ALL'", connection);
            command.ExecuteNonQuery();
            Console.WriteLine("Foreign key check constraint enabled.");
        }
    }
}
