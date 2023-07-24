using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using Bogus;
using Microsoft.Data.SqlClient;
using SQLDataGenerator.Models;

namespace SQLDataGenerator.DataGenerators
{
    public abstract class DataGenerator
    {
        protected readonly DataGeneratorConfiguration Config;
        private readonly Faker _faker;

        protected DataGenerator(DataGeneratorConfiguration config)
        {
            Config = config;
            _faker = CreateFaker();
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

                // Generate and insert data into each table.
                foreach (var tableName in tableNames.Where(tableName => tableData.ContainsKey(tableName)))
                {
                    var tableInfo = tableData[tableName];
                    InsertDataIntoTable(connection, tableName, tableInfo.Columns, tableInfo.ColumnTypes, tableInfo.ForeignKeyRelationships);
                }

                // Show a message indicating successful data generation.
                Console.WriteLine("Data generation completed.");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error occurred during data generation:");
                Console.WriteLine(ex.Message);
            }
        }

        protected abstract IDbConnection GetDbConnection();

        protected abstract List<string> GetTableNames(IDbConnection connection);

        protected abstract Dictionary<string, TableInfo> GetTableData(IDbConnection connection, List<string> tableNames);

        protected abstract void InsertDataIntoTable(IDbConnection connection, string tableName, List<string> columns, Dictionary<string, string> columnTypes, Dictionary<string, string> foreignKeyRelationships);

        protected abstract void DisableForeignKeyCheck(SqlConnection connection);

        protected abstract void EnableForeignKeyCheck(SqlConnection connection);

        protected string GetParamPlaceholders(List<string> columns, int rowIdx)
        {
            var placeholders = new List<string>();
            for (var i = 0; i < columns.Count; i++)
            {
                placeholders.Add($"@{columns[i]}{rowIdx}");
            }
            return string.Join(", ", placeholders);
        }

        private static Faker CreateFaker()
        {
            return new Faker();
        }

        protected object? GenerateRandomValueForDataType(string dataType, string columnName)
        {
            // Use Faker to generate random data based on column type and name.
            // You can add more data type cases as needed to handle other column types.
            switch (dataType.ToLower())
            {
                case "nvarchar":
                case "varchar":
                case "text":
                    if (columnName.ToLower().Contains("name") || columnName.ToLower().Contains("fullname"))
                    {
                        return _faker.Name.FullName();
                    }
                    if (columnName.ToLower().Contains("email"))
                    {
                        return _faker.Internet.Email();
                    }
                    if (columnName.ToLower().Contains("address"))
                    {
                        return _faker.Address.FullAddress();
                    }
                    return _faker.Lorem.Word();
                case "int":
                case "bigint":
                case "smallint":
                case "tinyint":
                    return _faker.Random.Number(1, 100);

                case "float":
                case "real":
                case "decimal":
                case "numeric":
                    return _faker.Random.Decimal(1, 100);

                case "bit":
                    return _faker.Random.Bool();

                case "date":
                case "datetime":
                case "datetime2":
                    return _faker.Date.Past();

                default:
                    return null;
            }
        }
    }
}
