using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text.RegularExpressions;
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
            dataType = dataType.ToLower();

            // Use Faker to generate random data based on column type and name.
            switch (dataType)
            {
                case "nvarchar":
                case "varchar":
                case "text":
                    if (Regex.IsMatch(columnName, @"\b(?:name|fullname)\b", RegexOptions.IgnoreCase))
                    {
                        return _faker.Name.FullName();
                    }
                    if (Regex.IsMatch(columnName, @"\b(?:email)\b", RegexOptions.IgnoreCase))
                    {
                        return _faker.Internet.Email();
                    }
                    if (Regex.IsMatch(columnName, @"\b(?:address)\b", RegexOptions.IgnoreCase))
                    {
                        return _faker.Address.FullAddress();
                    }
                    if (Regex.IsMatch(columnName, @"\b(?:country)\b", RegexOptions.IgnoreCase))
                    {
                        return _faker.Address.Country();
                    }
                    if (Regex.IsMatch(columnName, @"\b(?:city)\b", RegexOptions.IgnoreCase))
                    {
                        return _faker.Address.City();
                    }
                    if (Regex.IsMatch(columnName, @"\b(?:zipcode)\b", RegexOptions.IgnoreCase))
                    {
                        return _faker.Address.ZipCode();
                    }
                    if (Regex.IsMatch(columnName, @"\b(?:status)\b", RegexOptions.IgnoreCase))
                    {
                        return GenerateRandomStatusValue();
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

                // Add more cases to handle other data types
                // For custom data types, you might need to implement your own logic.

                default:
                    return null;
            }
        }

        // Define your custom method to generate random status values
        private string GenerateRandomStatusValue()
        {
            string[] statusOptions = { "Active", "Inactive", "Pending" };
            int randomIndex = _faker.Random.Number(0, statusOptions.Length - 1);
            return statusOptions[randomIndex];
        }
    }
}
