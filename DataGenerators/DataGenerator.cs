using System.Data;
using Microsoft.Data.SqlClient;
using SQLDataGenerator.Models;

namespace SQLDataGenerator.DataGenerators;

public abstract class DataGenerator
{
    protected DataGeneratorConfiguration Config;

    protected DataGenerator(DataGeneratorConfiguration config)
    {
        Config = config;
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
            var tableColumns = GetTableColumns(connection, tableNames);

            // Generate and insert data into each table.
            foreach (var tableName in tableNames.Where(tableName => tableColumns.ContainsKey(tableName)))
            {
                InsertDataIntoTable(connection, tableName, tableColumns[tableName]);
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

    protected abstract Dictionary<string, List<string>> GetTableColumns(IDbConnection connection, List<string> tableNames);

    protected abstract void InsertDataIntoTable(IDbConnection connection, string tableName, List<string> columns);

    protected abstract void DisableForeignKeyCheck(SqlConnection connection);

    protected abstract void EnableForeignKeyCheck(SqlConnection connection);

    protected string GetParamPlaceholders(List<string> columns)
    {
        var placeholders = new List<string>();
        for (var i = 0; i < columns.Count; i++)
        {
            placeholders.Add($"@param{i}");
        }
        return string.Join(", ", placeholders);
    }
}