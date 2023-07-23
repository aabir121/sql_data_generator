using System.Data;
using Microsoft.Data.SqlClient;
using SQLDataGenerator.Models;

namespace SQLDataGenerator.DataGenerators;

public class SqlServerDataGenerator : DataGenerator
{
    public SqlServerDataGenerator(DataGeneratorConfiguration config) : base(config)
    {
    }

    protected override IDbConnection GetDbConnection()
    {
        // Create and return a SqlConnection for SQL Server.
        // Replace "YourServer" and "YourDatabase" with the appropriate values for your SQL Server instance.
        return new SqlConnection($"Data Source={Config.ServerName};Initial Catalog={Config.DatabaseName};User ID={Config.Username};Password={Config.Password};");
    }

    protected override List<string> GetTableNames(IDbConnection connection)
    {
        // Implement the query to get table names for SQL Server.
        // Use the provided connection to execute the query.
        // Return the list of table names.
        return null;
    }

    protected override Dictionary<string, List<string>> GetTableColumns(IDbConnection connection, List<string> tableNames)
    {
        // Implement the query to get column information and foreign key relationships for SQL Server.
        // Use the provided connection to execute the query for each table.
        // Return a dictionary with table names as keys and lists of column names as values.
        return null;
    }

    protected override void InsertDataIntoTable(IDbConnection connection, string tableName, List<string> columns)
    {
        // Implement the logic to generate and insert data into the table for SQL Server.
        // Use the provided connection to execute the insert queries with the generated data.
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