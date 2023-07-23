using SQLDataGenerator.Models;

namespace SQLDataGenerator.DataGenerators;

public class SqlServerDataGenerator : DataGenerator
{
    public SqlServerDataGenerator(DataGeneratorConfiguration config) : base(config)
    {
    }

    public override void GenerateData()
    {
        // Implement data generation logic specific to SQL Server.
        // Remember to show proper logs in the console during each step.
    }
}