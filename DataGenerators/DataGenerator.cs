using SQLDataGenerator.Models;

namespace SQLDataGenerator.DataGenerators;

public abstract class DataGenerator
{
    private DataGeneratorConfiguration _config;

    protected DataGenerator(DataGeneratorConfiguration config)
    {
        _config = config;
    }

    public abstract void GenerateData();
}