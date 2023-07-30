using SQLDataGenerator.DataGenerators;
using SQLDataGenerator.Models;
using SQLDataGenerator.Models.Config;

namespace SQLDataGenerator.Helpers;

public abstract class DataGeneratorFactory
{
    public static DataGenerator CreateDataGenerator(DbServerType serverType, Configuration config)
    {
        return serverType switch
        {
            DbServerType.SqlServer => new SqlServerDataGenerator(config),
            DbServerType.MySql => throw new NotSupportedException("MySQL is not supported yet."),
            DbServerType.PostgreSql => new PostgreSqlDataGenerator(config),
            _ => throw new NotSupportedException("Invalid database server type.")
        };
    }
}