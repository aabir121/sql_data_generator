using SQLDataGenerator.DataGenerators;
using SQLDataGenerator.Models;

namespace SQLDataGenerator.Helpers;

public class DataGeneratorFactory
{
    public static DataGenerator CreateDataGenerator(ServerConfiguration serverConfig, UserConfiguration userConfig)
    {
        return serverConfig.ServerType switch
        {
            DbServerType.SqlServer => new SqlServerDataGenerator(serverConfig, userConfig),
            DbServerType.MySql => throw new NotSupportedException("MySQL is not supported yet."),
            DbServerType.PostgreSql => throw new NotSupportedException("PostgreSQL is not supported yet."),
            _ => throw new NotSupportedException("Invalid database server type.")
        };
    }
}