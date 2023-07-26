
using SQLDataGenerator.DataGenerators;
using SQLDataGenerator.Helpers;
using SQLDataGenerator.Models;

// Take user inputs for data generation configuration.
var serverConfig = GetUserInputForServerConfig();
var userConfig = GetUserInputForUserConfig();

// Create the appropriate DataGenerator using the factory.
var dataGenerator = DataGeneratorFactory.CreateDataGenerator(serverConfig, userConfig);

// Generate data for the selected database.
dataGenerator.GenerateData();

// Show a message indicating the successful completion of data generation.
Console.WriteLine("Data generation completed successfully!");

static ServerConfiguration GetUserInputForServerConfig()
{
    var config = new ServerConfiguration();

    Console.WriteLine("Select the database server type:");
    Console.WriteLine("1. SQL Server");
    Console.WriteLine("2. MySQL (Not supported yet)");
    Console.WriteLine("3. PostgreSQL (Not supported yet)");
    Console.Write("Enter the option (1, 2, or 3): ");

    if (!int.TryParse(Console.ReadLine(), out var option) || !Enum.IsDefined(typeof(DbServerType), option))
    {
        Console.WriteLine("Invalid option. Please select a valid option (1, 2, or 3).");
        return GetUserInputForServerConfig();
    }

    config.ServerType = (DbServerType)option;

    Console.Write("Enter the server name: ");
    config.ServerName = Console.ReadLine() ?? string.Empty;

    Console.Write("Enter the database name: ");
    config.DatabaseName = Console.ReadLine() ?? string.Empty;

    Console.Write("Enter the schema name: ");
    config.SchemaName = Console.ReadLine() ?? string.Empty;

    Console.Write("Enter the username: ");
    config.Username = Console.ReadLine() ?? string.Empty;

    Console.Write("Enter the password: ");
    config.Password = Console.ReadLine() ?? string.Empty;

    Console.Write("Enter the default number of rows to generate in each table: ");
    if (!int.TryParse(Console.ReadLine(), out var numberOfRows) || numberOfRows <= 0)
    {
        Console.WriteLine("Invalid input. Number of rows must be a positive integer.");
        return GetUserInputForServerConfig();
    }
    config.NumberOfRows = numberOfRows;

    return config;
}

static UserConfiguration GetUserInputForUserConfig()
{
    Console.Write("Enter the configuration file path: ");
    var userConfigPath = Console.ReadLine() ?? string.Empty;

    if (string.IsNullOrEmpty(userConfigPath))
    {
        Console.WriteLine("User configuration path is not valid");
        return GetUserInputForUserConfig();
    }

    var parser = new ConfigurationParser();
    return parser.ParseFromJson(userConfigPath);
}