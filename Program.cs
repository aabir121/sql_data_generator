using SQLDataGenerator.Helpers;
using SQLDataGenerator.Models;
using SQLDataGenerator.Models.Config;

// Take user inputs for data generation configuration.
var serverType = GetServerTypeFromUser();
var config = GetAndParseUserConfig();

// Create the appropriate DataGenerator using the factory.
var dataGenerator = DataGeneratorFactory.CreateDataGenerator(serverType, config);

// Generate data for the selected database.
dataGenerator.GenerateData();

// Show a message indicating the successful completion of data generation.
Console.WriteLine("Data generation completed successfully!");

static DbServerType GetServerTypeFromUser()
{
    while (true)
    {
        Console.WriteLine("Select the database server type:");
        Console.WriteLine("1. SQL Server");
        Console.WriteLine("2. MySQL (Not supported yet)");
        Console.WriteLine("3. PostgreSQL (Not supported yet)");
        Console.Write("Enter the option (1, 2, or 3): ");

        if (int.TryParse(Console.ReadLine(), out var option) && Enum.IsDefined(typeof(DbServerType), option)) return (DbServerType)option;
        Console.WriteLine("Invalid option. Please select a valid option (1, 2, or 3).");
    }
}

static Configuration GetAndParseUserConfig()
{
    while (true)
    {
        Console.Write("Enter the configuration file path: ");
        var userConfigPath = Console.ReadLine() ?? string.Empty;

        if (string.IsNullOrEmpty(userConfigPath))
        {
            Console.WriteLine("User configuration path is not valid");
            continue;
        }

        var parser = new ConfigurationParser();
        return parser.ParseFromJson(userConfigPath);
    }
}