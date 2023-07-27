namespace SQLDataGenerator.Helpers;

using System.IO;
using Newtonsoft.Json;
using SQLDataGenerator.Models.Config;

public class ConfigurationParser
{
    public Configuration ParseFromJson(string jsonFilePath)
    {
        // Read the JSON file content.
        var jsonContent = File.ReadAllText(jsonFilePath);

        // Deserialize the JSON content into the UserConfiguration object.
        var userConfig = JsonConvert.DeserializeObject<Configuration>(jsonContent);

        return userConfig ?? throw new InvalidOperationException();
    }
}
