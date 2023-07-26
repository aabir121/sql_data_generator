using SQLDataGenerator.Models;

namespace SQLDataGenerator.Helpers;

using System.IO;
using Newtonsoft.Json;

public class ConfigurationParser
{
    public UserConfiguration ParseFromJson(string jsonFilePath)
    {
        // Read the JSON file content.
        var jsonContent = File.ReadAllText(jsonFilePath);

        // Deserialize the JSON content into the UserConfiguration object.
        var userConfig = JsonConvert.DeserializeObject<UserConfiguration>(jsonContent);

        return userConfig ?? throw new InvalidOperationException();
    }
}
