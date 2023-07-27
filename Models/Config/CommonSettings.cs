using Newtonsoft.Json;

namespace SQLDataGenerator.Models.Config;

public class CommonSettings
{
    [JsonProperty("numberOfRows")]
    public int NumberOfRows { get; set; }
}
