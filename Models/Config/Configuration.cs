using Newtonsoft.Json;

namespace SQLDataGenerator.Models.Config;

public class Configuration
{
    public Configuration(ServerConfiguration serverConfiguration, CommonSettings commonSettings, TableSettings tableSettings)
    {
        ServerConfiguration = serverConfiguration;
        CommonSettings = commonSettings;
        TableSettings = tableSettings;
    }

    [JsonProperty("database")]
    public ServerConfiguration ServerConfiguration { get; set; }

    [JsonProperty("commonSettings")]
    public CommonSettings CommonSettings { get; set; }

    [JsonProperty("tableSettings")]
    public TableSettings TableSettings { get; set; }    
}