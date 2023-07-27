using Newtonsoft.Json;

namespace SQLDataGenerator.Models.Config;

public class Configuration
{
    [JsonProperty("database")]
    public ServerConfiguration ServerConfiguration { get; set; }

    [JsonProperty("commonSettings")]
    public CommonSettings CommonSettings { get; set; }

    [JsonProperty("tableSettings")]
    public TableSettings TableSettings { get; set; }    
}