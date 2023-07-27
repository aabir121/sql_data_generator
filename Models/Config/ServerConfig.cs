using Newtonsoft.Json;

namespace SQLDataGenerator.Models.Config;

public class ServerConfiguration
{
    [JsonProperty("serverName")]
    public string ServerName { get; set; }

    [JsonProperty("databaseName")]
    public string DatabaseName { get; set; }

    [JsonProperty("schemaName")]
    public string SchemaName { get; set; }

    [JsonProperty("username")]
    public string Username { get; set; }

    [JsonProperty("password")]
    public string Password { get; set; }
}