using Newtonsoft.Json;

namespace SQLDataGenerator.Models.Config;

public class ServerConfiguration
{
    public ServerConfiguration(string serverName, string databaseName, string schemaName, string username, string password)
    {
        ServerName = serverName;
        DatabaseName = databaseName;
        SchemaName = schemaName;
        Username = username;
        Password = password;
    }

    [JsonProperty("serverName")]
    public string ServerName { get; set; }
    
    [JsonProperty("port")]
    public int Port { get; set; }

    [JsonProperty("databaseName")]
    public string DatabaseName { get; set; }

    [JsonProperty("schemaName")]
    public string SchemaName { get; set; }

    [JsonProperty("username")]
    public string Username { get; set; }

    [JsonProperty("password")]
    public string Password { get; set; }
}