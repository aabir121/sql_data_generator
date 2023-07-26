namespace SQLDataGenerator.Models;

public class ServerConfiguration
{
    public DbServerType ServerType { get; set; }
    public string ServerName { get; set; }
    public string DatabaseName { get; set; }
    public string SchemaName { get; set; }
    public string Username { get; set; }
    public string Password { get; set; }
    public int NumberOfRows { get; set; }
}