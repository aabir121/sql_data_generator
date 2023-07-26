namespace SQLDataGenerator.Models;

public class Configuration
{
    public string ServerName { get; set; }
    public string DatabaseName { get; set; }
    public string SchemaName { get; set; }
    public string Username { get; set; }
    public string Password { get; set; }
    public int NumberOfRows { get; set; }
    public Dictionary<string, TableConfiguration?> Tables { get; set; }
}