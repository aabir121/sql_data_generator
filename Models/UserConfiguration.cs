namespace SQLDataGenerator.Models;

public record UserConfiguration(Dictionary<string, TableConfiguration?> Tables);