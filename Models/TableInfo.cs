namespace SQLDataGenerator.Models;

public class TableInfo
{
    public List<string> Columns { get; set; }
    public List<string> PrimaryColumns { get; set; }
    public Dictionary<string, string> ColumnTypes { get; set; }
    public Dictionary<string, int?> ColumnMaxLengths { get; set; }
    public Dictionary<string, string> ForeignKeyRelationships { get; set; }

    public TableInfo()
    {
        Columns = new List<string>();
        PrimaryColumns = new List<string>();
        ColumnTypes = new Dictionary<string, string>();
        ColumnMaxLengths = new Dictionary<string, int?>();
        ForeignKeyRelationships = new Dictionary<string, string>();
    }
}
