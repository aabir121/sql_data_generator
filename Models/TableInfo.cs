namespace SQLDataGenerator.Models;

public class TableInfo
{
    public List<string> Columns { get; set; }
    public Dictionary<string, string> ColumnTypes { get; set; }
    public Dictionary<string, string> ForeignKeyRelationships { get; set; }

    public TableInfo()
    {
        Columns = new List<string>();
        ColumnTypes = new Dictionary<string, string>();
        ForeignKeyRelationships = new Dictionary<string, string>();
    }
}
