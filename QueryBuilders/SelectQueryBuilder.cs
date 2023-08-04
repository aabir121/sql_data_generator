using System.Text;
using SQLDataGenerator.Models;

namespace SQLDataGenerator.Helpers;

public class SelectQueryBuilder
{
    private readonly StringBuilder _query;
    private readonly DbServerType _serverType;

    public SelectQueryBuilder(DbServerType dbServerType)
    {
        _serverType = dbServerType;
        _query = new StringBuilder();
        _query.Append("SELECT ");
    }

    public SelectQueryBuilder ColumnsWithAliases(Dictionary<string, string> columns)
    {
        foreach (var (key, value) in columns)
        {
            _query.Append($"{key} AS {value}");
            _query.Append(", ");
        }

        _query.Length -= 2; // Remove Trailing comma
        return this;
    }

    public SelectQueryBuilder Limit(int limit)
    {
        if (_serverType == DbServerType.SqlServer)
        {
            _query.Append($" TOP({limit}) ");
            return this;
        }

        _query.Append($" LIMIT {limit}");
        return this;
    }

    public SelectQueryBuilder From(string tableName)
    {
        _query.Append($" FROM {tableName} ");
        return this;
    }

    public SelectQueryBuilder Where(string condition)
    {
        _query.Append($"WHERE {condition} ");
        return this;
    }

    public SelectQueryBuilder OrderBy(Dictionary<string, string> columns)
    {
        _query.Append(" ORDER BY ");
        foreach (var (key, value) in columns)
        {
            _query.Append($"{key} {value}");
            _query.Append(", ");
        }

        _query.Length -= 2; // Remove Trailing comma

        return this;
    }

    public string Build()
    {
        return _query.ToString();
    }
}