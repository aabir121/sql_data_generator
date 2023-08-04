using System.Text;

namespace SQLDataGenerator.Helpers;

public class InsertQueryBuilder
{
    private readonly StringBuilder _query;

    public InsertQueryBuilder()
    {
        _query = new StringBuilder();
    }

    public InsertQueryBuilder InsertInto(string tableName)
    {
        _query.Append($"INSERT INTO {tableName}");
        return this;
    }

    public InsertQueryBuilder Columns(List<string> columns)
    {
        if (columns.Count == 0)
        {
            return this;
        }

        _query.Append(" (");
        _query.Append(columns[0]);

        for (int i = 1; i < columns.Count; i++)
        {
            _query.Append(", ");
            _query.Append(columns[i]);
        }

        _query.Append(")");

        return this;
    }

    public InsertQueryBuilder ParamPlaceholders(int startIdx, int endIdx, IEnumerable<string> columns)
    {
        _query.Append(" VALUES");
        for (var i = startIdx; i < endIdx; i++)
        {
            var placeholders = new StringBuilder();
            foreach (var column in columns)
            {
                var parameterName = $"@{column}{i}";
                placeholders.Append(parameterName);
                placeholders.Append(", ");
            }

            placeholders.Length -= 2; // Remove the trailing comma and space

            _query.Append($"({placeholders}),");
        }

        _query.Length--; // Remove last trailing comma

        return this;
    }

    public string Build()
    {
        return _query.ToString();
    }
}