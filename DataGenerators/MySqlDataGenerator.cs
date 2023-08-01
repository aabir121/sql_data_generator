using System.Data;
using System.Text;
using MySql.Data.MySqlClient;
using SQLDataGenerator.Constants;
using SQLDataGenerator.Helpers;
using SQLDataGenerator.Models;
using SQLDataGenerator.Models.Config;

namespace SQLDataGenerator.DataGenerators;

public class MySqlDataGenerator : DataGenerator
{
    public MySqlDataGenerator(Configuration config) : base(config)
    {
    }

    protected override IDbConnection GetDbConnection()
    {
        return new MySqlConnection($"server={ServerConfig.ServerName};port={ServerConfig.Port};" +
                                   $"user={ServerConfig.Username};password={ServerConfig.Password};" +
                                   $"database={ServerConfig.DatabaseName}");
    }

    protected override List<string> GetTableNames(IDbConnection connection)
    {
        var tableNames = GetTableNames((MySqlConnection)connection);
        var (graph, indegree) = InitTableGraph(tableNames);
        PopulateTableGraphAndIndegree((MySqlConnection)connection, tableNames, graph, indegree);

        // Perform topological sort
        var result = new List<string>();
        var queue = new Queue<string>(indegree.Where(kvp => kvp.Value == 0).Select(kvp => kvp.Key));

        while (queue.Count > 0)
        {
            var currentTable = queue.Dequeue();
            result.Add(currentTable);

            if (!graph.TryGetValue(currentTable, out var value)) continue;
            foreach (var dependentTable in value)
            {
                indegree[dependentTable]--;

                if (indegree[dependentTable] == 0)
                    queue.Enqueue(dependentTable);
            }
        }

        return result;
    }

    protected override Dictionary<string, TableInfo> GetTableData(IDbConnection connection, List<string> tableNames)
    {
        var tableData = new Dictionary<string, TableInfo>();

        var tableDepMap = PopulateTableDepMap(connection);
        var tableInfoMap = PopulateBasicTableInfoMap(connection);

        // Retrieve column names and data types for each table
        foreach (var tableName in tableNames)
        {
            if (!tableInfoMap.TryGetValue(tableName, out var tableInfo))
            {
                continue;
            }

            if (tableDepMap.TryGetValue(tableName, out var value))
            {
                tableInfo.ForeignKeyRelationships = value;
            }

            tableData.Add(tableName, tableInfo);
        }

        return tableData;
    }

    protected override void InsertDataIntoTable(IDbConnection connection, string tableName, TableInfo tableInfo,
        TableConfig? tableConfig)
    {
        try
        {
            DisableForeignKeyCheck(connection);

            var primaryColumn = tableInfo.PrimaryColumns[0]; // Assuming the first column is the primary key column.
            if (!tableInfo.ColumnTypes.TryGetValue(primaryColumn, out var primaryDataType))
            {
                primaryDataType = "int";
            }

            var lastRowId = primaryDataType.StartsWith("int")
                ? GetLastIdForIntegerPrimaryColumn(connection, tableName, primaryColumn)
                : null;

            // Generate and insert data in batches.
            var batchSize = GetAchievableBatchSize(tableInfo.Columns.Count); // Set the desired batch size.
            var totalRows = GetNumberOfRowsToInsert(tableConfig);
            // Console.WriteLine($"Starting to insert {totalRows} rows for {tableName} with batch size {batchSize}");

            var batches = (totalRows + batchSize - 1) / batchSize; // Calculate the number of batches.
            var referenceTableValueMap = new Dictionary<string, List<object>>();

            for (var batchIndex = 0; batchIndex < batches; batchIndex++)
            {
                var startIndex = batchIndex * batchSize;
                var endIndex = Math.Min(startIndex + batchSize, totalRows);
                // Console.WriteLine(
                //     $"Preparing Insert statements for {tableName} and for row number {startIndex} till {endIndex}");

                var insertSql =
                    new StringBuilder(
                        $"INSERT INTO {tableName} ({string.Join(", ", tableInfo.Columns)}) VALUES ");

                for (var i = startIndex; i < endIndex; i++)
                {
                    insertSql.Append($"({GetParamPlaceholders(tableInfo.Columns, i)}),");
                }

                insertSql.Length--;

                using var command = new MySqlCommand(insertSql.ToString(), (MySqlConnection)connection);
                // Create a new batch of parameters for each iteration.
                command.Parameters.Clear();

                // Generate and insert data for each row in the batch.
                for (var rowIndex = startIndex; rowIndex < endIndex; rowIndex++)
                {
                    foreach (var column in tableInfo.Columns)
                    {
                        if (!tableInfo.ColumnTypes.TryGetValue(column, out var dataType)) continue;
                        if (!tableInfo.ColumnMaxLengths.TryGetValue(column, out var maxLength)) continue;
                        object? value;
                        if (tableInfo.ForeignKeyRelationships.TryGetValue(column, out var referencedColumn))
                        {
                            // Generate data for referencing column based on the referenced table.
                            var referencedTable = referencedColumn[..referencedColumn.IndexOf('.')];
                            var referencedTableIdColumn =
                                referencedColumn[(referencedColumn.IndexOf('.') + 1)..];
                            var mapKey = $"{referencedTable}.{referencedTableIdColumn}";
                            List<object> possibleValues;
                            if (!referenceTableValueMap.ContainsKey(mapKey))
                            {
                                possibleValues = GetAllPossibleValuesForReferencingColumn(connection, referencedTable,
                                    referencedTableIdColumn);
                                referenceTableValueMap[mapKey] = possibleValues;
                            }
                            else
                            {
                                possibleValues = referenceTableValueMap[mapKey];
                            }

                            value = possibleValues[FakerUtility.Instance.Random.Int(0, possibleValues.Count - 1)];
                        }
                        else
                        {
                            if (column == primaryColumn && dataType.StartsWith("int"))
                            {
                                value = ++lastRowId;
                            }
                            else if (column == primaryColumn && dataType.StartsWith("char"))
                            {
                                value = Guid.NewGuid().ToString();
                            }
                            else
                            {
                                value = GenerateRandomValue(dataType, column, maxLength,
                                    tableConfig != null &&
                                    tableConfig.ValidValues.TryGetValue(column, out var validVals)
                                        ? validVals
                                        : null);
                            }
                        }

                        command.Parameters.AddWithValue($"@{column}{rowIndex}", value);
                    }
                }

                ReportProgress(batchSize, batches, batchIndex, totalRows);

                command.ExecuteNonQuery();
            }

            Console.WriteLine();

            // Re-enable foreign key constraints after data insertion.
            EnableForeignKeyCheck(connection);

            RowsInsertedMap[tableName] = totalRows;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error occurred while generating data for Table {tableName}:");
            Console.WriteLine(ex.Message);
            Console.WriteLine(ex.StackTrace);
        }
    }

    protected override object? GenerateRandomValueBasedOnDataType(string dataType, string columnName, int? maxLength)
    {
        dataType = dataType.ToLower();

        switch (dataType)
        {
            case "nvarchar":
            case "varchar":
            case "text":
                return FakerUtility.GenerateTextValue(columnName, maxLength);

            case "int":
            case "bigint":
            case "smallint":
            case "tinyint":
                return FakerUtility.GetRandomInt();

            case "float":
            case "real":
            case "decimal":
            case "numeric":
                return FakerUtility.GetRandomDecimal();

            case "bit":
                return FakerUtility.GetRandomBool();

            case "date":
            case "datetime":
            case "datetime2":
                return FakerUtility.GetRandomDate();
            default:
                return null;
        }
    }

    protected virtual void DisableForeignKeyCheck(IDbConnection connection)
    {
        try
        {
            using var command = connection.CreateCommand();
            command.CommandText = MySqlServerConstants.DisableForeignKeyCheckQuery;
            command.ExecuteNonQuery();
            Console.WriteLine("Foreign key check constraint disabled.");
        }
        catch (Exception ex)
        {
            Console.WriteLine("Error occurred while disabling foreign key constraints:");
            Console.WriteLine(ex.Message);
            Console.WriteLine(ex.StackTrace);
        }
    }

    protected virtual void EnableForeignKeyCheck(IDbConnection connection)
    {
        try
        {
            using var command = connection.CreateCommand();
            command.CommandText = MySqlServerConstants.EnableForeignKeyCheckQuery;
            command.ExecuteNonQuery();
            Console.WriteLine("Foreign key check constraint enabled.");
        }
        catch (Exception ex)
        {
            Console.WriteLine("Error occurred while enabling foreign key constraints:");
            Console.WriteLine(ex.Message);
            Console.WriteLine(ex.StackTrace);
        }
    }

    private static HashSet<string> GetTableNames(MySqlConnection connection)
    {
        var tableNames = new HashSet<string>();
        using var command = connection.CreateCommand();
        command.CommandText = MySqlServerConstants.GetTableNamesQuery;

        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            tableNames.Add(reader.GetString(0));
        }

        return tableNames;
    }

    private static (Dictionary<string, List<string>> graph, Dictionary<string, int> indegree) InitTableGraph(
        HashSet<string> tableNames)
    {
        var graph = new Dictionary<string, List<string>>();
        var indegree = new Dictionary<string, int>();

        foreach (var tableName in tableNames)
        {
            graph[tableName] = new List<string>();
            indegree[tableName] = 0;
        }

        return (graph, indegree);
    }

    private void PopulateTableGraphAndIndegree(MySqlConnection connection, HashSet<string> tableNames,
        IDictionary<string, List<string>> graph,
        IDictionary<string, int> indegree)
    {
        using var command = connection.CreateCommand();
        command.CommandText = MySqlServerConstants.GetDependencyQuery;
        command.Parameters.AddWithValue("@DatabaseName", ServerConfig.DatabaseName);

        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            var parTab = reader.GetString(2);
            var depTab = reader.GetString(0);

            if (!graph.ContainsKey(parTab))
            {
                graph[parTab] = new List<string>();
            }

            graph[parTab].Add(depTab);
            indegree[depTab]++;
        }
    }

    private static int? GetLastIdForIntegerPrimaryColumn(IDbConnection connection, string tableName,
        string primaryColumnName)
    {
        using var command = connection.CreateCommand();
        command.CommandText =
            $"select {primaryColumnName} from {tableName} ORDER BY {primaryColumnName} DESC LIMIT 1;";

        var result = command.ExecuteScalar();
        return result == DBNull.Value ? 1 : Convert.ToInt32(result);
    }

    private static List<object> GetAllPossibleValuesForReferencingColumn(IDbConnection connection,
        string referencedTable, string referencedIdColumn)
    {
        using var command = connection.CreateCommand();
        command.CommandText =
            $"SELECT {referencedIdColumn} FROM {referencedTable} ORDER BY RAND() LIMIT 100";

        var result = new List<object>();

        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            var value = reader[0];
            if (value != DBNull.Value) // Check for possible null values
            {
                result.Add(value);
            }
        }

        return result;
    }
    
    private Dictionary<string, Dictionary<string, string>> PopulateTableDepMap(IDbConnection connection)
    {
        var tableDepMap = new Dictionary<string, Dictionary<string, string>>();

        // Retrieve foreign key relationships for the current table.
        using (var command = (MySqlCommand)connection.CreateCommand())
        {
            command.CommandText = MySqlServerConstants.GetDependencyQuery;
            command.Parameters.AddWithValue("@DatabaseName", ServerConfig.DatabaseName);

            using (var reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    var tableName = reader.GetString(0);
                    var columnName = reader.GetString(1);
                    var constraintName = $"{reader.GetString(2)}.{reader.GetString(3)}";

                    if (!tableDepMap.ContainsKey(tableName))
                    {
                        tableDepMap[tableName] = new Dictionary<string, string>();
                    }

                    tableDepMap[tableName][columnName] = constraintName;
                }
            }
        }

        return tableDepMap;
    }

    private Dictionary<string, TableInfo> PopulateBasicTableInfoMap(IDbConnection connection)
    {
        var tableInfoMap = new Dictionary<string, TableInfo>();
        
        using (var command = (MySqlCommand)connection.CreateCommand())
        {
            command.CommandText = MySqlServerConstants.GetColumnsQuery;
            command.Parameters.AddWithValue("@DatabaseName", ServerConfig.DatabaseName);

            using (var reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    var tableName = reader.GetString(0);
                    if (!tableInfoMap.ContainsKey(tableName))
                    {
                        tableInfoMap[tableName] = new TableInfo();
                    }
                    
                    var columnName = reader.GetString(1);
                    var dataType = reader.GetString(2);

                    var isPrimary = "PRI".Equals(reader.GetString(3));
                    if (isPrimary)
                    {
                        tableInfoMap[tableName].PrimaryColumns.Add(columnName);
                    }

                    int? maxLength = reader.GetValue(4) == DBNull.Value ? null : reader.GetInt32(4);

                    tableInfoMap[tableName].Columns.Add(columnName);
                    tableInfoMap[tableName].ColumnTypes.Add(columnName, dataType);
                    tableInfoMap[tableName].ColumnMaxLengths.Add(columnName, maxLength);
                }
            }
        }

        return tableInfoMap;
    }
}