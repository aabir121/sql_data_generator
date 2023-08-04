using System.Data;
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

    protected override List<string?> GetTableNames()
    {
        var tableNames = GetAllTableNames();
        var (graph, indegree) = InitTableGraph(tableNames);
        PopulateTableGraphAndIndegree(graph, indegree);

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

    protected override Dictionary<string, TableInfo> GetTableData(List<string> tableNames)
    {
        var tableData = new Dictionary<string, TableInfo>();

        var tableDepMap = PopulateTableDepMap();
        var tableInfoMap = PopulateBasicTableInfoMap();

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

    protected override void InsertDataIntoTable(string tableName, TableInfo tableInfo,
        TableConfig? tableConfig)
    {
        try
        {
            DisableForeignKeyCheck();

            var primaryColumn = tableInfo.PrimaryColumns[0]; // Assuming the first column is the primary key column.
            if (!tableInfo.ColumnTypes.TryGetValue(primaryColumn, out var primaryDataType))
            {
                primaryDataType = "int";
            }

            var lastRowId = primaryDataType.StartsWith("int")
                ? GetLastIdForIntegerPrimaryColumn(tableName, primaryColumn)
                : null;

            // Generate and insert data in batches.
            var batchSize = GetAchievableBatchSize(tableInfo.Columns.Count); // Set the desired batch size.
            var totalRows = GetNumberOfRowsToInsert(tableConfig);

            var batches = (totalRows + batchSize - 1) / batchSize; // Calculate the number of batches.
            var referenceTableValueMap = new Dictionary<string, List<object>>();

            for (var batchIndex = 0; batchIndex < batches; batchIndex++)
            {
                var startIndex = batchIndex * batchSize;
                var endIndex = Math.Min(startIndex + batchSize, totalRows);

                var queryBuilder = new InsertQueryBuilder();
                queryBuilder.InsertInto($"{tableName}")
                    .Columns(tableInfo.Columns)
                    .ParamPlaceholders(startIndex, endIndex, tableInfo.Columns);

                var commandParams = GetParameters<MySqlParameter>(startIndex, endIndex, tableInfo,
                    referenceTableValueMap, ref lastRowId, tableConfig);

                ExecuteNonQueryCommand(queryBuilder.Build(), commandParams);

                ReportProgress(batchSize, batches, batchIndex, totalRows);
            }

            Console.WriteLine();

            // Re-enable foreign key constraints after data insertion.
            EnableForeignKeyCheck();

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

    private void DisableForeignKeyCheck()
    {
        try
        {
            ExecuteNonQueryCommand(MySqlQueries.DisableForeignKeyCheckQuery, new List<IDbDataParameter>());
            Console.WriteLine("Foreign key check constraint disabled.");
        }
        catch (Exception ex)
        {
            Console.WriteLine("Error occurred while disabling foreign key constraints:");
            Console.WriteLine(ex.Message);
            Console.WriteLine(ex.StackTrace);
        }
    }

    private void EnableForeignKeyCheck()
    {
        try
        {
            ExecuteNonQueryCommand(MySqlQueries.EnableForeignKeyCheckQuery, new List<IDbDataParameter>());
            Console.WriteLine("Foreign key check constraint enabled.");
        }
        catch (Exception ex)
        {
            Console.WriteLine("Error occurred while enabling foreign key constraints:");
            Console.WriteLine(ex.Message);
            Console.WriteLine(ex.StackTrace);
        }
    }

    private HashSet<string> GetAllTableNames()
    {
        var queryResult = ExecuteSqlQuery(MySqlQueries.TableNamesQuery,
            new List<IDbDataParameter>
            {
                new MySqlParameter("@DatabaseName", ServerConfig.DatabaseName)
            });

        var tableNames = new HashSet<string>();
        foreach (var row in queryResult)
        {
            tableNames.Add(GetDataFromRow<string>(row, MySqlColumnNames.ColumnName) ?? string.Empty);
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

    private void PopulateTableGraphAndIndegree(IDictionary<string, List<string>> graph,
        IDictionary<string, int> indegree)
    {
        var queryResult = ExecuteSqlQuery(MySqlQueries.DependencyQuery, new List<IDbDataParameter>
        {
            new MySqlParameter("@DatabaseName", ServerConfig.DatabaseName)
        });
        foreach (var row in queryResult)
        {
            var parTab = GetDataFromRow<string>(row, MySqlColumnNames.ReferencedTableName);
            var depTab = GetDataFromRow<string>(row, MySqlColumnNames.TableName);

            if (parTab == null || depTab == null)
            {
                continue;
            }

            if (!graph.ContainsKey(parTab))
            {
                graph[parTab] = new List<string>();
            }

            graph[parTab].Add(depTab);
            indegree[depTab]++;
        }
    }

    private int? GetLastIdForIntegerPrimaryColumn(string tableName,
        string primaryColumnName)
    {
        var queryBuilder = new SelectQueryBuilder(DbServerType.MySql)
            .ColumnsWithAliases(new Dictionary<string, string>
            {
                [primaryColumnName] = MySqlColumnNames.ColumnName
            })
            .From($"{tableName}")
            .OrderBy(new Dictionary<string, string>
            {
                [primaryColumnName] = "DESC"
            })
            .Limit(1);

        var queryResult = ExecuteSqlQuery(queryBuilder.Build(), new List<IDbDataParameter>());

        if (queryResult.Count == 1)
        {
            return GetDataFromRow<int>(queryResult[0], SqlServerColumnNames.ColumnName);
        }

        return 0;
    }

    protected override List<object?> AllPossibleValuesForReferencingColumn(string referencedTable,
        string referencedIdColumn)
    {
        var queryBuilder = new SelectQueryBuilder(DbServerType.MySql)
            .ColumnsWithAliases(new Dictionary<string, string>
            {
                [referencedIdColumn] = MySqlColumnNames.ColumnName
            })
            .From($"{referencedTable}")
            .OrderBy(new Dictionary<string, string>
            {
                ["RAND()"] = "DESC"
            })
            .Limit(100);

        var queryResult = ExecuteSqlQuery(queryBuilder.Build(), new List<IDbDataParameter>());

        return queryResult.Select(row => GetDataFromRow<object>(row, MySqlColumnNames.ColumnName)).ToList();
    }

    private Dictionary<string, Dictionary<string, string>> PopulateTableDepMap()
    {
        var tableDepMap = new Dictionary<string, Dictionary<string, string>>();

        var queryResult = ExecuteSqlQuery(MySqlQueries.DependencyQuery, new List<IDbDataParameter>
        {
            new MySqlParameter("@DatabaseName", ServerConfig.DatabaseName)
        });
        foreach (var row in queryResult)
        {
            var tableName = GetDataFromRow<string>(row, MySqlColumnNames.TableName);
            var columnName = GetDataFromRow<string>(row, MySqlColumnNames.ColumnName);
            if (tableName == null || columnName == null)
            {
                continue;
            }

            var constraintName =
                $"{GetDataFromRow<string>(row, MySqlColumnNames.ReferencedTableName)}.{GetDataFromRow<string>(row, MySqlColumnNames.ReferencedColumnName)}";

            if (!tableDepMap.ContainsKey(tableName))
            {
                tableDepMap[tableName] = new Dictionary<string, string>();
            }

            tableDepMap[tableName][columnName] = constraintName;
        }

        return tableDepMap;
    }

    private Dictionary<string, TableInfo> PopulateBasicTableInfoMap()
    {
        var tableInfoMap = new Dictionary<string, TableInfo>();

        var queryResult = ExecuteSqlQuery(MySqlQueries.ColumnsQuery, new List<IDbDataParameter>
        {
            new MySqlParameter("@DatabaseName", ServerConfig.DatabaseName)
        });

        foreach (var row in queryResult)
        {
            var tableName = GetDataFromRow<string>(row, MySqlColumnNames.TableName);
            var columnName = GetDataFromRow<string>(row, MySqlColumnNames.ColumnName);
            var dataType = GetDataFromRow<string>(row, MySqlColumnNames.DataType);
            var isPrimary = "PRI".Equals(GetDataFromRow<string>(row, MySqlColumnNames.ColumnKey));
            int? maxLength = GetDataFromRow<int>(row, MySqlColumnNames.CharacterMaximumLength);

            if (tableName == null || columnName == null || dataType == null) continue;

            if (!tableInfoMap.ContainsKey(tableName))
            {
                tableInfoMap[tableName] = new TableInfo();
            }

            if (isPrimary)
            {
                tableInfoMap[tableName].PrimaryColumns.Add(columnName);
            }

            tableInfoMap[tableName].Columns.Add(columnName);
            tableInfoMap[tableName].ColumnTypes.Add(columnName, dataType);
            tableInfoMap[tableName].ColumnMaxLengths.Add(columnName, maxLength);
        }

        return tableInfoMap;
    }
}