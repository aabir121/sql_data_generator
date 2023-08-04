using System.Data;
using Npgsql;
using NpgsqlTypes;
using SQLDataGenerator.Constants;
using SQLDataGenerator.Helpers;
using SQLDataGenerator.Models;
using SQLDataGenerator.Models.Config;

namespace SQLDataGenerator.DataGenerators
{
    public class PostgreSqlDataGenerator : DataGenerator
    {
        public PostgreSqlDataGenerator(Configuration config)
            : base(config)
        {
        }

        protected override IDbConnection GetDbConnection()
        {
            // Create and return a NpgsqlConnection for PostgreSQL.
            return new NpgsqlConnection($"Host={ServerConfig.ServerName};Port={ServerConfig.Port};" +
                                        $"Database={ServerConfig.DatabaseName};Username={ServerConfig.Username};Password={ServerConfig.Password};");
        }

        protected override List<string?> GetTableNames()
        {
            var tableNames = GetAllTableNames();
            var (graph, indegree) = InitTableGraph(tableNames);
            PopulateTableGraphAndIndegree(tableNames, graph, indegree);

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

            var primaryColumnsMap = GetPrimaryColumns();
            var foreignKeyMap = GetForeignKeyRelationshipsMap();
            var tableInfoMap = PopulateTableInfoWithBasicData();

            // Retrieve column names and data types for each table
            foreach (var tableName in tableNames)
            {
                if (!tableInfoMap.TryGetValue(tableName, out var tableInfo))
                {
                    continue;
                }

                if (foreignKeyMap.TryGetValue(tableName, out var keyMap))
                {
                    tableInfo.ForeignKeyRelationships = keyMap;
                }

                if (primaryColumnsMap.TryGetValue(tableName, out var pkColumns))
                {
                    tableInfo.PrimaryColumns = pkColumns;
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
                // Disable foreign key constraints before inserting data.
                DisableForeignKeyCheck();

                var primaryColumn = tableInfo.PrimaryColumns[0]; // Assuming the first column is the primary key column.
                if (!tableInfo.ColumnTypes.TryGetValue(primaryColumn, out var primaryDataType))
                {
                    primaryDataType = "int";
                }

                var lastRowId = primaryDataType.StartsWith("int")
                    ? GetLastIdForIntegerPrimaryColumn(ServerConfig.SchemaName, tableName, primaryColumn)
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

                    var queryBuilder = new InsertQueryBuilder();
                    queryBuilder.InsertInto($"{ServerConfig.SchemaName}.{tableName}")
                        .Columns(tableInfo.Columns)
                        .ParamPlaceholders(startIndex, endIndex, tableInfo.Columns);

                    var commandParams = GetParameters<NpgsqlParameter>(startIndex, endIndex,
                        tableInfo, referenceTableValueMap, ref lastRowId, tableConfig);

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

        protected override object? GenerateRandomValueBasedOnDataType(string postgresDataType, string columnName,
            int? maxLength)
        {
            try
            {
                postgresDataType = postgresDataType.ToLower();

                var npgsqlDbType = GetNpgsqlDbType(postgresDataType);

                switch (npgsqlDbType)
                {
                    case NpgsqlDbType.Text:
                    case NpgsqlDbType.Varchar:
                    case NpgsqlDbType.Json:
                        return FakerUtility.GenerateTextValue(columnName, maxLength);

                    case NpgsqlDbType.Integer:
                    case NpgsqlDbType.Bigint:
                    case NpgsqlDbType.Smallint:
                        return FakerUtility.GetRandomInt();
                    case NpgsqlDbType.Numeric:
                    case NpgsqlDbType.Real:
                    case NpgsqlDbType.Double:
                        return FakerUtility.GetRandomDecimal();
                    case NpgsqlDbType.Boolean:
                        return FakerUtility.GetRandomBool();
                    case NpgsqlDbType.Date:
                    case NpgsqlDbType.Timestamp:
                        return FakerUtility.GetRandomDate();
                    default:
                        return null;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(
                    $"Error occurred while generating random value for column {columnName} with data type {postgresDataType}:");
                Console.WriteLine(ex.Message);
                Console.WriteLine(ex.StackTrace);
                return null;
            }
        }

        private void DisableForeignKeyCheck()
        {
            try
            {
                ExecuteNonQueryCommand(PostgreSqlServerQueries.DisableForeignKeyCheckQuery,
                    new List<IDbDataParameter>());
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
                ExecuteNonQueryCommand(PostgreSqlServerQueries.EnableForeignKeyCheckQuery,
                    new List<IDbDataParameter>());
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
            var queryResult = ExecuteSqlQuery(PostgreSqlServerQueries.TableNamesQuery, new List<IDbDataParameter>
            {
                new NpgsqlParameter("@SchemaName", ServerConfig.SchemaName)
            });
            
            var tableNames = new HashSet<string>();

            foreach (var row in queryResult)
            {
                tableNames.Add(GetDataFromRow<string>(row, PostgreSqlColumnNames.TableName) ?? string.Empty);
            }
            
            return tableNames;
        }

        private (Dictionary<string, List<string>> graph, Dictionary<string, int> indegree) InitTableGraph(
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

        private static string FindLongestPrefix(HashSet<string> referenceSet, string input)
        {
            var longestPrefix = "";

            foreach (var str in referenceSet)
            {
                var i = 0;
                while (i < str.Length && i < input.Length && str[i] == input[i])
                {
                    i++;
                }

                if (i > 0 && i > longestPrefix.Length)
                {
                    longestPrefix = str[..i];
                }
            }

            return longestPrefix;
        }

        private void PopulateTableGraphAndIndegree(HashSet<string> tableNames,
            IDictionary<string, List<string>> graph,
            IDictionary<string, int> indegree)
        {
            var queryResult = ExecuteSqlQuery(PostgreSqlServerQueries.ForeignKeyConstraintsQuery,
                new List<IDbDataParameter>
                {
                    new NpgsqlParameter("@SchemaName", ServerConfig.SchemaName)
                });
            
            foreach (var row in queryResult)
            {
                var parTab = FindLongestPrefix(tableNames, 
                    GetDataFromRow<string>(row, PostgreSqlColumnNames.UniqueConstraintName) ?? string.Empty);
                var depTab = FindLongestPrefix(tableNames, 
                    GetDataFromRow<string>(row, PostgreSqlColumnNames.ConstraintName) ?? string.Empty);

                if (!graph.ContainsKey(parTab))
                {
                    graph[parTab] = new List<string>();
                }

                graph[parTab].Add(depTab);
                indegree[depTab]++;
            }
        }

        private static NpgsqlDbType GetNpgsqlDbType(string dataTypeStr)
        {
            dataTypeStr = dataTypeStr.ToLower();

            switch (dataTypeStr)
            {
                case "text":
                case "character varying":
                    return NpgsqlDbType.Text;
                case "integer":
                    return NpgsqlDbType.Integer;
                case "smallint":
                    return NpgsqlDbType.Smallint;
                case "bigint":
                    return NpgsqlDbType.Bigint;
                case "real":
                    return NpgsqlDbType.Real;
                case "double precision":
                    return NpgsqlDbType.Double;
                case "numeric":
                case "decimal":
                    return NpgsqlDbType.Numeric;
                case "boolean":
                    return NpgsqlDbType.Boolean;
                case "bytea":
                    return NpgsqlDbType.Bytea;
                case "timestamp":
                case "timestamp without time zone":
                case "timestamp with time zone":
                    return NpgsqlDbType.Timestamp;
                case "date":
                    return NpgsqlDbType.Date;
                case "time":
                case "time without time zone":
                case "time with time zone":
                    return NpgsqlDbType.Time;
                case "uuid":
                    return NpgsqlDbType.Uuid;
                // Add more cases for other PostgreSQL data types as needed
                default:
                    throw new ArgumentException("Unsupported PostgreSQL data type.", nameof(dataTypeStr));
            }
        }

        private int? GetLastIdForIntegerPrimaryColumn(string schemaName,
            string tableName, string primaryColumnName)
        {
            var queryBuilder = new SelectQueryBuilder(DbServerType.PostgreSql)
                .ColumnsWithAliases(new Dictionary<string, string>
                {
                    [primaryColumnName] = PostgreSqlColumnNames.ColumnName
                })
                .From($"{schemaName}.{tableName}")
                .OrderBy(new Dictionary<string, string>
                {
                    [primaryColumnName] = "DESC"
                })
                .Limit(1);

            var queryResult = ExecuteSqlQuery(queryBuilder.Build(), new List<IDbDataParameter>());
            return GetDataFromRow<int>(queryResult.FirstOrDefault(), PostgreSqlColumnNames.ColumnName);
        }

        protected override List<object?> AllPossibleValuesForReferencingColumn(string referencedTable,
            string referencedIdColumn)
        {
            var queryBuilder = new SelectQueryBuilder(DbServerType.PostgreSql)
                .ColumnsWithAliases(new Dictionary<string, string> { 
                    [referencedIdColumn] = PostgreSqlColumnNames.ColumnName
                })
                .From($"{ServerConfig.SchemaName}.{referencedTable}")
                .OrderBy(new Dictionary<string, string>
                {
                    ["RANDOM()"] = "DESC"
                })
                .Limit(100);

            var queryResult = ExecuteSqlQuery(queryBuilder.Build(), new List<IDbDataParameter>());
            return queryResult.Select(row => GetDataFromRow<object>(row, PostgreSqlColumnNames.ColumnName)).ToList();
        }

        private Dictionary<string, List<string>> GetPrimaryColumns()
        {
            var queryResult = ExecuteSqlQuery(PostgreSqlServerQueries.PrimaryColumnQuery, new List<IDbDataParameter>
            {
                new NpgsqlParameter("@SchemaName", ServerConfig.SchemaName)
            });

            var primaryColumnsMap = new Dictionary<string, List<string>>();

            foreach (var row in queryResult)
            {
                var tableName = GetDataFromRow<string>(row, PostgreSqlColumnNames.TableName);
                var primaryColumnName = GetDataFromRow<string>(row, PostgreSqlColumnNames.ColumnName);

                if (tableName == null || primaryColumnName == null)
                {
                    continue;
                }

                if (!primaryColumnsMap.ContainsKey(tableName))
                {
                    primaryColumnsMap[tableName] = new List<string>();
                }

                primaryColumnsMap[tableName].Add(primaryColumnName);
            }

            return primaryColumnsMap;
        }

        private Dictionary<string, Dictionary<string, string>> GetForeignKeyRelationshipsMap()
        {
            var queryResult = ExecuteSqlQuery(PostgreSqlServerQueries.ForeignKeyRelationshipsQuery, 
                new List<IDbDataParameter>
                {
                    new NpgsqlParameter("@SchemaName", ServerConfig.SchemaName)
                });

            var foreignKeyMap = new Dictionary<string, Dictionary<string, string>>();

            foreach(var row in queryResult)
            {
                var tableName = GetDataFromRow<string>(row, PostgreSqlColumnNames.TableName)?.Split(".")[1];
                var columnName = GetDataFromRow<string>(row, PostgreSqlColumnNames.ColumnName);
                var referencedTableName = GetDataFromRow<string>(row, PostgreSqlColumnNames.ReferencedTableName);
                var referencedColumnName = GetDataFromRow<string>(row, PostgreSqlColumnNames.ReferencedColumnName);

                if (tableName == null || columnName == null || referencedTableName == null)
                {
                    continue;
                }

                if (!foreignKeyMap.ContainsKey(tableName))
                {
                    foreignKeyMap[tableName] = new Dictionary<string, string>();
                }

                // Save the foreign key relationship information.
                foreignKeyMap[tableName].Add(columnName,
                    $"{referencedTableName.Split(".")[1]}.{referencedColumnName}");
            }

            return foreignKeyMap;
        }

        private Dictionary<string, TableInfo> PopulateTableInfoWithBasicData()
        {
            var queryResult = ExecuteSqlQuery(PostgreSqlServerQueries.ForeignKeyRelationshipsQuery,
                new List<IDbDataParameter>
                {
                    new NpgsqlParameter("@SchemaName", ServerConfig.SchemaName)
                });

            var tableInfoMap = new Dictionary<string, TableInfo>();

            foreach(var row in queryResult)
            {
                var tableName = GetDataFromRow<string>(row, PostgreSqlColumnNames.TableName);
                var columnName = GetDataFromRow<string>(row, PostgreSqlColumnNames.ColumnName);
                var dataType = GetDataFromRow<string>(row, PostgreSqlColumnNames.DataType);
                int? maxLength = GetDataFromRow<int>(row, PostgreSqlColumnNames.CharacterMaximumLength);

                if (tableName == null || columnName == null || dataType == null)
                {
                    continue;
                }

                if (!tableInfoMap.ContainsKey(tableName))
                {
                    tableInfoMap[tableName] = new TableInfo();
                }


                tableInfoMap[tableName].Columns.Add(columnName);
                tableInfoMap[tableName].ColumnTypes.Add(columnName, dataType);
                tableInfoMap[tableName].ColumnMaxLengths.Add(columnName, maxLength);
            }

            return tableInfoMap;
        }
    }
}