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
                ExecuteNonQueryCommand(PostgreSqlServerConstants.DisableForeignKeyCheckQuery,
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
                ExecuteNonQueryCommand(PostgreSqlServerConstants.EnableForeignKeyCheckQuery,
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
            var tableNames = new HashSet<string>();
            using var command = Connection.CreateCommand();
            command.CommandText = PostgreSqlServerConstants.GetTableNamesQuery;
            command.Parameters.Add(new NpgsqlParameter("@SchemaName", ServerConfig.SchemaName));

            using var reader = command.ExecuteReader();
            while (reader.Read())
            {
                tableNames.Add(reader.GetString(0));
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
            using var command = Connection.CreateCommand();
            command.CommandText = PostgreSqlServerConstants.GetForeignKeyConstraintsQuery;
            command.Parameters.Add(new NpgsqlParameter("@SchemaName", ServerConfig.SchemaName));

            using var reader = command.ExecuteReader();
            while (reader.Read())
            {
                var parTab = FindLongestPrefix(tableNames, reader.GetString(1));
                var depTab = FindLongestPrefix(tableNames, reader.GetString(0));

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
            using var command = Connection.CreateCommand();
            command.CommandText =
                $"select {primaryColumnName} from {schemaName}.{tableName} ORDER BY {primaryColumnName} DESC LIMIT 1;";

            var result = command.ExecuteScalar();
            return result == DBNull.Value ? 1 : Convert.ToInt32(result);
        }

        protected override List<object?> AllPossibleValuesForReferencingColumn(string referencedTable,
            string referencedIdColumn)
        {
            using var command = Connection.CreateCommand();
            command.CommandText =
                $"SELECT {referencedIdColumn} FROM {ServerConfig.SchemaName}.{referencedTable} ORDER BY RANDOM() LIMIT 100";

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

        private Dictionary<string, List<string>> GetPrimaryColumns()
        {
            var primaryColumnsMap = new Dictionary<string, List<string>>();
            using (var command = (NpgsqlCommand)Connection.CreateCommand())
            {
                command.CommandText = PostgreSqlServerConstants.GetPrimaryColumnQuery;
                command.Parameters.AddWithValue("@SchemaName", NpgsqlDbType.Text, ServerConfig.SchemaName);

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var tableName = reader.GetString(0);
                        var primaryColumnName = reader.GetString(1);

                        if (!primaryColumnsMap.ContainsKey(tableName))
                        {
                            primaryColumnsMap[tableName] = new List<string>();
                        }

                        primaryColumnsMap[tableName].Add(primaryColumnName);
                    }
                }
            }

            return primaryColumnsMap;
        }

        private Dictionary<string, Dictionary<string, string>> GetForeignKeyRelationshipsMap()
        {
            var foreignKeyMap = new Dictionary<string, Dictionary<string, string>>();

            // Retrieve foreign key relationships for the current table.
            using (var command = (NpgsqlCommand)Connection.CreateCommand())
            {
                command.CommandText = PostgreSqlServerConstants.GetForeignKeyRelationshipsQuery;
                command.Parameters.AddWithValue("@SchemaName", NpgsqlDbType.Text, ServerConfig.SchemaName);

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var tableName = reader.GetString(1).Split(".")[1];

                        if (!foreignKeyMap.ContainsKey(tableName))
                        {
                            foreignKeyMap[tableName] = new Dictionary<string, string>();
                        }

                        var columnName = reader.GetString(2);
                        var referencedTableName = reader.GetString(3);
                        var referencedColumnName = reader.GetString(4);

                        // Save the foreign key relationship information.
                        foreignKeyMap[tableName].Add(columnName,
                            $"{referencedTableName.Split(".")[1]}.{referencedColumnName}");
                    }
                }
            }

            return foreignKeyMap;
        }

        private Dictionary<string, TableInfo> PopulateTableInfoWithBasicData()
        {
            var tableInfoMap = new Dictionary<string, TableInfo>();

            using (var command = (NpgsqlCommand)Connection.CreateCommand())
            {
                command.CommandText = PostgreSqlServerConstants.GetTableColumnsQuery;
                command.Parameters.AddWithValue("@SchemaName", NpgsqlDbType.Text, ServerConfig.SchemaName);

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
                        int? maxLength = reader.GetValue(3) == DBNull.Value ? null : reader.GetInt32(3);

                        tableInfoMap[tableName].Columns.Add(columnName);
                        tableInfoMap[tableName].ColumnTypes.Add(columnName, dataType);
                        tableInfoMap[tableName].ColumnMaxLengths.Add(columnName, maxLength);
                    }
                }
            }

            return tableInfoMap;
        }
    }
}