using System.Data;
using System.Text;
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

        protected override List<string> GetTableNames(IDbConnection connection)
        {
            var tableNames = GetTableNames((NpgsqlConnection)connection);
            var (graph, indegree) = InitTableGraph(tableNames);
            PopulateTableGraphAndIndegree((NpgsqlConnection)connection, tableNames, graph, indegree);

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

            var primaryColumnsMap = GetPrimaryColumns(connection);
            var foreignKeyMap = GetForeignKeyRelationshipsMap(connection);
            var tableInfoMap = PopulateTableInfoWithBasicData(connection);

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

        protected override void InsertDataIntoTable(IDbConnection connection, string tableName, TableInfo tableInfo,
            TableConfig? tableConfig)
        {
            try
            {
                // Disable foreign key constraints before inserting data.
                DisableForeignKeyCheck(connection);

                var primaryColumn = tableInfo.PrimaryColumns[0]; // Assuming the first column is the primary key column.
                if (!tableInfo.ColumnTypes.TryGetValue(primaryColumn, out var primaryDataType))
                {
                    primaryDataType = "int";
                }

                var lastRowId = primaryDataType.StartsWith("int")
                    ? GetLastIdForIntegerPrimaryColumn(connection, ServerConfig.SchemaName, tableName, primaryColumn)
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
                            $"INSERT INTO {ServerConfig.SchemaName}.{tableName} ({string.Join(", ", tableInfo.Columns)}) VALUES ");

                    for (var i = startIndex; i < endIndex; i++)
                    {
                        insertSql.Append($"({GetParamPlaceholders(tableInfo.Columns, i)}),");
                    }

                    insertSql.Length--;

                    using var command = new NpgsqlCommand(insertSql.ToString(), (NpgsqlConnection)connection);
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
                                    possibleValues = GetAllPossibleValuesForReferencingColumn(connection,
                                        ServerConfig.SchemaName,
                                        referencedTable,
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
                                    value = new Guid().ToString();
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

                            command.Parameters.AddWithValue($"@{column}{rowIndex}", GetNpgsqlDbType(dataType), value);
                        }
                    }

                    ReportProgress(batchSize, batches, batchIndex, totalRows);

                    command.ExecuteNonQuery();
                }
                
                Console.WriteLine();

                // Re-enable foreign key constraints after data insertion.
                EnableForeignKeyCheck((NpgsqlConnection)connection);

                RowsInsertedMap[tableName] = totalRows;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error occurred while generating data for Table {tableName}:");
                Console.WriteLine(ex.Message);
                Console.WriteLine(ex.StackTrace);
            }
        }

        protected override object? GenerateRandomValueBasedOnDataType(string postgresDataType, string columnName, int? maxLength)
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
                Console.WriteLine($"Error occurred while generating random value for column {columnName} with data type {postgresDataType}:");
                Console.WriteLine(ex.Message);
                Console.WriteLine(ex.StackTrace);
                return null;
            }
        }

        protected virtual void DisableForeignKeyCheck(IDbConnection connection)
        {
            try
            {
                using var command = connection.CreateCommand();
                command.CommandText = PostgreSqlServerConstants.DisableForeignKeyCheckQuery;
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
                command.CommandText = PostgreSqlServerConstants.EnableForeignKeyCheckQuery;
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

        private HashSet<string> GetTableNames(NpgsqlConnection connection)
        {
            var tableNames = new HashSet<string>();
            using var command = connection.CreateCommand();
            command.CommandText = PostgreSqlServerConstants.GetTableNamesQuery;
            command.Parameters.AddWithValue("@SchemaName", NpgsqlDbType.Text, ServerConfig.SchemaName);

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

        private void PopulateTableGraphAndIndegree(NpgsqlConnection connection, HashSet<string> tableNames,
            IDictionary<string, List<string>> graph,
            IDictionary<string, int> indegree)
        {
            using var command = connection.CreateCommand();
            command.CommandText = PostgreSqlServerConstants.GetForeignKeyConstraintsQuery;
            command.Parameters.AddWithValue("@SchemaName", NpgsqlDbType.Text, ServerConfig.SchemaName);

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

        private static int? GetLastIdForIntegerPrimaryColumn(IDbConnection connection, string schemaName,
            string tableName, string primaryColumnName)
        {
            using var command = connection.CreateCommand();
            command.CommandText =
                $"select {primaryColumnName} from {schemaName}.{tableName} ORDER BY {primaryColumnName} DESC LIMIT 1;";

            var result = command.ExecuteScalar();
            return result == DBNull.Value ? 1 : Convert.ToInt32(result);
        }

        private static List<object> GetAllPossibleValuesForReferencingColumn(IDbConnection connection,
            string schemaName,
            string referencedTable, string referencedIdColumn)
        {
            using var command = connection.CreateCommand();
            command.CommandText =
                $"SELECT {referencedIdColumn} FROM {schemaName}.{referencedTable} ORDER BY RANDOM() LIMIT 100";

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
        
                private Dictionary<string, List<string>> GetPrimaryColumns(IDbConnection connection)
        {
            var primaryColumnsMap = new Dictionary<string, List<string>>();
            using (var command = (NpgsqlCommand)connection.CreateCommand())
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

        private Dictionary<string, Dictionary<string, string>> GetForeignKeyRelationshipsMap(IDbConnection connection)
        {
            var foreignKeyMap = new Dictionary<string, Dictionary<string, string>>();
            
            // Retrieve foreign key relationships for the current table.
            using (var command = (NpgsqlCommand)connection.CreateCommand())
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

        private Dictionary<string, TableInfo> PopulateTableInfoWithBasicData(IDbConnection connection)
        {
            var tableInfoMap = new Dictionary<string, TableInfo>();
            
            using (var command = (NpgsqlCommand)connection.CreateCommand())
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
