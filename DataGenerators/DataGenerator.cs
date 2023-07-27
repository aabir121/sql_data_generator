using System.Data;
using System.Text.RegularExpressions;
using Bogus;
using Microsoft.Data.SqlClient;
using SQLDataGenerator.Models;
using SQLDataGenerator.Models.Config;

namespace SQLDataGenerator.DataGenerators
{
    public abstract class DataGenerator
    {
        protected readonly ServerConfiguration ServerConfig;
        protected readonly CommonSettings CommonSettings;
        protected readonly TableSettings TableSettings;

        private readonly Faker _faker;

        protected DataGenerator(Configuration config)
        {
            ServerConfig = config.ServerConfiguration;
            CommonSettings = config.CommonSettings;
            TableSettings = config.TableSettings;

            _faker = new Faker();
        }

        public void GenerateData()
        {
            try
            {
                // Connect to the database server using the provided credentials.
                using var connection = GetDbConnection();
                connection.Open();
                Console.WriteLine("Connected to the database server.");

                // Retrieve all table names from the selected schema.
                var tableNames = GetTableNames(connection);

                // Get column information and foreign key relationships for each table.
                var tableData = GetTableData(connection, tableNames);

                var tableConfigsMap = new Dictionary<string, TableConfig>();
                foreach(var c in TableSettings.Config)
                {
                    tableConfigsMap[c.Name] = c;
                }

                // Generate and insert data into each table.
                var tableNamesToWorkWith = FilterBasedOnSettings(tableNames);
                foreach (var tableName in tableNamesToWorkWith)
                {
                    var tableInfo = tableData[tableName];
                    InsertDataIntoTable(connection, tableName, tableInfo, tableConfigsMap.TryGetValue(tableName, out var config) ? config : null);
                }

                // Show a message indicating successful data generation.
                Console.WriteLine("Data generation completed.");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error occurred during data generation:");
                Console.WriteLine(ex.Message);
                Console.WriteLine(ex.StackTrace);
            }
        }

        private List<string> FilterBasedOnSettings(List<string> tableNames)
        {
            if (TableSettings.TableFilter == null)
            {
                return tableNames;
            }

            var values = TableSettings.TableFilter.Values;
            if (TableSettings.TableFilter.FilterMode == FilterMode.Include)
            {
                return tableNames.Where((x) => values.Contains(x)).ToList();
            }
            else
            {
                return tableNames.Where((x) => !values.Contains(x)).ToList();
            }
        }

        protected abstract IDbConnection GetDbConnection();

        protected abstract List<string> GetTableNames(IDbConnection connection);

        protected abstract Dictionary<string, TableInfo> GetTableData(IDbConnection connection, List<string> tableNames);

        protected abstract void InsertDataIntoTable(IDbConnection connection, string tableName, TableInfo tableInfo, TableConfig? tableConfig);

        protected abstract void DisableForeignKeyCheck(SqlConnection connection);

        protected abstract void EnableForeignKeyCheck(SqlConnection connection);

        protected string GetParamPlaceholders(List<string> columns, int rowIdx)
        {
            var placeholders = columns.Select(t => $"@{t}{rowIdx}").ToList();
            return string.Join(", ", placeholders);
        }

        protected object? GenerateRandomValueForDataType(string dataType, string columnName,
            List<object>? tableConfigValidValues)
        {
            if (tableConfigValidValues != null)
            {
                return _faker.PickRandom(tableConfigValidValues);
            }

            dataType = dataType.ToLower();

            // Use Faker to generate random data based on column type and name.
            switch (dataType)
            {
                case "nvarchar":
                case "varchar":
                case "text":
                    if (Regex.IsMatch(columnName, @"\b(?:name|fullname)\b", RegexOptions.IgnoreCase))
                    {
                        return _faker.Name.FullName();
                    }
                    if (Regex.IsMatch(columnName, @"\b(?:email)\b", RegexOptions.IgnoreCase))
                    {
                        return _faker.Internet.Email();
                    }
                    if (Regex.IsMatch(columnName, @"\b(?:address)\b", RegexOptions.IgnoreCase))
                    {
                        return _faker.Address.FullAddress();
                    }
                    if (Regex.IsMatch(columnName, @"\b(?:phone)\b", RegexOptions.IgnoreCase))
                    {
                        return _faker.Phone.PhoneNumber();
                    }
                    if (Regex.IsMatch(columnName, @"\b(?:password)\b", RegexOptions.IgnoreCase))
                    {
                        return _faker.Internet.Password();
                    }
                    if (Regex.IsMatch(columnName, @"\b(?:picture)\b", RegexOptions.IgnoreCase))
                    {
                        return _faker.Image.LoremFlickrUrl();
                    }
                    if (Regex.IsMatch(columnName, @"\b(?:url)\b", RegexOptions.IgnoreCase))
                    {
                        return _faker.Internet.Url();
                    }
                    if (Regex.IsMatch(columnName, @"\b(?:price)\b", RegexOptions.IgnoreCase))
                    {
                        return _faker.Commerce.Price();
                    }
                    if (Regex.IsMatch(columnName, @"\b(?:review)\b", RegexOptions.IgnoreCase))
                    {
                        return _faker.Rant.Review();
                    }
                    if (Regex.IsMatch(columnName, @"\b(?:country)\b", RegexOptions.IgnoreCase))
                    {
                        return _faker.Address.Country();
                    }
                    if (Regex.IsMatch(columnName, @"\b(?:city)\b", RegexOptions.IgnoreCase))
                    {
                        return _faker.Address.City();
                    }
                    if (Regex.IsMatch(columnName, @"\b(?:zipcode)\b", RegexOptions.IgnoreCase))
                    {
                        return _faker.Address.ZipCode();
                    }
                    if (Regex.IsMatch(columnName, @"\b(?:message)\b", RegexOptions.IgnoreCase))
                    {
                        return _faker.Lorem.Text();
                    }

                    return Regex.IsMatch(columnName, @"\b(?:description)\b", RegexOptions.IgnoreCase) ? _faker.Random.Words() : _faker.Lorem.Word();
                case "int":
                case "bigint":
                case "smallint":
                case "tinyint":
                    return _faker.Random.Number(1, 100);

                case "float":
                case "real":
                case "decimal":
                case "numeric":
                    return _faker.Random.Decimal(1, 100);

                case "bit":
                    return _faker.Random.Bool();

                case "date":
                case "datetime":
                case "datetime2":
                    return _faker.Date.Past();

                // Add more cases to handle other data types
                // For custom data types, you might need to implement your own logic.

                default:
                    return null;
            }
        }
    }
}
