# Data Generator Tool

The Data Generator Tool is a C# .NET Core console application that allows you to generate random data for SQL Server databases. It provides an interactive command-line interface to specify the config file location, which contains the database server, database name, schema name, table configurations, and the number of rows to generate data. The tool aims to abide by clean coding principles, SOLID principles, and design patterns to maintain code organization and extensibility. Currently, this tool assumes that no rows are present in any of those tables in the database, and the primary key is an integer column.

## Features

- Choose the database server (SQL Server, MySQL, PostgreSQL) for data generation.
- Input the config file location to load the database and table configurations.
- Specify the number of rows to generate for each table.
- The tool scans the database schema and retrieves all table names and their columns.
- Handles foreign key relationships and disables foreign key check constraints before data insertion.
- Generates randomized data for each column based on its data type and name.
- Ensures no duplicate entries are created in the primary key column during data generation.
- Re-enables foreign key constraints after successful data insertion.

## Limitations
- Only works with SQL Server for now
- Assumes that there is only one primary column per table
- Assumes the primary column is of type Integer.

## Requirements

- .NET Core SDK
- SQL Server, MySQL, or PostgreSQL server (depending on the chosen database)

## Installation

1. Clone the repository to your local machine.
2. Open the command prompt or terminal and navigate to the project folder.
3. Build the application using the following command:

```
dotnet build
```

## Usage

1. Prepare your `config.json` file with the necessary database and table configurations.
2. Run the application using the following command, providing the path to your `config.json` when prompted

```
dotnet run
```

## Config File (config.json)

The `config.json` file should follow the structure below:

```json
{
  "database": {
    "serverName": "YOUR_SERVER_NAME",
    "databaseName": "YOUR_DATABASE_NAME",
    "port": 1111,
    "schemaName": "YOUR_SCHEMA_NAME",
    "username": "YOUR_USERNAME",
    "password": "YOUR_PASSWORD"
  },
  "commonSettings": {
    "numberOfRows": 10000
  },
  "tableSettings": {
    "filter": {
      "mode": "Include",
      "values": ["Table1", "Table2"]
    },
    "config": [
      {
        "name": "Table1",
        "numberOfRows": 1000,
        "validValues": {
          "Column1": ["Value1", "Value2", "Value3"],
          "Column2": ["ValueA", "ValueB", "ValueC"]
        }
      },
      {
        "name": "Table2",
        "numberOfRows": 50,
        "validValues": {
          "Column1": "1-5"
        }
      }
    ]
  }
}
```

Sure! Here's a bullet point list describing the `config.json` file and how to use the `tableSettings`:

#### Config.json Structure:

- `database`: Contains the database connection details.
    - `serverName`: Replace with your SQL Server name.
    - `databaseName`: Replace with your database name.
    - `port`: Replace with your database port number (remove the property if you do not have any specific)
    - `schemaName`: Replace with your database schema name.
    - `username`: Replace with your database username.
    - `password`: Replace with your database password.

- `commonSettings`: Contains common configuration settings for data generation.
    - `numberOfRows`: Replace with the number of rows you want to generate for each table.

- `tableSettings`: Contains table-specific configurations for data generation.
    - `filter`: Specifies whether to include or exclude certain tables for data generation.
        - `mode`: Use `"Include"` or `"Exclude"` to include or exclude tables, respectively.
        - `values`: Replace with an array of table names you want to include or exclude.

- `config`: Contains individual configurations for each table.
    - Each entry represents a table's data generation settings.
    - `name`: Replace with the table name.
    - `numberOfRows`: Replace with the number of rows you want to generate for this table.
    - `validValues`: Contains column-specific valid values for randomized data generation.
        - For each column, replace the column name with an array of valid values or a range (e.g., `"1-10"`).
        - The data generator will randomly select values from these arrays for each column during data generation.

#### How to Use the `tableSettings`:

1. Prepare your `config.json` file with the necessary database and table configurations.

2. In the `"tableSettings"`, define the tables you want to include or exclude for data generation using the `"filter"`:
    - If you want to include specific tables, set `"mode": "Include"` and provide the table names in the `"values"` array.
    - If you want to exclude specific tables, set `"mode": "Exclude"` and provide the table names in the `"values"` array.

3. For each table you want to generate data for, add an entry in the `"config"` array:
    - `"name"`: Replace with the table name.
    - `"numberOfRows"`: Replace with the number of rows you want to generate for this table.

4. For each table, specify the valid values for each column in the `"validValues"` object:
    - Replace the column names with an array of valid values or a range (e.g., `"1-10"`).
    - The data generator will randomly select values from these arrays for each column during data generation.

## Extending the Tool

The tool currently supports data generation for SQL Server. To add support for other database servers (MySQL, PostgreSQL), follow these steps:

1. Create a new class that inherits from the `DataGenerator` abstract class.
2. Implement the abstract methods to handle database-specific operations (e.g., retrieving table names, column information, generating random data, disabling/enabling foreign key constraints).
3. Update the `DataGeneratorFactory` to include a new case for your database type and return the corresponding `DataGenerator` implementation.

## License

This project is licensed under the MIT License.

## Acknowledgments

The Data Generator Tool uses the [Bogus](https://github.com/bchavez/Bogus) library to generate randomized data.

## Contributors

- Aabir Hassan (aabir121@gmail.com)

## Contact

For any inquiries or support, please contact aabir121@gmail.com.

## Note

This tool is currently under development, and support for MySQL is planned for future updates. Stay tuned for more features and improvements!
