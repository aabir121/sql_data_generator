# Data Generator Tool

The Data Generator Tool is a C# .NET Core console application that allows you to generate random data for SQL Server databases. It provides an interactive command-line interface to specify the database server, database name, schema name, table names, and the number of rows to generate data. The tool aims to abide by clean coding principles, SOLID principles, and design patterns to maintain code organization and extensibility. Currently this tool assumes that no rows are present in any of those tables in the database and the primary key is an integer column.

## Features

- Choose the database server (SQL Server, MySQL, PostgreSQL) for data generation.
- Input server name, database name, schema name, username, and password for database connection.
- Specify the number of rows to generate for each table.
- The tool scans the database schema and retrieves all table names and their columns.
- Handles foreign key relationships and disables foreign key check constraints before data insertion.
- Generates randomized data for each column based on its data type and name.
- Ensures no duplicate entries are created in the primary key column during data generation.
- Re-enables foreign key constraints after successful data insertion.

## Requirements

- .NET Core SDK
- SQL Server, MySQL, or PostgreSQL server (depending on the chosen database)

## Installation

1. Clone the repository to your local machine.
2. Open the command prompt or terminal and navigate to the project folder.
3. Build the application using the following command:

```bash
dotnet build
```

## Usage

1. Run the application using the following command:

```bash
dotnet run
```

2. Choose the database server type (1 for SQL Server, 2 for MySQL, 3 for PostgreSQL) and provide the necessary connection information.
3. Input the number of rows to generate for each table in the specified schema.

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

This tool is currently under development, and support for MySQL and PostgreSQL is planned for future updates. Stay tuned for more features and improvements!
