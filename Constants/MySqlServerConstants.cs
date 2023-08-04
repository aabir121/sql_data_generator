namespace SQLDataGenerator.Constants;

public static class MySqlColumnNames
{
    public const string TableName = "TABLE_NAME";
    public const string ColumnName = "COLUMN_NAME";
    public const string DataType = "DATA_TYPE";
    public const string CharacterMaximumLength = "CHARACTER_MAXIMUM_LENGTH";
    public const string ReferencedTableName = "REFERENCED_TABLE_NAME";
    public const string ReferencedColumnName = "REFERENCED_COLUMN_NAME";
    public const string ColumnKey = "COLUMN_KEY";
}

public static class MySqlQueries
{
    public const string TableNamesQuery = @"SELECT table_name AS COLUMN_NAME
                                            FROM information_schema.tables
                                            WHERE table_schema = @DatabaseName";

    public const string ColumnsQuery = @"select TABLE_NAME, COLUMN_NAME, DATA_TYPE, COLUMN_KEY, CHARACTER_MAXIMUM_LENGTH
                                            from information_schema.COLUMNS
                                            where TABLE_SCHEMA = @DatabaseName";

    public const string DependencyQuery =
        @"select TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
            from information_schema.KEY_COLUMN_USAGE
            where TABLE_SCHEMA = @DatabaseName and CONSTRAINT_NAME <> 'PRIMARY'";

    public const string EnableForeignKeyCheckQuery = "SET FOREIGN_KEY_CHECKS = 1";

    public const string DisableForeignKeyCheckQuery = "SET FOREIGN_KEY_CHECKS = 0";
}