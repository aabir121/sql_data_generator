namespace SQLDataGenerator.Constants;

public static class PostgreSqlColumnNames
{
    public const string TableName = "TABLE_NAME";
    public const string ColumnName = "COLUMN_NAME";
    public const string DataType = "DATA_TYPE";
    public const string CharacterMaximumLength = "CHARACTER_MAXIMUM_LENGTH";
    public const string ReferencedTableName = "REFERENCED_TABLE_NAME";
    public const string ReferencedColumnName = "REFERENCED_COLUMN_NAME";
    public const string ConstraintName = "CONSTRAINT_NAME";
    public const string UniqueConstraintName = "UNIQUE_CONSTRAINT_NAME";
}
public static class PostgreSqlServerQueries
{
    public const string TableNamesQuery =
        @"SELECT table_name as TABLE_NAME 
            FROM information_schema.tables 
            WHERE table_schema = @SchemaName";

    public const string TableColumnsQuery =
        @"SELECT table_name AS TABLE_NAME, column_name AS COLUMN_NAME, data_type AS DATA_TYPE,
                character_maximum_length AS CHARACTER_MAXIMUM_LENGTH 
            FROM information_schema.columns 
            WHERE table_schema = @SchemaName";

    public const string PrimaryColumnQuery =
        @"SELECT
                table_name AS TABLE_NAME,
                column_name AS COLUMN_NAME
            FROM
                information_schema.key_column_usage
            WHERE
                constraint_name IN (
                    SELECT
                        constraint_name
                    FROM
                        information_schema.table_constraints
                    WHERE
                        table_schema = @SchemaName
                        AND constraint_type = 'PRIMARY KEY'
                )";

    public const string ForeignKeyRelationshipsQuery = @"
        SELECT
            conrelid::regclass::text AS TABLE_NAME,
            a.attname AS COLUMN_NAME,
            confrelid::regclass::text AS REFERENCED_TABLE_NAME,
            a2.attname AS REFERENCED_COLUMN_NAME
        FROM
            pg_constraint AS c
            JOIN pg_namespace AS ns ON c.connamespace = ns.oid
            JOIN pg_attribute AS a ON c.conrelid = a.attrelid AND a.attnum = ANY(c.conkey)
            JOIN pg_attribute AS a2 ON c.confrelid = a2.attrelid AND a2.attnum = ANY(c.confkey)
        WHERE
            ns.nspname = @SchemaName AND c.contype = 'f'";

    public const string ForeignKeyConstraintsQuery = 
        @"select constraint_name AS CONSTRAINT_NAME, 
            unique_constraint_name AS UNIQUE_CONSTRAINT_NAME
        from information_schema.referential_constraints
        where unique_constraint_schema = @SchemaName";
    
    public const string EnableForeignKeyCheckQuery = "SET session_replication_role = 'origin'";
   
    public const string DisableForeignKeyCheckQuery = "SET session_replication_role = 'replica'";
}