namespace SQLDataGenerator.Constants;

public static class PostgreSqlServerConstants
{
    public const string GetTableNamesQuery =
        "SELECT table_name FROM information_schema.tables WHERE table_schema = @SchemaName";

    public const string GetTableColumnsQuery =
        @"SELECT table_name, column_name, data_type, character_maximum_length 
            FROM information_schema.columns 
            WHERE table_schema = @SchemaName";

    public const string GetPrimaryColumnQuery =
        @"SELECT
                table_name,
                column_name
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

    public const string GetForeignKeyRelationshipsQuery = @"
        SELECT
            conname AS ForeignKeyName,
            conrelid::regclass::text AS TableName,
            a.attname AS ColumnName,
            confrelid::regclass::text AS ReferencedTableName,
            a2.attname AS ReferencedColumnName
        FROM
            pg_constraint AS c
            JOIN pg_namespace AS ns ON c.connamespace = ns.oid
            JOIN pg_attribute AS a ON c.conrelid = a.attrelid AND a.attnum = ANY(c.conkey)
            JOIN pg_attribute AS a2 ON c.confrelid = a2.attrelid AND a2.attnum = ANY(c.confkey)
        WHERE
            ns.nspname = @SchemaName AND c.contype = 'f'";

    public const string GetForeignKeyConstraintsQuery = @"select constraint_name, unique_constraint_name
                from information_schema.referential_constraints
                where unique_constraint_schema = @SchemaName";
    
    public const string EnableForeignKeyCheckQuery = "SET session_replication_role = 'origin'";
   
    public const string DisableForeignKeyCheckQuery = "SET session_replication_role = 'replica'";
}