namespace SQLDataGenerator.Constants;

public static class SqlServerConstants
{
    public const string GetTableNamesQuery =
        "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = @SchemaName";

    public const string GetTableColumnsQuery =
        @"SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
            FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = @SchemaName;";

    public const string GetForeignKeyRelationshipsQuery = 
        @"SELECT
                OBJECT_NAME(fk.parent_object_id) AS TableName,
                cpa.name AS ColumnName,
                OBJECT_NAME(fk.referenced_object_id) AS ReferencedTableName,
                cref.name AS ReferencedColumnName
            FROM
                sys.foreign_keys fk
                INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
                INNER JOIN sys.columns cpa ON fkc.parent_object_id = cpa.object_id AND fkc.parent_column_id = cpa.column_id
                INNER JOIN sys.columns cref ON fkc.referenced_object_id = cref.object_id AND fkc.referenced_column_id = cref.column_id
                INNER JOIN sys.tables tpa ON fk.parent_object_id = tpa.object_id
                INNER JOIN sys.tables tref ON fk.referenced_object_id = tref.object_id
            WHERE
                SCHEMA_NAME(tpa.schema_id) = @SchemaName
                AND SCHEMA_NAME(tref.schema_id) = @SchemaName";

    public const string GetPrimaryColumnsQuery =
        @"SELECT CCU.TABLE_NAME, CCU.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
            JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS CCU
                ON TC.CONSTRAINT_NAME = CCU.CONSTRAINT_NAME
            WHERE TC.CONSTRAINT_TYPE = 'PRIMARY KEY'
                AND TC.TABLE_SCHEMA = @SchemaName";
    
    public const string EnableForeignKeyCheckQuery = "EXEC sp_MSforeachtable @command1='ALTER TABLE ? WITH CHECK CHECK CONSTRAINT ALL'";
   
    public const string DisableForeignKeyCheckQuery = "EXEC sp_MSforeachtable @command1='ALTER TABLE ? NOCHECK CONSTRAINT ALL'";
}