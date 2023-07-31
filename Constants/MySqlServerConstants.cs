namespace SQLDataGenerator.Constants;

public static class MySqlServerConstants
{
   public const string GetTableNamesQuery = "SHOW TABLES";

   public const string GetColumnsQuery = @"select COLUMN_NAME, DATA_TYPE, COLUMN_KEY, CHARACTER_MAXIMUM_LENGTH
                                            from information_schema.COLUMNS
                                            where TABLE_SCHEMA = @DatabaseName
                                            AND TABLE_NAME = @TableName";

   public const string GetDependencyQuery = 
       @"select TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
            from information_schema.KEY_COLUMN_USAGE
            where TABLE_SCHEMA = @DatabaseName and CONSTRAINT_NAME <> 'PRIMARY'";

   public const string EnableForeignKeyCheckQuery = "SET FOREIGN_KEY_CHECKS = 1";
   
   public const string DisableForeignKeyCheckQuery = "SET FOREIGN_KEY_CHECKS = 0";
}