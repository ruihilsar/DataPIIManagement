package data.cascade.sqlserver;

public class CascadeSqlServerSQLOperator {
		
		/**
	     * To check if the table field is a primary key.
	     */
	    public static String SQL_PRIMARY_KEY = "SELECT  K.TABLE_NAME, K.COLUMN_NAME, K.CONSTRAINT_NAME "
	    				+ "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS C "
	    				+ "JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS K ON C.TABLE_NAME = K.TABLE_NAME "
	    	            + "AND C.CONSTRAINT_CATALOG = K.CONSTRAINT_CATALOG "
	    	            + "AND C.CONSTRAINT_SCHEMA = K.CONSTRAINT_SCHEMA "
	    	            + "AND C.CONSTRAINT_NAME = K.CONSTRAINT_NAME "
	    	            + "WHERE   C.CONSTRAINT_TYPE = 'PRIMARY KEY' ";
	    
	    /**
	     * To check if the table field is a foreign key.
	     */
	    public static String SQL_FOREIGN_KEY = "SELECT  K.TABLE_NAME, K.COLUMN_NAME, K.CONSTRAINT_NAME "
	    				+ "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS C "
	    				+ "JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS K ON C.TABLE_NAME = K.TABLE_NAME "
	    	            + "AND C.CONSTRAINT_CATALOG = K.CONSTRAINT_CATALOG "
	    	            + "AND C.CONSTRAINT_SCHEMA = K.CONSTRAINT_SCHEMA "
	    	            + "AND C.CONSTRAINT_NAME = K.CONSTRAINT_NAME "
	    	            + "WHERE   C.CONSTRAINT_TYPE = 'FOREIGN KEY' ";
	    
	    /**
	     * To check if the primary key is composite key.
	     */
	    public static String SQL_PRIMARY_KEY_PART_OF_COMPOSITE_KEY = "SELECT COLUMN_NAME "
	    				+ "FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE "
	    				+ "WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + QUOTENAME(CONSTRAINT_NAME)), 'IsPrimaryKey') = 1 ";
	    
	    /**
	     * To check if foreign key then get the reference/parent table (get first result).
	     */
	    public static String SQL_REFERENCE_TABLE_AND_COLUMNS = "SELECT temp.constraint_name, temp.parent_table, temp.table_name, temp.parent_columns, temp.field_names " 
	    				+ "FROM ( " 
	    					+ "SELECT " 
	    					+ "	a.name as constraint_name, " 
	    					+ "	b.name as parent_table, " 
	    					+ "	c.name as table_name, " 
	    					+ "	STUFF(( " 
	    					+ " 	SELECT ',' + c.name " 
	    					+ "		FROM sys.foreign_key_columns b " 
	    					+ "		INNER JOIN sys.columns c ON b.referenced_object_id = c.object_id " 
	    					+ "		AND b.referenced_column_id = c.column_id " 
	    					+ "		WHERE a.object_id = b.constraint_object_id " 
	    					+ "		FOR XML PATH('')), 1, 1, '') as parent_columns, " 
	    					+ "	STUFF(( " 
	    					+ "		SELECT ',' + c.name " 
	    					+ "		FROM sys.foreign_key_columns b " 
	    					+ "		INNER JOIN sys.columns c ON b.parent_object_id = c.object_id " 
	    					+ "		AND b.parent_column_id = c.column_id " 
	    					+ "		WHERE a.object_id = b.constraint_object_id " 
	    					+ "		FOR XML PATH('')), 1, 1, '') as field_names " 
	    					+ "FROM sys.foreign_keys a " 
	    					+ "INNER JOIN sys.tables b ON a.referenced_object_id = b.object_id " 
	    					+ "INNER JOIN sys.tables c ON a.parent_object_id = c.object_id "
	    				+ ") as temp ";
	    				
	    /**
	     * with the parent info, now get all foreign keys for that parent.
	     */
	    public static String SQL_ALL_FOREIGN_KEYS = "SELECT " 
	    					+ "	a.name as constraint_name, " 
	    					+ "	b.name as parent_table, " 
	    					+ "	c.name as table_name, " 
	    					+ "	STUFF(( " 
	    					+ "		SELECT ',' + c.name " 
	    					+ "		FROM sys.foreign_key_columns b " 
	    					+ "		INNER JOIN sys.columns c ON b.referenced_object_id = c.object_id " 
	    					+ "		AND b.referenced_column_id = c.column_id " 
	    					+ "		WHERE a.object_id = b.constraint_object_id " 
	    					+ "		FOR XML PATH('')), 1, 1, '') parent_columns, " 
	    					+ "	STUFF(( " 
	    					+ "		SELECT ',' + c.name " 
	    					+ "		FROM sys.foreign_key_columns b " 
	    					+ "		INNER JOIN sys.columns c ON b.parent_object_id = c.object_id " 
	    					+ "		AND b.parent_column_id = c.column_id " 
	    					+ "		WHERE a.object_id = b.constraint_object_id " 
	    					+ "		FOR XML PATH('')), 1, 1, '') field_names " 
	    					+ " FROM sys.foreign_keys a " 
	    					+ " INNER JOIN sys.tables b ON a.referenced_object_id = b.object_id " 
	    					+ " INNER JOIN sys.tables c ON a.parent_object_id = c.object_id ";
	    
	    /**
	     * query to get all of the columns.
	     */
	    public static String SQL_ALL_COLUMNS = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "; 
}
