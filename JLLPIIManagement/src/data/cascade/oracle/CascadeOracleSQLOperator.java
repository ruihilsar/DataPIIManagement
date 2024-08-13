package data.cascade.oracle;

public class CascadeOracleSQLOperator {
		
		/**
	     * To check if the table field is a primary key.
	     */
	    public static String SQL_PRIMARY_KEY = "SELECT cols.table_name, cols.column_name, cols.position, cons.status, cons.owner, cons.constraint_type "
	    		+ "FROM all_constraints cons, all_cons_columns cols "
	    		+ "WHERE cons.constraint_name = cols.constraint_name "
	    		+ "AND cons.owner = cols.owner "
	    		+ "AND cons.constraint_type = 'P' ";
	    
	    /**
	     * To check if the table field is a foreign key.
	     */
	    public static String SQL_FOREIGN_KEY = "SELECT cols.table_name, cols.column_name, cols.position, cons.status, cons.owner, cons.constraint_type "
				+ "FROM all_constraints cons, all_cons_columns cols "
				+ "WHERE cons.constraint_name = cols.constraint_name "
				+ "AND cons.owner = cols.owner "
				+ "AND cons.constraint_type = 'R' ";
	    
	    /**
	     * To check if foreign key then get the current table's foreign key by given column (get first result).
	     */
	    public static String SQL_CURRENT_TABLE_FK_COLUMNS = "SELECT temp.constraint_name, temp.table_name, temp.key_columns "
	    		+ "FROM ( "
	            + "SELECT a.constraint_name, a.table_name, listagg(a.column_name,',') within group( order by  a.position ) key_columns "
	            + "FROM user_cons_columns a "
	            + "JOIN user_constraints c ON a.owner = c.owner AND a.constraint_name = c.constraint_name "
	            + "WHERE c.constraint_type = 'R' "
	            + "GROUP BY a.constraint_name, a.table_name "
	            + ") temp ";
	    
	    /**
	     * To check if foreign key then get the reference table name and columns' names (get first result).
	     */
	    public static String SQL_REFERENCE_TABLE_AND_COLUMNS = "SELECT x.constraint_name, x.table_name current_table, x.key_columns current_columns, y.table_name foreign_table, y.key_columns foreign_columns " 
	    		+ "FROM " 
	    		+ "(    " 
	    		+ "    SELECT a.constraint_name, a.table_name, listagg(a.column_name,',') within group( order by a.position ) key_columns " 
	    		+ "    FROM user_cons_columns a " 
	    		+ "    JOIN user_constraints c ON a.owner = c.owner AND a.constraint_name = c.constraint_name " 
	    		+ "    WHERE c.constraint_type = 'R' " 
	    		+ "    GROUP BY a.constraint_name, a.table_name " 
	    		+ ") x " 
	    		+ "LEFT OUTER JOIN  " 
	    		+ "(    " 
	    		+ "	SELECT a.constraint_name, b.table_name, listagg(b.column_name,',') within group( order by b.position ) key_columns " 
	    		+ "    FROM user_cons_columns a " 
	    		+ "    JOIN user_constraints c ON a.owner = c.owner AND a.constraint_name = c.constraint_name " 
	    		+ "	   JOIN user_constraints c_pk ON c.r_owner = c_pk.owner AND c.r_constraint_name = c_pk.constraint_name " 
	    		+ "    JOIN user_cons_columns b ON C_PK.owner = b.owner AND C_PK.CONSTRAINT_NAME = b.constraint_name AND b.POSITION = a.POSITION " 
	    		+ "    WHERE c.constraint_type = 'R' " 
	    		+ "    GROUP BY a.constraint_name, b.table_name " 
	    		+ ") y ON x.constraint_name = y.constraint_name ";
	    
	    /**
	     * query to get all of the columns.
	     */
	    public static String SQL_ALL_COLUMNS = "SELECT col.column_id, col.owner as schema_name, col.table_name, col.column_name " 
	    		+ "FROM sys.all_tab_columns col " 
	    		+ "INNER JOIN sys.all_tables t ON col.owner = t.owner AND col.table_name = t.table_name "; 
}
