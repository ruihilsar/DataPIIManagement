package data.cascade.sqlserver;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import data.JdbcSQLOperator;
import data.cascade.CascadeService;

public class CascadeSqlServerService implements CascadeService {
	
	private static Logger logger = Logger.getLogger(CascadeSqlServerService.class);
	
	/**
	 * table name
	 */
	private String tableName;
	
	/**
	 * field name
	 */
	private String fieldName;
	
	/**
	 * the field value before the update
	 */
	private Object oldValue;
	
	/**
	 * the field value after the update
	 */
	private Object newValue;
	
	/**
	 * the composite key map with the format of Map<field_name, field old value>
	 */
	private Map<String, Object> compositeKeyMap;
	
	/**
	 * the shared static Map contains old composite Key pair(columns name, column old value) when creating recursive new composite key need.
	 * inner map is a LinkedHashMap that contains the composite key column and value, for example:
	 * 			1. LinkedHashMap{bl_id=HQ, fl_id=17} or
	 * 			2. LinkedHashMap{bl_id=HQ, fl_id=17, rm_id=001}
	 * outer map is a LinkedHash map are in store the reference table as a Map Key and HashSet of inner map as key value, it is based on how many tables we need to create new records in recursively for example:
	 * 			1. LinkedHashhMap{fl=list of LinkedHashMap case 1} or
	 * 			2. LinkedHashMap{rm=list of LinkedHashMap case 2}
	 */
	private static Map<String, Set<Map<String,Object>>> recursiveNewCompositeKeyMap;

	public CascadeSqlServerService() {
	}
	
    public CascadeSqlServerService(String tableName, String fieldName, Object oldValue, Object newValue, Map<String, Object> compositeKeyMap) {
    	this.tableName = tableName;
    	this.fieldName = fieldName;
    	this.oldValue = oldValue;
    	this.newValue = newValue;
    	this.compositeKeyMap = compositeKeyMap;
	}
    
    public String getTableName() {
    	return this.tableName;
    }
    
    public void setTableName(String tableName) {
    	this.tableName = tableName;
    }
    
    public String getFieldName() {
    	return this.fieldName;
    }
    
    public void setFieldName(String fieldName) {
    	this.fieldName = fieldName;
    }
    
    public Object getOldValue() {
    	return this.oldValue;
    }
    
    public void setOldValue(Object oldValue) {
    	this.oldValue = oldValue;
    }
    
    public Object getNewValue() {
    	return this.newValue;
    }
    
    public void setNewValue(Object newValue) {
    	this.newValue = newValue;
    }
    
    public Map<String, Object> getCompositeKeyMap() {
    	return this.compositeKeyMap;
    }
    
    public void setCompositeKeyMap(Map<String, Object> compositeKeyMap) {
    	this.compositeKeyMap = compositeKeyMap;
    }
    
    @Override
    /**
     * To check if the column is a primary key of the table.
     *
     * @return true: is a part of the primary key, false: is not a primary key
     */
    public boolean isPrimaryKey() {
    	
    	PreparedStatement pt = null;
		ResultSet rs = null;
    	
    	String sql = CascadeSqlServerSQLOperator.SQL_PRIMARY_KEY;
    	sql += " AND K.COLUMN_NAME = '" + this.fieldName +"' " 
    			+ "AND K.table_name = '" + this.tableName + "'";
    	
    	try {
	    	pt = JdbcSQLOperator.getPreparedStatement(sql);
			rs = JdbcSQLOperator.executeQuery(pt);
			
			if (!rs.isBeforeFirst() ) {
				System.out.println("Info: the given table and column is not a part of the primary key.");
			    return false;
			}
			else {
				System.out.println("Info: the given table and column is a part of the primary key.");
				return true;
			}
			
    	} catch(SQLException se){
			//Handle errors for JDBC
			se.printStackTrace();
		} catch(Exception e){
			//Handle errors for Class.forName
			e.printStackTrace();
		} finally{
			//finally block used to close resources
			try{
				if(rs != null) {
					rs.close();
				}
				if(pt != null) {
					pt.close();
				}
			}catch(SQLException se){
				se.printStackTrace();
			}
		}
		return false;
    }

    @Override
    /**
     * To check if the column is a foreign key of the table.
     *
     * @return true: is a part of the foreign key, false: is not a foreign key
     */
    public boolean isForeignKey() {
    	PreparedStatement pt = null;
		ResultSet rs = null;
    	
    	String sql = CascadeSqlServerSQLOperator.SQL_FOREIGN_KEY;
    	sql += " AND K.COLUMN_NAME = '" + this.fieldName +"' " 
    			+ "AND K.table_name = '" + this.tableName + "'";
    	
    	try {
	    	pt = JdbcSQLOperator.getPreparedStatement(sql);
			rs = JdbcSQLOperator.executeQuery(pt);
			
			if (!rs.isBeforeFirst()) {
				System.out.println("Info: the given table and column is not a foreign key.");
			    return false;
			}
			else {
				System.out.println("Info: the given table and column is a foreign key.");
				return true;
			}
			
    	} catch(SQLException se){
			//Handle errors for JDBC
			se.printStackTrace();
		} catch(Exception e){
			//Handle errors for Class.forName
			e.printStackTrace();
		} finally{
			//finally block used to close resources
			try{
				if(rs != null) {
					rs.close();
				}
				if(pt != null) {
					pt.close();
				}
			}catch(SQLException se){
				se.printStackTrace();
			}
		}
		return false;
    }
    
    @Override
    /**
     * return a list of current table's column(s) for the given table and column into a format of List<String>.
     * e.g. if the given primary key field is:
     * 			rm.rm_id, 
     * 		it should return the current table's column(s):
     * 			bl_id, fl_id, rm_id.
     *
     * @return List<String> list of field name(s).
     */
    public List<String> getPrimaryKeyCurrentColumns() {
    	List<String> columns = new ArrayList<String>();
    	
    	PreparedStatement pt = null;
		ResultSet rs = null;
    	
    	String sql = CascadeSqlServerSQLOperator.SQL_PRIMARY_KEY_PART_OF_COMPOSITE_KEY;
    	sql += " AND TABLE_NAME = '" + this.tableName + "' "
    		+ " ORDER BY ORDINAL_POSITION ";
    	
    	try {
	    	pt = JdbcSQLOperator.getPreparedStatement(sql);
			rs = JdbcSQLOperator.executeQuery(pt);
			
			if (!rs.isBeforeFirst()) {
				System.out.println("getPrimaryKeyCurrentColumns Error: The field column is not a primary key. this method should NOT be used in this case.");
				logger.error("getPrimaryKeyCurrentColumns Error: The field column is not a primary key. this method should NOT be used in this case.");
				return null;
			}
			
			while(rs.next()) {
				String columnName = rs.getString("COLUMN_NAME");
				if(columnName != null && !columnName.isEmpty() && !"null".equalsIgnoreCase(columnName)) {
					columns.add(columnName);
				}
			}
			
    	} catch(SQLException se){
			//Handle errors for JDBC
			se.printStackTrace();
		} catch(Exception e){
			//Handle errors for Class.forName
			e.printStackTrace();
		} finally{
			//finally block used to close resources
			try{
				if(rs != null) {
					rs.close();
				}
				if(pt != null) {
					pt.close();
				}
			}catch(SQLException se){
				se.printStackTrace();
			}
		}
    	
    	return columns;
    }
    
    @Override
    /**
     * return a list of current table's column(s) for the given table and column into a format of List<String>.
     * e.g. if the given foreign key field is:
     * 			mo_ta.from_fl_id, 
     * 		it should return the current table's column(s):
     * 			from_bl_id, from_fl_id.
     *
     * @return List<String> list of field name(s).
     */
    public List<String> getForeignKeyCurrentColumns(){
    	List<String> columns = null;
    	
    	PreparedStatement pt = null;
		ResultSet rs = null;
    	
    	String sql = CascadeSqlServerSQLOperator.SQL_REFERENCE_TABLE_AND_COLUMNS;
    	sql += " WHERE table_name = '" + this.tableName + "' " 
				+ " AND (field_names = '" + this.fieldName 
					+ "' or field_names like '" + this.fieldName 
					+ ",%' or field_names like '%," + this.fieldName 
					+ "' or field_names like '%," + this.fieldName 
					+ ",%') " 
				+ " ORDER by parent_columns ";
    	
    	try {
	    	pt = JdbcSQLOperator.getPreparedStatement(sql);
			rs = JdbcSQLOperator.executeQuery(pt);
			
			// get the first row of result
			if(rs.next()) {
				String referenceColumns = rs.getString("parent_columns");
				String fieldColumns = rs.getString("field_names");
				
				if(referenceColumns == null 
						|| fieldColumns == null 
						|| referenceColumns.isEmpty()
						|| fieldColumns.isEmpty()) {
					System.out.println("getForeignKeyCurrentColumns Error: There is something wrong for the sql statement.");
					logger.error("getForeignKeyCurrentColumns Error: There is something wrong for the sql statement.");
					return null;
				}
				
				int indexFields = findIndex(fieldColumns, this.fieldName);
				
				if (indexFields == -1) {
					System.out.println("getForeignKeyCurrentColumns Error: the column field is a foreign key, but it is not included in the field columns. " 
							+ "There is something wrong for the sql statement.");
					logger.error("getForeignKeyCurrentColumns Error: the column field is a foreign key, but it is not included in the field columns. "
							+ "There is something wrong for the sql statement.");
					return null;
				}
				
				String[] curStrArr = fieldColumns.split(",");
				
				columns = Arrays.asList(curStrArr);
			} else {
				System.out.println("Info: The field column is not a foreign key. this method should NOT be used in this case.");
				logger.info("Info: The field column is not a foreign key. this method should NOT be used in this case.");
				return null;
			}
			
    	} catch(SQLException se){
			//Handle errors for JDBC
			se.printStackTrace();
		} catch(Exception e){
			//Handle errors for Class.forName
			e.printStackTrace();
		} finally{
			//finally block used to close resources
			try{
				if(rs != null) {
					rs.close();
				}
				if(pt != null) {
					pt.close();
				}
			}catch(SQLException se){
				se.printStackTrace();
			}
		}
    	
    	return columns;
    }
    
    @Override
    /**
     * return a reference table and column(s) for the given table and column(s) into a format of Map<table_name, list of field column(s)>.
     * e.g. if the given foreign key field is:
     * 			mo_ta.from_fl_id, 
     * 		it should return the reference table and column(s):
     * 			fl.(bl_id, fl_id).
     *
     * @return Map<String, List<String>> one pair of reference table name and field name(s). the size of this return map should always be 1.
     */
    public Map<String, List<String>> getForeignKeyReferenceTableColumns(){
    	Map<String, List<String>> tableColumn = new LinkedHashMap<String, List<String>>();
    	
    	PreparedStatement pt = null;
		ResultSet rs = null;
		String referenceTable = null;
		String referenceField = null;
    	
    	String sql = CascadeSqlServerSQLOperator.SQL_REFERENCE_TABLE_AND_COLUMNS;
    	sql += " WHERE table_name = '" + this.tableName + "' " 
				+ " AND (field_names = '" + this.fieldName 
					+ "' or field_names like '" + this.fieldName 
					+ ",%' or field_names like '%," + this.fieldName 
					+ "' or field_names like '%," + this.fieldName 
					+ ",%') " 
				// Add order by to make sure the first return result is the reference table and columns.
				+ " ORDER by parent_columns ";
    	
    	try {
	    	pt = JdbcSQLOperator.getPreparedStatement(sql);
			rs = JdbcSQLOperator.executeQuery(pt);
			
			// get the first row of result
			if(rs.next()) {
				referenceTable = rs.getString("parent_table");
				String referenceColumns = rs.getString("parent_columns");
				String fieldColumns = rs.getString("field_names");
				
				if(referenceColumns == null 
						|| fieldColumns == null 
						|| referenceColumns.isEmpty()
						|| fieldColumns.isEmpty()) {
					System.out.println("getForeignKeyReferenceTableColumns Error: There is something wrong for the sql statement.");
					logger.error("getForeignKeyReferenceTableColumns Error: There is something wrong for the sql statement.");
					return null;
				}
				
				int indexFields = findIndex(fieldColumns, this.fieldName);
				
				if (indexFields == -1) {
					System.out.println("getForeignKeyReferenceTableColumns Error: the column field is a foreign key, but it is not included in the field columns. "
							+ "There is something wrong for the sql statement.");
					logger.error("getForeignKeyReferenceTableColumns Error: the column field is a foreign key, but it is not included in the field columns. "
							+ "There is something wrong for the sql statement.");
					return null;
				}
				
				String[] curStrArr = fieldColumns.split(",");
				String[] refStrArr = referenceColumns.split(",");
				
				if(curStrArr.length != refStrArr.length) {
					System.out.println("getForeignKeyReferenceTableColumns Error: the current foreign key is not matching the reference table's key. "
							+ "There is something wrong for the sql statement or database schema.");
					logger.error("getForeignKeyReferenceTableColumns Error: the current foreign key is not matching the reference table's key. "
							+ "There is something wrong for the sql statement or database schema.");
					return null;
				}
				
				referenceField = refStrArr[indexFields];
				
				if(referenceTable == null || referenceField == null) {
					System.out.println("getForeignKeyReferenceTableColumns Error: the reference table and column can not be null. "
							+ "There is something wrong for the sql statement.");
					logger.error("getForeignKeyReferenceTableColumns Error: the reference table and column can not be null. "
							+ "There is something wrong for the sql statement.");
					return null;
				}
				
				tableColumn.put(referenceTable, Arrays.asList(refStrArr));
			} else {
				System.out.println("Info: The field column is not a foreign key. this method should NOT be used in this case.");
				logger.info("Info: The field column is not a foreign key. this method should NOT be used in this case.");
				return null;
			}
			
    	} catch(SQLException se){
			//Handle errors for JDBC
			se.printStackTrace();
		} catch(Exception e){
			//Handle errors for Class.forName
			e.printStackTrace();
		} finally{
			//finally block used to close resources
			try{
				if(rs != null) {
					rs.close();
				}
				if(pt != null) {
					pt.close();
				}
			}catch(SQLException se){
				se.printStackTrace();
			}
		}
    	
    	return tableColumn;
    }
    
    @Override
    /**
     * create a new row of record in the reference table with a new primary key value.
     * e.g. if the reference table and column is:
     * 			em.em_id = "AFM"
     * 		it will insert a new record with a new value:
     * 			em.em_id = "AFMXXX"
     */
    public void createNewReferenceRecord() {
    	StringBuffer insertStatement = prepareCreateNewReferenceRecordStatement();
    	
    	if(insertStatement == null) {
    		System.out.println("createNewReferenceRecord Error: something wrong for insert statement.");
    		logger.error("createNewReferenceRecord Error: something wrong for insert statement.");
    		return;
    	}

		try {
			int execuable = JdbcSQLOperator.execute(insertStatement.toString());
			
			if(execuable > 0) {
				System.out.println("createNewReferenceRecord info: insert sql statement is executed : " + execuable);
				logger.info("createNewReferenceRecord Info: insert sql statement is executed.");
			}
					
		} catch(SQLException se){
			//Handle errors for JDBC
			se.printStackTrace();
		}
    }
    
    @Override
    /**
     * return a list of foreign keys based on the given reference table name.
     * e.g. if the reference table is fl, it should return a list of table records using fl as a foreign key
     * 		this can be one of the record in the list:
     * 			Map<mo_ta, List<from_bl_id, from_fl_id>>.
     *
     * @return List<Map<String, List<String>>> foreignKeyList
     */
    public List<Map<String, List<String>>> getForeignKeyList(){
    	
    	if(this.tableName == null 
    			|| this.fieldName == null
    			|| this.oldValue == null
    			|| this.newValue == null
    			|| this.compositeKeyMap == null) {
    		System.out.println("getForeignKeyList Error: The reference table and columns can not be null.");
    		logger.error("getForeignKeyList Error: The reference table and columns can not be null.");
    		return null;
    	}
    	
    	String referenceTable = null;
    	
    	if(isForeignKey()) {
    		Map<String, List<String>> referenceTableMap = getForeignKeyReferenceTableColumns();
        	Map.Entry<String, List<String>> entry = referenceTableMap.entrySet().iterator().next();
    		referenceTable = entry.getKey();
    	}
    	else if(isPrimaryKey()) {
    		referenceTable = this.tableName;
    	}
    	else {
    		System.out.println("getForeignKeyList Error: the input table and column is not a key, we should not implement any cascade update logic on it.");
    		logger.error("getForeignKeyList Error: the input table and column is not a key, we should not implement any cascade update logic on it.");
    		return null;
    	}
    	
		if(referenceTable == null || referenceTable.isEmpty()) {
			System.out.println("getForeignKeyList Error: The reference table name can not be null.");
			logger.error("getForeignKeyList Error: The reference table name can not be null.");
    		return null;
		}
		
		List<Map<String, List<String>>> foreignKeyList = new ArrayList<Map<String, List<String>>>();

    	PreparedStatement pt = null;
		ResultSet rs = null;
    	
    	String sql = CascadeSqlServerSQLOperator.SQL_ALL_FOREIGN_KEYS;
    	sql += " WHERE b.name='" + referenceTable + "'";
    	
    	try {
	    	pt = JdbcSQLOperator.getPreparedStatement(sql);
			rs = JdbcSQLOperator.executeQuery(pt);
			
			while(rs.next()) {
				String targetTableName = rs.getString("table_name");
				String referenceColumns = rs.getString("parent_columns");
				String fieldColumns = rs.getString("field_names");
				Map<String, List<String>> tableColumn = new LinkedHashMap<String, List<String>>();
				
				if(referenceColumns == null 
						|| fieldColumns == null 
						|| referenceColumns.isEmpty()
						|| fieldColumns.isEmpty()) {
					System.out.println("getForeignKeyList Error: There is something wrong for the sql statement.");
					logger.error("getForeignKeyList Error: There is something wrong for the sql statement.");
					return null;
				}
				
				String[] curStrArr = fieldColumns.split(",");
				String[] refStrArr = referenceColumns.split(",");
				
				if(curStrArr.length != refStrArr.length) {
					System.out.println("getForeignKeyList Error: the current foreign key is not matching the reference table's key."
							+ "There is something wrong for the sql statement or database schema.");
					logger.error("getForeignKeyList Error: the current foreign key is not matching the reference table's key. "
							+ "There is something wrong for the sql statement or database schema.");
					return null;
				}
				
				if(targetTableName == null) {
					System.out.println("getForeignKeyList Error: the reference table and column can not be null."
							+ "There is something wrong for the sql statement.");
					logger.error("getForeignKeyList Error: the reference table and column can not be null. "
							+ "There is something wrong for the sql statement.");
					return null;
				}
				
				tableColumn.put(targetTableName, Arrays.asList(curStrArr));
				foreignKeyList.add(tableColumn);
			}
			
    	} catch(SQLException se){
			//Handle errors for JDBC
			se.printStackTrace();
		} catch(Exception e){
			//Handle errors for Class.forName
			e.printStackTrace();
		} finally{
			//finally block used to close resources
			try{
				if(rs != null) {
					rs.close();
				}
				if(pt != null) {
					pt.close();
				}
			}catch(SQLException se){
				se.printStackTrace();
			}
		}
    	
    	return foreignKeyList;
    }
    
    @Override
    /**
     * To update all of the foreign key record if found.
     */
    public void updateForeignKeys() {
    	if(this.tableName == null 
    			|| this.fieldName == null
    			|| this.oldValue == null
    			|| this.newValue == null
    			|| this.compositeKeyMap == null) {
    		System.out.println("updateForeignKeys Error: The reference table and columns can not be null.");
    		logger.error("updateForeignKeys Error: The reference table and columns can not be null.");
    		return;
    	}
    	
    	List<String> recursiveTableNameList = new ArrayList<String>();
    	// initiate the global recursiveNewCompositeKeyMap
    	recursiveNewCompositeKeyMap = new LinkedHashMap<String, Set<Map<String, Object>>>();
    			
    	List<Map<String, List<String>>> foreignKeyList = getForeignKeyList();
    	for(Map<String, List<String>> foreignKeyMap : foreignKeyList) {
    		// in each loop, foreignKeyMap should have only 1 record.
    		// they are combined with <foreignKeyTableName, List of composite key columns> in the format of <String, List<String>>
    		// the order of composite key columns list should be the same order of this.compositeKeyMap
    		
        	Map.Entry<String, List<String>> entry = foreignKeyMap.entrySet().iterator().next();
    		String foreignKeyTableName = entry.getKey();
    		List<String> fieldsList = entry.getValue();
    		
    		StringBuffer whereClause = prepareWhereClause(fieldsList);
    		if(whereClause == null || whereClause.length() == 0) {
    			System.out.println("updateForeignKeys Error: The where clause can not be empty.");
    			logger.error("updateForeignKeys Error: The where clause can not be empty.");
    			return;
    		}
    		
    		if(!hasRecord(foreignKeyTableName, whereClause)) {
    			continue;
    		}
    		
    		String updatingFieldName = getMatchingFieldName(fieldsList);
    		
    		
    		// Defect here.
    		// The UPDATE statement conflicted with the FOREIGN KEY constraint "xxxxxxxxxx"
    		// the real case is below:
    		// when update work_pkg_bids.project_id, we have a constraint conflict on work_pkg_bids_work_pkg_id
    		// 				ALTER TABLE [afm].[work_pkg_bids]  WITH CHECK ADD  CONSTRAINT [work_pkg_bids_work_pkg_id] FOREIGN KEY([project_id], [work_pkg_id])
    		//				REFERENCES [afm].[work_pkgs] ([project_id], [work_pkg_id])
    		// we have to create the new records recursively on work_pkg_bids as well with the new masked project_id

    		if(recursiveNewRecordNeeded(foreignKeyTableName, updatingFieldName)) { 
    			prepareRecursiveCreateNewRecords(foreignKeyTableName, updatingFieldName, whereClause);
    		}
    		
    		// have to add here to check if updatingFieldName is part of the primary key in foreignKeyTableName
    		// if yes, add all eligible primary key records in recursiveNewCompositeKeyMap
    		// re-use method : prepareRecursiveNewCompositeKeyMap
    		if(isPartOfPrimaryKey(foreignKeyTableName, updatingFieldName)) {
    			
    			List<String> fieldNameList = getPrimaryKeyCurrentColumnsWithTableName(foreignKeyTableName);
    			
    			String fieldColumns = null;
    			for (int i = 0; i < fieldNameList.size(); i++) {
    				if(i == 0) {
    					fieldColumns = fieldNameList.get(i);
    				} else {
    					fieldColumns += "," + fieldNameList.get(i);
    				}
    			}
    			
    			Set<Map<String,Object>> recursiveCompositeKeySet = new HashSet<Map<String, Object>>();
    			recursiveCompositeKeySet = prepareRecursiveNewCompositeKeyMap(fieldColumns, foreignKeyTableName, fieldColumns, whereClause);
				
				String key = foreignKeyTableName + "|" + updatingFieldName;
				
				// push the set into the map, also need to remove the duplications.
				Set<Map<String,Object>> currentSet = recursiveNewCompositeKeyMap.get(key);
				if(currentSet == null || currentSet.isEmpty()) {
					recursiveNewCompositeKeyMap.put(key, recursiveCompositeKeySet);
				} else {
					// compare current set and recursiveCompositeKeySet to merge the same elements of the map inside
					currentSet.addAll(recursiveCompositeKeySet);
					recursiveNewCompositeKeyMap.replace(key, currentSet);
				}
    		}
    		
    	}
    	
    	// check if we need to do the recursive cascade update
    	// if we have recursive cascade update data in the map, it means we have to do the recursive create new records for the child records.
    	if(!recursiveNewCompositeKeyMap.isEmpty()) {
    		// create all child records recursively.
    		recursiveCreateNewReferenceRecord();
    		
    		// we also record the child table names into a list
    		for(Map.Entry<String, Set<Map<String,Object>>> globalMapSet : recursiveNewCompositeKeyMap.entrySet()) {
        		String targetTableAndField = globalMapSet.getKey();

        		if(targetTableAndField == null || targetTableAndField.isEmpty()) {
        			System.out.println("updateForeignKeys Error: The targetTableAndField can not be empty.");
        			logger.error("updateForeignKeys Error: The targetTableAndField can not be empty.");
            		return;
        		}
        		
        		String[] tableFieldArr = targetTableAndField.split("\\|");
        		if (tableFieldArr == null || tableFieldArr.length != 2) {
        			System.out.println("updateForeignKeys Error: The target table name and column name have something wrong.");
        			logger.error("updateForeignKeys Error: The target table name and column name have something wrong.");
            		return;
        		}
        		
        		String tableName = tableFieldArr[0];
        		String fieldName = tableFieldArr[1];
        		
        		if (tableName == null || fieldName == null || tableName.isEmpty() || fieldName.isEmpty()) {
        			System.out.println("updateForeignKeys Error: The target table name and column name can not be empty.");
        			logger.error("updateForeignKeys Error: The target table name and column name can not be empty.");
            		return;
        		}
        		
        		recursiveTableNameList.add(tableName);
    		}
    	}
    	
    	
    	for(Map<String, List<String>> foreignKeyMap : foreignKeyList) {
    		// in each loop, foreignKeyMap should have only 1 record.
    		// they are combined with <foreignKeyTableName, List of composite key columns> in the format of <String, List<String>>
    		// the order of composite key columns list should be the same order of this.compositeKeyMap
    		
        	Map.Entry<String, List<String>> entry = foreignKeyMap.entrySet().iterator().next();
    		String foreignKeyTableName = entry.getKey();
    		List<String> fieldsList = entry.getValue();
    		
    		// if the foreign key table name is included in the recursive table name list we just built,
    		// it means it is the child record, and we have already created the new records in this table in method --  recursiveCreateNewReferenceRecord.
    		// so we dont need to do any updates, jump on the next foreign table in the loop.
    		if(recursiveTableNameList.contains(foreignKeyTableName)) {
    			continue;
    		}
    		
    		StringBuffer whereClause = prepareWhereClause(fieldsList);
    		if(whereClause == null || whereClause.length() == 0) {
    			System.out.println("updateForeignKeys Error: The where clause can not be empty.");
    			logger.error("updateForeignKeys Error: The where clause can not be empty.");
    			return;
    		}
    		
    		if(!hasRecord(foreignKeyTableName, whereClause)) {
    			continue;
    		}
    		
    		String updatingFieldName = getMatchingFieldName(fieldsList);
    		
    		String sql = "UPDATE " + foreignKeyTableName 
	    				+ " SET " + updatingFieldName + " = '" + this.newValue.toString() + "' "
	    				+ " WHERE " + whereClause;
	    	
	    	try {
	    		int execuable = JdbcSQLOperator.execute(sql);
	    		
	    		if(execuable > 0) {
	    			System.out.println("updateForeignKeys Info: " + execuable + " update sql statement is executed " + foreignKeyTableName + ".");
	    			logger.info("updateForeignKeys Info: update sql statement is executed " + foreignKeyTableName + ".");
	    		}
	    				
	    	} catch(SQLException se){
	    		//Handle errors for JDBC
	    		se.printStackTrace();
	    	}
    	}	
    	
    }
    
    @Override
    /**
     * To remove the old reference table record after updating it with the new value.
     */
    public void deleteOldReferenceRecord() {
    	// Qian adds here to recursively delete the possible child records of this old reference table first.
    	if(recursiveNewCompositeKeyMap != null && !recursiveNewCompositeKeyMap.isEmpty()) {
    		// delete all child records recursively.
    		recursiveDeleteReferenceRecord();
    		
    		// assign recursiveNewCompositeKeyMap to the default value.
    		recursiveNewCompositeKeyMap = null;
    	}
    	
    	StringBuffer deleteStatement = prepareDeleteReferenceRecordStatement();
    	
    	if(deleteStatement == null) {
    		System.out.println("deleteOldReferenceRecord Error: something wrong for delete statement.");
    		logger.error("deleteOldReferenceRecord Error: something wrong for delete statement.");
    		return;
    	}

		try {
			int execuable = JdbcSQLOperator.execute(deleteStatement.toString());
			
			if(execuable > 0) {
				System.out.println("deleteOldReferenceRecord Info: delete sql statement is executed.");
				logger.info("deleteOldReferenceRecord Info: delete sql statement is executed.");
			}
					
		} catch(SQLException se){
			//Handle errors for JDBC
			se.printStackTrace();
		}
    }
    
    /**
     * prepare the insert statement.
     * @return StringBuffer insert statement.
     */
    private StringBuffer prepareCreateNewReferenceRecordStatement() {
    	if(this.tableName == null 
    			|| this.fieldName == null
    			|| this.oldValue == null
    			|| this.newValue == null
    			|| this.compositeKeyMap == null) {
    		System.out.println("prepareCreateNewReferenceRecordStatement Error: The reference table and columns can not be null.");
    		logger.error("prepareCreateNewReferenceRecordStatement Error: The reference table and columns can not be null.");
    		return null;
    	}
    	
    	String referenceTable = null;
    	List<String> referenceTableFieldsList = null;
    	
    	if(isForeignKey()) {
    		Map<String, List<String>> referenceTableMap = getForeignKeyReferenceTableColumns();
        	Map.Entry<String, List<String>> entry = referenceTableMap.entrySet().iterator().next();
    		referenceTable = entry.getKey();
    		referenceTableFieldsList = entry.getValue();
    	}
    	else if(isPrimaryKey()) {
    		referenceTable = this.tableName;
    		referenceTableFieldsList = getPrimaryKeyCurrentColumns();
    	}
    	else {
    		System.out.println("prepareCreateNewReferenceRecordStatement Error: the input table and column is not a key, we should not implement any cascade update logic on it.");
    		logger.error("prepareCreateNewReferenceRecordStatement Error: the input table and column is not a key, we should not implement any cascade update logic on it.");
    		return null;
    	}
    	
		
		if(referenceTable == null || referenceTable.isEmpty()) {
			System.out.println("prepareCreateNewReferenceRecordStatement Error: The reference table name can not be null.");
			logger.error("prepareCreateNewReferenceRecordStatement Error: The reference table name can not be null.");
    		return null;
		}
		
		String targetFieldName = getMatchingFieldName(referenceTableFieldsList);
		StringBuffer whereClause = prepareWhereClause(referenceTableFieldsList);
		List<String> fieldNameList = getAllColumns(referenceTable);
		if(targetFieldName == null || targetFieldName.isEmpty()) {
			System.out.println("prepareCreateNewReferenceRecordStatement Error: The target updating field name can not be empty.");
			logger.error("prepareCreateNewReferenceRecordStatement Error: The target updating field name can not be empty.");
			return null;
		}
		
		if(whereClause == null || whereClause.length() == 0) {
			System.out.println("prepareCreateNewReferenceRecordStatement Error: The target updating field name can not be empty.");
			logger.error("prepareCreateNewReferenceRecordStatement Error: The where clause can not be empty.");
			return null;
		}
		
		if(fieldNameList == null || fieldNameList.size() == 0) {
			System.out.println("prepareCreateNewReferenceRecordStatement Error: The fieldNameList can not be empty.");
			logger.error("prepareCreateNewReferenceRecordStatement Error: The fieldNameList can not be empty.");
			return null;
		}
		
		// ------------- prepare the insert statement ---------------
		// ----------------------------------------------------------
		StringBuffer sqlInsert = new StringBuffer();
		sqlInsert.append("INSERT INTO ");
		sqlInsert.append(referenceTable);
		sqlInsert.append(" ( ");
		
		for(int i= 0; i < fieldNameList.size(); i++) {
			sqlInsert.append(fieldNameList.get(i));
			if(i != fieldNameList.size() -1){
            	sqlInsert.append(", ");
            }
		}
		
		sqlInsert.append(" ) ");
		sqlInsert.append(" SELECT ");
		
		boolean isTargetFound = false;
		for(int i= 0; i < fieldNameList.size(); i++) {
			String currentFieldName = fieldNameList.get(i);
			
			if(targetFieldName.equalsIgnoreCase(currentFieldName)) {
				isTargetFound = true;
				sqlInsert.append(" '");
				sqlInsert.append(this.newValue.toString());
				sqlInsert.append("' AS ");
			}
			
			sqlInsert.append(currentFieldName);
			
			if(i != fieldNameList.size() -1){
            	sqlInsert.append(" , ");
            }
		}
		
		if(!isTargetFound) {
			System.out.println("prepareCreateNewReferenceRecordStatement Error: The taget updating field is not in the list.");
			logger.error("prepareCreateNewReferenceRecordStatement Error: The taget updating field is not in the list.");
			return null;
		}
		
		sqlInsert.append(" FROM ");
		sqlInsert.append(referenceTable);
		sqlInsert.append(" WHERE ");
		sqlInsert.append(whereClause);
		
		System.out.println("prepareCreateNewReferenceRecordStatement insert statement: " + sqlInsert);
		logger.info("prepareCreateNewReferenceRecordStatement insert statement: " + sqlInsert);
		// ------------------ end insert statement ------------------
		// ----------------------------------------------------------
		
		return sqlInsert;
    }
    
    
    /**
     * prepare the insert statement.
     * @param String targetTableAndField	the insert table name and target field name combination with | as delimiter .
     * @param StringBuffer whereClause
     * @return StringBuffer insert statement.
     */
    private StringBuffer prepareRecursiveCreateNewReferenceRecordStatement(String targetTableAndField, StringBuffer whereClause) {
    	if(this.tableName == null 
    			|| this.fieldName == null
    			|| this.oldValue == null
    			|| this.newValue == null
    			|| this.compositeKeyMap == null) {
    		System.out.println("prepareRecursiveCreateNewReferenceRecordStatement Error: the initial table columns info can not be null.");
    		logger.error("prepareRecursiveCreateNewReferenceRecordStatement Error: the initial table columns info can not be null.");
    		return null;
    	}
		
		if(targetTableAndField == null || targetTableAndField.isEmpty()) {
			System.out.println("prepareRecursiveCreateNewReferenceRecordStatement Error: The targetTableAndField can not be empty.");
			logger.error("prepareRecursiveCreateNewReferenceRecordStatement Error: The targetTableAndField can not be empty.");
    		return null;
		}
		
		String[] tableFieldArr = targetTableAndField.split("\\|");
		if (tableFieldArr == null || tableFieldArr.length != 2) {
			System.out.println("prepareRecursiveCreateNewReferenceRecordStatement Error: The target table name and column name have something wrong.");
			logger.error("prepareRecursiveCreateNewReferenceRecordStatement Error: The target table name and column name have something wrong.");
    		return null;
		}
		
		String tableName = tableFieldArr[0];
		String fieldName = tableFieldArr[1];
		
		if (tableName == null || fieldName == null || tableName.isEmpty() || fieldName.isEmpty()) {
			System.out.println("prepareRecursiveCreateNewReferenceRecordStatement Error: The target table name and column name can not be empty.");
			logger.error("prepareRecursiveCreateNewReferenceRecordStatement Error: The target table name and column name can not be empty.");
    		return null;
		}
		
		List<String> fieldNameList = getAllColumns(tableName);
		
		if(whereClause == null || whereClause.length() == 0) {
			System.out.println("prepareRecursiveCreateNewReferenceRecordStatement Error: The where clause can not be empty.");
			logger.error("prepareRecursiveCreateNewReferenceRecordStatement Error: The where clause can not be empty.");
			return null;
		}
		
		if(fieldNameList == null || fieldNameList.size() == 0) {
			System.out.println("prepareRecursiveCreateNewReferenceRecordStatement Error: The fieldNameList can not be empty.");
			logger.error("prepareRecursiveCreateNewReferenceRecordStatement Error: The fieldNameList can not be empty.");
			return null;
		}
		
		// ------------- prepare the insert statement ---------------
		// ----------------------------------------------------------
		StringBuffer sqlInsert = new StringBuffer();
		sqlInsert.append("INSERT INTO ");
		sqlInsert.append(tableName);
		sqlInsert.append(" ( ");
		
		for(int i= 0; i < fieldNameList.size(); i++) {
			sqlInsert.append(fieldNameList.get(i));
			if(i != fieldNameList.size() -1){
            	sqlInsert.append(", ");
            }
		}
		
		sqlInsert.append(" ) ");
		sqlInsert.append(" SELECT ");
		
		boolean isTargetFound = false;
		for(int i= 0; i < fieldNameList.size(); i++) {
			String currentFieldName = fieldNameList.get(i);
			
			if(fieldName.equalsIgnoreCase(currentFieldName)) {
				isTargetFound = true;
				sqlInsert.append(" '");
				sqlInsert.append(this.newValue.toString());
				sqlInsert.append("' AS ");
			}
			
			sqlInsert.append(currentFieldName);
			
			if(i != fieldNameList.size() -1){
            	sqlInsert.append(" , ");
            }
		}
		
		if(!isTargetFound) {
			System.out.println("prepareRecursiveCreateNewReferenceRecordStatement Error: The taget updating field is not in the list.");
			logger.error("prepareRecursiveCreateNewReferenceRecordStatement Error: The taget updating field is not in the list.");
			return null;
		}
		
		sqlInsert.append(" FROM ");
		sqlInsert.append(tableName);
		sqlInsert.append(" WHERE ");
		sqlInsert.append(whereClause);
		
		System.out.println("prepareRecursiveCreateNewReferenceRecordStatement insert statement: " + sqlInsert);
		logger.info("prepareRecursiveCreateNewReferenceRecordStatement insert statement: " + sqlInsert);
		// ------------------ end insert statement ------------------
		// ----------------------------------------------------------
		
		return sqlInsert;
    }
    
    /**
     * To recursively remove the child records of old reference table record after updating it with the new value.
     */
    private void recursiveDeleteReferenceRecord() {
    	
    	List<String> reverseOrderedKeys = new ArrayList<String>(recursiveNewCompositeKeyMap.keySet());
    	Collections.reverse(reverseOrderedKeys);
    	for (String targetTableAndField : reverseOrderedKeys) {
    		Set<Map<String, Object>> compositeKeySet = recursiveNewCompositeKeyMap.get(targetTableAndField);
    		
    		for(Map<String,Object> compositeKeyMap : compositeKeySet) {
    			
    			StringBuffer whereClause = prepareRecursiveWhereClause(compositeKeyMap);
    		
		    	StringBuffer deleteStatement = prepareRecursiveDeleteReferenceRecordStatement(targetTableAndField, whereClause);
		    	
		    	if(deleteStatement == null) {
		    		System.out.println("recursiveDeleteReferenceRecord Error: something wrong for delete statement.");
		    		logger.error("recursiveDeleteReferenceRecord Error: something wrong for delete statement.");
		    		return;
		    	}
		    	
		    	System.out.println("recursiveDeleteReferenceRecord Info: delete sql statement :" + deleteStatement.toString());
				logger.info("recursiveDeleteReferenceRecord Info: delete sql statement :" + deleteStatement.toString());

				try {
					int execuable = JdbcSQLOperator.execute(deleteStatement.toString());
					
					if(execuable > 0) {
						System.out.println("recursiveDeleteReferenceRecord Info: delete sql statement is executed.");
						logger.info("recursiveDeleteReferenceRecord Info: delete sql statement is executed.");
					}
							
				} catch(SQLException se){
					//Handle errors for JDBC
					se.printStackTrace();
				}
	    	}
    	}
    	
    }
    
    /**
     * prepare the delete statement.
     * @return StringBuffer delete statement.
     */
    private StringBuffer prepareDeleteReferenceRecordStatement() {
    	if(this.tableName == null 
    			|| this.fieldName == null
    			|| this.oldValue == null
    			|| this.newValue == null
    			|| this.compositeKeyMap == null) {
    		System.out.println("prepareDeleteReferenceRecordStatement Error: The reference table and columns can not be null.");
    		logger.error("prepareDeleteReferenceRecordStatement Error: The reference table and columns can not be null.");
    		return null;
    	}
    	
    	String referenceTable = null;
    	List<String> referenceTableFieldsList = null;
    	
    	if(isForeignKey()) {
    		Map<String, List<String>> referenceTableMap = getForeignKeyReferenceTableColumns();
        	Map.Entry<String, List<String>> entry = referenceTableMap.entrySet().iterator().next();
    		referenceTable = entry.getKey();
    		referenceTableFieldsList = entry.getValue();
    	}
    	else if(isPrimaryKey()) {
    		referenceTable = this.tableName;
    		referenceTableFieldsList = getPrimaryKeyCurrentColumns();
    	}
    	else {
    		System.out.println("prepareDeleteReferenceRecordStatement Error: the input table and column is not a key, we should not implement any cascade update logic on it.");
    		logger.error("prepareDeleteReferenceRecordStatement Error: the input table and column is not a key, we should not implement any cascade update logic on it.");
    		return null;
    	}
    	
		
		if(referenceTable == null || referenceTable.isEmpty()) {
			System.out.println("prepareDeleteReferenceRecordStatement Error: The reference table and columns can not be null.");
			logger.error("prepareDeleteReferenceRecordStatement Error: The reference table name can not be null.");
    		return null;
		}
		
		//String targetFieldName = getMatchingFieldName(referenceTableFieldsList);
		StringBuffer whereClause = prepareWhereClause(referenceTableFieldsList);
		
		StringBuffer sqlDelete = new StringBuffer();
		sqlDelete.append("DELETE FROM ");
		sqlDelete.append(referenceTable);
		sqlDelete.append(" WHERE ");
		sqlDelete.append(whereClause);
		
		System.out.println("prepareDeleteReferenceRecordStatement delete statement: " + sqlDelete);
		logger.info("prepareDeleteReferenceRecordStatement delete statement: " + sqlDelete);
		
		return sqlDelete;
    }
    

    /**
     * prepare the delete statement.
     * @param String targetTableAndField	the insert table name and target field name combination with | as delimiter .
     * @param StringBuffer whereClause
     * @return StringBuffer delete statement.
     */
    private StringBuffer prepareRecursiveDeleteReferenceRecordStatement(String targetTableAndField, StringBuffer whereClause) {
    	if(this.tableName == null 
    			|| this.fieldName == null
    			|| this.oldValue == null
    			|| this.newValue == null
    			|| this.compositeKeyMap == null) {
    		System.out.println("prepareRecursiveDeleteReferenceRecordStatement Error: The reference table and columns can not be null.");
    		logger.error("prepareRecursiveDeleteReferenceRecordStatement Error: The reference table and columns can not be null.");
    		return null;
    	}
    	
    	if(targetTableAndField == null || targetTableAndField.isEmpty()) {
			System.out.println("prepareRecursiveDeleteReferenceRecordStatement Error: The targetTableAndField can not be empty.");
			logger.error("prepareRecursiveDeleteReferenceRecordStatement Error: The targetTableAndField can not be empty.");
    		return null;
		}
		
		String[] tableFieldArr = targetTableAndField.split("\\|");
		if (tableFieldArr == null || tableFieldArr.length != 2) {
			System.out.println("prepareRecursiveDeleteReferenceRecordStatement Error: The target table name and column name have something wrong.");
			logger.error("prepareRecursiveDeleteReferenceRecordStatement Error: The target table name and column name have something wrong.");
    		return null;
		}
		
		String tableName = tableFieldArr[0];
		String fieldName = tableFieldArr[1];
		
		if (tableName == null || fieldName == null || tableName.isEmpty() || fieldName.isEmpty()) {
			System.out.println("prepareRecursiveDeleteReferenceRecordStatement Error: The target table name and column name can not be empty.");
			logger.error("prepareRecursiveDeleteReferenceRecordStatement Error: The target table name and column name can not be empty.");
    		return null;
		}
		
		if(whereClause == null || whereClause.length() == 0) {
			System.out.println("prepareRecursiveDeleteReferenceRecordStatement Error: The where clause can not be empty.");
			logger.error("prepareRecursiveDeleteReferenceRecordStatement Error: The where clause can not be empty.");
			return null;
		}
		
		StringBuffer sqlDelete = new StringBuffer();
		sqlDelete.append("DELETE FROM ");
		sqlDelete.append(tableName);
		sqlDelete.append(" WHERE ");
		sqlDelete.append(whereClause);
		
		System.out.println("prepareRecursiveDeleteReferenceRecordStatement delete statement: " + sqlDelete);
		logger.info("prepareRecursiveDeleteReferenceRecordStatement delete statement: " + sqlDelete);
		
		return sqlDelete;
    }
    
    /**
     * check if there is a record return.
     * @param String tableName
     * @param StringBuffer whereClause
     * @return boolean true or false.
     */
    private boolean hasRecord(String tableName, StringBuffer whereClause) {
    	PreparedStatement pt = null;
		ResultSet rs = null;
    	
    	String sql = "SELECT 1 FROM " + tableName + " WHERE " + whereClause.toString();
    	
    	System.out.println("hasRecord sql statement: " + sql);
		logger.info("hasRecord sql statement: " + sql);
    	
    	try {
	    	pt = JdbcSQLOperator.getPreparedStatement(sql);
			rs = JdbcSQLOperator.executeQuery(pt);
			
			if (!rs.isBeforeFirst() ) {    
			    return false;
			}
			else {
				return true;
			}
			
    	} catch(SQLException se){
			//Handle errors for JDBC
			se.printStackTrace();
		} catch(Exception e){
			//Handle errors for Class.forName
			e.printStackTrace();
		} finally{
			//finally block used to close resources
			try{
				if(rs != null) {
					rs.close();
				}
				if(pt != null) {
					pt.close();
				}
			}catch(SQLException se){
				se.printStackTrace();
			}
		}
		return false;
    }
    
    /**
     * prepare the where clause for the insert and delete statement.
     * @param List<String> fieldsList
     * @return StringBuffer	the where clause.
     */
    private StringBuffer prepareWhereClause(List<String> fieldsList) {
    	if(fieldsList.size() != this.compositeKeyMap.size()) {
    		System.out.println("prepareWhereClause Error: the current table fields are not matching the reference table fields.");
			logger.error("prepareWhereClause Error: the current table fields are not matching the reference table fields.");
    		return null;
		}
		
		StringBuffer whereClause = new StringBuffer();
		
		// the order of fields and values should match based on the previous calculation.
		int index = 0;
		for (Map.Entry<String, Object> compositeKeyEntry : this.compositeKeyMap.entrySet()) {
		    Object val = compositeKeyEntry.getValue();
		    String whereFieldName = fieldsList.get(index);
		    if(val == null) {
		    	if(index == 0) {
		    		whereClause.append(whereFieldName);
			    	whereClause.append(" IS NULL ");
		    	}
		    	else {
		    		whereClause.append(" AND ");
		    		whereClause.append(whereFieldName);
			    	whereClause.append(" IS NULL ");
		    	}
		    }
		    else {
		    	if(index == 0) {
		    		whereClause.append(" rtrim(");
		    		whereClause.append(whereFieldName);
			    	whereClause.append(") = '");
			    	whereClause.append(val.toString());
			    	whereClause.append("' ");
		    	}
		    	else {
		    		whereClause.append(" AND rtrim(");
		    		whereClause.append(whereFieldName);
			    	whereClause.append(") = '");
			    	whereClause.append(val.toString());
			    	whereClause.append("' ");
		    	}
		    }
		    
		    index++;
		}
		
		System.out.println("prepareWhereClause sql statement: " + whereClause);
		logger.info("prepareWhereClause sql statement: " + whereClause);
		
		return whereClause;
    }
    
    /**
     * prepare the where clause for the recursive cascade update.
     * @param Map<String,Object> compositeKeyMap
     * @return StringBuffer	the where clause.
     */
    private StringBuffer prepareRecursiveWhereClause(Map<String,Object> compositeKeyMap) {
    	if(compositeKeyMap == null || compositeKeyMap.isEmpty()) {
    		System.out.println("prepareRecursiveWhereClause Error: something is wrong with the current reference table's composite key building.");
			logger.error("prepareRecursiveWhereClause Error: something is wrong with the current reference table's composite key building.");
    		return null;
		}
		
		StringBuffer whereClause = new StringBuffer();
		
		int index = 0;
		for (Map.Entry<String, Object> compositeKeyEntry : compositeKeyMap.entrySet()) {
		    Object val = compositeKeyEntry.getValue();
		    String fieldName = compositeKeyEntry.getKey();
		    
		    if(val == null) {
		    	if(index == 0) {
		    		whereClause.append(fieldName);
			    	whereClause.append(" IS NULL ");
		    	}
		    	else {
		    		whereClause.append(" AND ");
		    		whereClause.append(fieldName);
			    	whereClause.append(" IS NULL ");
		    	}
		    }
		    else {
		    	if(index == 0) {
		    		whereClause.append(" rtrim(");
		    		whereClause.append(fieldName);
			    	whereClause.append(") = '");
			    	whereClause.append(val.toString());
			    	whereClause.append("' ");
		    	}
		    	else {
		    		whereClause.append(" AND rtrim(");
		    		whereClause.append(fieldName);
			    	whereClause.append(") = '");
			    	whereClause.append(val.toString());
			    	whereClause.append("' ");
		    	}
		    }
		    
		    index++;
		}
		
		System.out.println("prepareRecursiveWhereClause sql statement: " + whereClause);
		logger.info("prepareRecursiveWhereClause sql statement: " + whereClause);
		
		return whereClause;
    }
    
    /**
     * find out the matching field name from a composite key.
     * 		e.g. if mo_ta.from_fl_id matches with rm.fl_id
     * @param List<String> fieldsList
     * @return String	a field name.
     */
    private String getMatchingFieldName(List<String> fieldsList) {
    	if(fieldsList.size() != this.compositeKeyMap.size()) {
    		System.out.println("getMatchingFieldName Error: the current table fields are not matching the reference table fields.");
			logger.error("getMatchingFieldName Error: the current table fields are not matching the reference table fields.");
    		return null;
		}
		
		// the order of fields and values should match based on the previous calculation.
		int index = 0;
		for (Map.Entry<String, Object> compositeKeyEntry : this.compositeKeyMap.entrySet()) {
		    String compositeFieldName = compositeKeyEntry.getKey();
		    if(compositeFieldName.equalsIgnoreCase(this.fieldName)) {
		    	break;
		    }
		    index++;
		}
		
		if(index >= this.compositeKeyMap.size()) {
			System.out.println("getMatchingFieldName Error: the given field name is not included in the compositeKeyMap.");
			logger.error("getMatchingFieldName Error: the given field name is not included in the compositeKeyMap.");
    		return null;
		}
		
		return fieldsList.get(index);
    }
    
    /**
     * retrieve all of the column name from a table.
     * @param String tableName
     * @return List<String> 		a list of the column names.
     */
    private List<String> getAllColumns(String tableName){
    	List<String> columnsList = new ArrayList<String>();
    	PreparedStatement pt = null;
		ResultSet rs = null;
    	
    	String sql = CascadeSqlServerSQLOperator.SQL_ALL_COLUMNS;
    	sql += " WHERE TABLE_NAME = N'" + tableName + "'";
    	
    	System.out.println("getAllColumns sql statement: " + sql);
		logger.info("getAllColumns sql statement: " + sql);
    	
    	try {
	    	pt = JdbcSQLOperator.getPreparedStatement(sql);
			rs = JdbcSQLOperator.executeQuery(pt);
			
			while(rs.next()) {
				String columnName = rs.getString("COLUMN_NAME");
				columnsList.add(columnName);
			}
	    } catch(SQLException se){
			//Handle errors for JDBC
			se.printStackTrace();
		} catch(Exception e){
			//Handle errors for Class.forName
			e.printStackTrace();
		} finally{
			//finally block used to close resources
			try{
				if(rs != null) {
					rs.close();
				}
				if(pt != null) {
					pt.close();
				}
			}catch(SQLException se){
				se.printStackTrace();
			}
		}
    	
    	return columnsList;
    }
    
    /**
     * Qian adds here to create new records in the parent tables recursively when detecting the giving target column is at the non-last position of composite key.
     * @param String tableName				the current table name.
     * @param String fieldName				the current field name.
     * @return boolean value 				return if there is any record.
     */
    private boolean recursiveNewRecordNeeded(String tableName, String fieldName) {
    	PreparedStatement pt = null;
		ResultSet rs = null;
    	
		String sql = CascadeSqlServerSQLOperator.SQL_REFERENCE_TABLE_AND_COLUMNS;
    	sql += " WHERE table_name = '" + tableName + "' " 
				+ " AND (field_names like '" + fieldName 
					+ ",%' or field_names like '%," + fieldName 
					+ ",%') " 
				+ " ORDER by parent_columns ";
    	
    	try {
	    	pt = JdbcSQLOperator.getPreparedStatement(sql);
			rs = JdbcSQLOperator.executeQuery(pt);
			
			if (!rs.isBeforeFirst() ) {
				System.out.println("recursiveNewRecordNeeded Info: no need to recursively create new records.");
			    return false;
			}
			else {
				System.out.println("recursiveNewRecordNeeded Info: need to recursively create new records.");
				return true;
			}
			
    	} catch(SQLException se){
			//Handle errors for JDBC
			se.printStackTrace();
		} catch(Exception e){
			//Handle errors for Class.forName
			e.printStackTrace();
		} finally{
			//finally block used to close resources
			try{
				if(rs != null) {
					rs.close();
				}
				if(pt != null) {
					pt.close();
				}
			}catch(SQLException se){
				se.printStackTrace();
			}
		}
		return false;
    }
    
    /**
     * Qian adds here to create new records in the parent(referenceTable) tables recursively when detecting the giving target column is at the non-last position of composite key.
     * @param String tableName				the current table name.
     * @param String fieldName				the current field name.
     * @return boolean value 				return if there is any record.
     */
    private void prepareRecursiveCreateNewRecords(String tableName, String fieldName, StringBuffer whereClause) {
    	PreparedStatement pt = null;
		ResultSet rs = null;
		String referenceTable = null;
		String referenceField = null;
    	
		// the orderby will have the new records created in order.
		String sql = CascadeSqlServerSQLOperator.SQL_REFERENCE_TABLE_AND_COLUMNS;
    	sql += " WHERE table_name = '" + tableName + "' " 
				+ " AND (field_names like '" + fieldName 
					+ ",%' or field_names like '%," + fieldName 
					+ ",%') " 
				+ " ORDER by parent_columns ";
    	
    	try {
	    	pt = JdbcSQLOperator.getPreparedStatement(sql);
			rs = JdbcSQLOperator.executeQuery(pt);
			
			if (!rs.isBeforeFirst() ) {
				System.out.println("prepareRecursiveCreateNewRecords Info: no need to recursively create new records.");
				return;
			}
			
			System.out.println("******************************* START recursively create new records **********************************");
			
			while(rs.next()) {
				// the referenceTable is the target table that we need to create a new record in.
				referenceTable = rs.getString("parent_table");
				
				// referenceColumns are the composite key pair that we use for reference in the target table.
				String referenceColumns = rs.getString("parent_columns");
				
				// fieldColumns are the current table composite key columns.
				String fieldColumns = rs.getString("field_names");
				
				Set<Map<String,Object>> recursiveCompositeKeySet = new HashSet<Map<String, Object>>();
				
				if(referenceColumns == null 
						|| fieldColumns == null 
						|| referenceColumns.isEmpty()
						|| fieldColumns.isEmpty()) {
					System.out.println("prepareRecursiveCreateNewRecords Error: There is something wrong for the sql statement.");
					logger.error("prepareRecursiveCreateNewRecords Error: There is something wrong for the sql statement.");
					return;
				}
				
				int indexFields = findIndex(fieldColumns, fieldName);
				
				if (indexFields == -1) {
					System.out.println("prepareRecursiveCreateNewRecords Error: the column field is a foreign key, but it is not included in the field columns. "
							+ "There is something wrong for the sql statement.");
					logger.error("prepareRecursiveCreateNewRecords Error: the column field is a foreign key, but it is not included in the field columns. "
							+ "There is something wrong for the sql statement.");
				}
				
				String[] curStrArr = fieldColumns.split(",");
				String[] refStrArr = referenceColumns.split(",");
				
				if(curStrArr.length != refStrArr.length) {
					System.out.println("prepareRecursiveCreateNewRecords Error: the current foreign key is not matching the reference table's key. "
							+ "There is something wrong for the sql statement or database schema.");
					logger.error("prepareRecursiveCreateNewRecords Error: the current foreign key is not matching the reference table's key. "
							+ "There is something wrong for the sql statement or database schema.");
				}
				
				referenceField = refStrArr[indexFields];
				
				if(referenceTable == null || referenceField == null) {
					System.out.println("prepareRecursiveCreateNewRecords Error: the reference table and column can not be null. "
							+ "There is something wrong for the sql statement.");
					logger.error("prepareRecursiveCreateNewRecords Error: the reference table and column can not be null. "
							+ "There is something wrong for the sql statement.");
				}
				
				recursiveCompositeKeySet = prepareRecursiveNewCompositeKeyMap(referenceColumns, tableName, fieldColumns, whereClause);
				
				String key = referenceTable + "|" + referenceField;
				
				// push the set into the map, also need to remove the duplications.
				Set<Map<String,Object>> currentSet = recursiveNewCompositeKeyMap.get(key);
				if(currentSet == null || currentSet.isEmpty()) {
					recursiveNewCompositeKeyMap.put(key, recursiveCompositeKeySet);
				} else {
					// compare current set and recursiveCompositeKeySet to merge the same elements of the map inside
					currentSet.addAll(recursiveCompositeKeySet);
					recursiveNewCompositeKeyMap.replace(key, currentSet);
				}
				
			}
			
    	} catch(SQLException se){
			//Handle errors for JDBC
			se.printStackTrace();
		} catch(Exception e){
			//Handle errors for Class.forName
			e.printStackTrace();
		} finally{
			//finally block used to close resources
			try{
				if(rs != null) {
					rs.close();
				}
				if(pt != null) {
					pt.close();
				}
			}catch(SQLException se){
				se.printStackTrace();
			}
		}
    }
    
    private Set<Map<String,Object>> prepareRecursiveNewCompositeKeyMap(String parentFieldColumns, String currentTableName, String currentFieldColumns, StringBuffer whereClause) {
    	
    	PreparedStatement pt = null;
		ResultSet rs = null;
		Set<Map<String,Object>> buildCompositeKeySet = new HashSet<Map<String, Object>>();
    	
		String sql = "SELECT DISTINCT " + currentFieldColumns + " FROM " + currentTableName + " WHERE " + whereClause.toString();
    	
    	System.out.println("prepareRecursiveNewCompositeKeyMap sql statement: " + sql);
		logger.info("prepareRecursiveNewCompositeKeyMap sql statement: " + sql);
    	
    	try {

	    	pt = JdbcSQLOperator.getPreparedStatement(sql);
			rs = JdbcSQLOperator.executeQuery(pt);
			
			if (!rs.isBeforeFirst()) {
				System.out.println("prepareRecursiveNewCompositeKeyMap Error: this method should NOT be used in this case.");
				logger.error("prepareRecursiveNewCompositeKeyMap Error: this method should NOT be used in this case.");
				return null;
			}
			
			while(rs.next()) { 
				ResultSetMetaData md = rs.getMetaData();
	            Map<String, Object> buildCompositeKeyMap = new LinkedHashMap<String, Object>();
	            
		        for (int i = 1; i <= md.getColumnCount(); i++) {
		            String name = md.getColumnName(i);
//		            String type = md.getColumnClassName(i);
		            int sqlType = md.getColumnType(i);
		            Object value = rs.getObject(name);
		            
		            if((sqlType == Types.VARCHAR || sqlType == Types.CHAR) && value != null) {
						String strObj = value.toString();
						strObj = strObj.trim();
						value = (Object) strObj;
					}
		            
		            String parentFieldColumnName = parentFieldColumns.split(",")[i-1];
		            
		            buildCompositeKeyMap.put(parentFieldColumnName, value);
		        }
		        
		        buildCompositeKeySet.add(buildCompositeKeyMap);
			}
			
    	} catch(SQLException se){
			//Handle errors for JDBC
			se.printStackTrace();
		} catch(Exception e){
			//Handle errors for Class.forName
			e.printStackTrace();
		} finally{
			//finally block used to close resources
			try{
				if(rs != null) {
					rs.close();
				}
				if(pt != null) {
					pt.close();
				}
			}catch(SQLException se){
				se.printStackTrace();
			}
		}
    	
    	return buildCompositeKeySet;
    }
    
    private void recursiveCreateNewReferenceRecord() {
    	
    	for(Map.Entry<String, Set<Map<String,Object>>> globalMapSet : recursiveNewCompositeKeyMap.entrySet()) {
    		String targetTableAndField = globalMapSet.getKey();
    		Set<Map<String,Object>> compositeKeySet = globalMapSet.getValue();
    		for(Map<String,Object> compositeKeyMap : compositeKeySet) {
    			
    			StringBuffer whereClause = prepareRecursiveWhereClause(compositeKeyMap);
    		
		    	StringBuffer insertStatement = prepareRecursiveCreateNewReferenceRecordStatement(targetTableAndField, whereClause);
		    	
		    	if(insertStatement == null) {
		    		System.out.println("recursiveCreateNewReferenceRecord Error: something wrong for insert statement.");
		    		logger.error("recursiveCreateNewReferenceRecord Error: something wrong for insert statement.");
		    		return;
		    	}
		    	
		    	System.out.println("recursiveCreateNewReferenceRecord info: insert sql statement : " + insertStatement.toString());
				logger.info("recursiveCreateNewReferenceRecord Info: insert sql statement: " + insertStatement.toString());
		
				try {
					int execuable = JdbcSQLOperator.execute(insertStatement.toString());
					
					if(execuable > 0) {
						System.out.println("recursiveCreateNewReferenceRecord info: insert sql statement is executed : " + execuable);
						logger.info("recursiveCreateNewReferenceRecord Info: insert sql statement is executed.");
					}
							
				} catch(SQLException se){
					//Handle errors for JDBC
					se.printStackTrace();
				}
	    	}
    	}
    }
    
    private boolean isPartOfPrimaryKey(String tableName, String fieldName) {
    	
    	PreparedStatement pt = null;
		ResultSet rs = null;
    	
    	String sql = CascadeSqlServerSQLOperator.SQL_PRIMARY_KEY;
    	sql += " AND K.COLUMN_NAME = '" + fieldName +"' " 
    			+ "AND K.table_name = '" + tableName + "'";
    	
    	try {
	    	pt = JdbcSQLOperator.getPreparedStatement(sql);
			rs = JdbcSQLOperator.executeQuery(pt);
			
			if (!rs.isBeforeFirst() ) {
				System.out.println("isPartOfPrimaryKey Info: the given table and column is not a part of the primary key.");
			    return false;
			}
			else {
				System.out.println("isPartOfPrimaryKey Info: the given table and column is a part of the primary key.");
				return true;
			}
			
    	} catch(SQLException se){
			//Handle errors for JDBC
			se.printStackTrace();
		} catch(Exception e){
			//Handle errors for Class.forName
			e.printStackTrace();
		} finally{
			//finally block used to close resources
			try{
				if(rs != null) {
					rs.close();
				}
				if(pt != null) {
					pt.close();
				}
			}catch(SQLException se){
				se.printStackTrace();
			}
		}
		return false;
    }
    
    private List<String> getPrimaryKeyCurrentColumnsWithTableName(String tableName) {
    	List<String> columns = new ArrayList<String>();
    	
    	PreparedStatement pt = null;
		ResultSet rs = null;
    	
    	String sql = CascadeSqlServerSQLOperator.SQL_PRIMARY_KEY_PART_OF_COMPOSITE_KEY;
    	sql += " AND TABLE_NAME = '" + tableName + "' "
    		+ " ORDER BY ORDINAL_POSITION ";
    	
    	try {
	    	pt = JdbcSQLOperator.getPreparedStatement(sql);
			rs = JdbcSQLOperator.executeQuery(pt);
			
			if (!rs.isBeforeFirst()) {
				System.out.println("getPrimaryKeyCurrentColumnsWithTableName Error: The field column is not a primary key. this method should NOT be used in this case.");
				logger.error("getPrimaryKeyCurrentColumnsWithTableName Error: The field column is not a primary key. this method should NOT be used in this case.");
				return null;
			}
			
			while(rs.next()) {
				String columnName = rs.getString("COLUMN_NAME");
				if(columnName != null && !columnName.isEmpty() && !"null".equalsIgnoreCase(columnName)) {
					columns.add(columnName);
				}
			}
			
    	} catch(SQLException se){
			//Handle errors for JDBC
			se.printStackTrace();
		} catch(Exception e){
			//Handle errors for Class.forName
			e.printStackTrace();
		} finally{
			//finally block used to close resources
			try{
				if(rs != null) {
					rs.close();
				}
				if(pt != null) {
					pt.close();
				}
			}catch(SQLException se){
				se.printStackTrace();
			}
		}
    	
    	return columns;
    }
    
    /**
     * a base string with comma as delimiter.
     * @param String entireStr	the base string.
     * @param String target		the target string.
     * @return int index. 		return the index of target string.
     */
    private int findIndex(String entireStr, String target) {
    	
        String[] entireStrArr = entireStr.split(",");
//        int index = Arrays.binarySearch(entireStrArr, target);
        
        int index = -1;
        for(int i = 0; i < entireStrArr.length; i++) {
        	if(entireStrArr[i].equalsIgnoreCase(target)) {
        		index = i;
        		break;
        	}
        }
        
        return (index < 0) ? -1 : index; 
    }
}
