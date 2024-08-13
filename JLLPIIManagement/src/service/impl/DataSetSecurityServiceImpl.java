package service.impl;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import algorithm.Algorithm;
import command.DataSetModel;
import command.PiiDataSourceModel;
import data.JdbcSQLOperator;
import data.cascade.CascadeHandler;
import data.cascade.CascadeService;
import service.DataSetSecurityService;

public class DataSetSecurityServiceImpl implements DataSetSecurityService{
	
	private String rdbms;
	
	private static Logger logger = Logger.getLogger(DataSetSecurityServiceImpl.class);
	
	private String getRdbms() {
		return this.rdbms;
	}
	private void setRdbms(String rdbms) {
		this.rdbms = rdbms;
	}
	
	public DataSetSecurityServiceImpl() {
		setRdbms(JdbcSQLOperator.getRDBMS());
	}

	@Override
	/**
	   * To retrieve data from database
	   * returns a collection of the pii data source model
	   * @param input - data set from pii data default xml
	   * @return List of PiiDataSourceModel values
	   */
	public List<PiiDataSourceModel> retrieveDataSource(List<DataSetModel> dataSetsList) {
		List<PiiDataSourceModel> datasourceList = new ArrayList<PiiDataSourceModel>();
		
		if(dataSetsList.isEmpty()) {
			return null;
		}
		
		PreparedStatement pt = null;
		ResultSet rs = null;
		
		for(DataSetModel dataSetModel: dataSetsList) {
			if(!dataSetModel.getEnabled()) {
				continue;
			}
			
			PiiDataSourceModel piiDataSourceModel = null;
			
			String tableName = dataSetModel.getTableName();
			String fieldName = dataSetModel.getFieldName();
			String restriction = dataSetModel.getRestriction();
			String algorithm = dataSetModel.getAlgorithm();
			boolean isCascadeUpdateNeeded = dataSetModel.getIsKey();
			
			List<String> queryColumns = new ArrayList<String>();
			
			if(isCascadeUpdateNeeded) {
				
				CascadeHandler cascadeHandler = new CascadeHandler(getRdbms());
				CascadeService cascadeService = cascadeHandler.initCascadeHandler(tableName, fieldName, null, null, null);
				
//				CascadeSqlServerService cascadeSqlServerService = new CascadeSqlServerService(tableName, fieldName, null, null, null);
				// 1. to check if the column is a foreign key(if it is both primary key and foreign key, 
				// 		we still need to check the foreign key first)
				// 2. if it is not foreign key, then check if the column is a primary key.
				if(cascadeService.isForeignKey()) {
					queryColumns = cascadeService.getForeignKeyCurrentColumns();
				} else if(cascadeService.isPrimaryKey()) {
					queryColumns = cascadeService.getPrimaryKeyCurrentColumns();
				} else {
					System.out.println("Error: The field column is not a key. We dont need the cascade update.");
					logger.error("Error: The field column is not a key. We dont need the cascade update.");
					return null;
				}
			} else {
				queryColumns.add(fieldName);
			}
			
			if(queryColumns.size() == 0) {
				System.out.println("Error: the query column can not be null. There is something wrong with the cascade update logic.");
				logger.error("Error: the query column can not be null. There is something wrong with the cascade update logic.");
				return null;
			}
			
			String selectStatement = "";
			for(int i = 0; i < queryColumns.size(); i++) {
				if(i == queryColumns.size() -1){
					selectStatement += queryColumns.get(i);
                }
                else {
                	selectStatement += queryColumns.get(i) + ", ";
                }
			}
			
			String sql = "";
			if(restriction == null || restriction.isEmpty()) {
				sql = "Select " + selectStatement + " From " + tableName;
			}
			else {
				sql = "Select " + selectStatement + " From " + tableName + " Where " + restriction;
			}
			System.out.println("Info: select query : " + sql);
			
			try {
				pt = JdbcSQLOperator.getPreparedStatement(sql);
				rs = JdbcSQLOperator.executeQuery(pt);
				
				List<Map<String, Object>> objectList = new ArrayList<Map<String, Object>>();
				boolean addToList = false;
				int targetSqlType = 0;
				
				while(rs.next()) {
					ResultSetMetaData rsmd = rs.getMetaData();
					final int columnCount = rsmd.getColumnCount();
					
					Map<String, Object> objectMap = new LinkedHashMap<String, Object>();
					addToList = false;

					for (int column = 1; column <= columnCount; column++) {
						
						Object obj = rs.getObject(column);
						String labelName = rsmd.getColumnName(column);
						int sqlType = rsmd.getColumnType(column);
						
						if(labelName == null) {
							System.out.println("Error: the column label name can not be null.");
							logger.error("Error: the column label name can not be null.");
							return null;
						}
						
						// ?? Qian comments here for suspicious issues, 
						// 1. if fieldName is not the labelName but the composite field name, we should also do the trim. Yes. resolved.
						// 2. need to test in Oracle if fieldName and label are capital or lower cases
						//    in oracle the label name is Capital. Yes tested, resolved.
						if(fieldName.equalsIgnoreCase(labelName)) {
							// we only work on the non-empty data
							if(obj == null) {
								continue;
							}
							
							targetSqlType = sqlType;
						}
						
						// Qian makes up here to trim the value object if the object is a String
						if((sqlType == Types.VARCHAR || sqlType == Types.CHAR) && obj != null) {
							String strObj = obj.toString();
							strObj = strObj.trim();
							obj = (Object) strObj;
						}
						objectMap.put(labelName, obj);
						addToList = true;
					}
					
					if (addToList) {
						objectList.add(objectMap);
					}
				}
				
				// remove the duplications from the list, we might need to use different data structure instead of Arraylist in the future.
				Set<Map<String, Object>> set = new HashSet<Map<String, Object>>(objectList);
				objectList.clear();
				objectList.addAll(set);
				
				if(!objectList.isEmpty()) {
					piiDataSourceModel = new PiiDataSourceModel(tableName, fieldName, restriction, targetSqlType, objectList, algorithm, isCascadeUpdateNeeded);
				} else {
					System.out.println("Info: There is no pii data found in " + tableName + "." + fieldName);
					logger.info("Info: There is no pii data found in " + tableName + "." + fieldName);
				}
				
				datasourceList.add(piiDataSourceModel);
						
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
				// We probably don't need to close the connection right here
//				try {
//					JdbcSQLOperator.closeConn();
//				} catch (SQLException e) {
//					e.printStackTrace();
//				}
			}
		}
		
		return datasourceList;
	}
	
	@Override
	/**
	   * To apply the security algorithm for the original value
	   * @param algorithm - algorithm
	   * @param dataValue - original data value
	   * @return T - target value after encryption, masking or different algorithm
	   */
	public Object applySecurityAlgorithm(String algorithm, Object dataValue, int sqlType){
		
		// the most common data type examples
		// https://docs.microsoft.com/en-us/sql/connect/jdbc/using-basic-data-types?view=sql-server-ver15
		// char, varchar 		--> 		java.lang.String				ok
		// datetime 			--> 		java.sql.Timestamp				ok
		// time 				--> 		java.sql.Time					no test yet
		// date					--> 		java.sql.Date					no test yet
		// float				--> 		java.lang.Double				no test yet
		// numeric(x,x) 		--> 		java.math.BigDecimal			ok
		// smallint, tinyint	-->			java.lang.Short					ok
		// int					-->			java.lang.Integer				ok
		// image				-->			java primitive type byte[]		ok
		
		if(dataValue == null) {
			return null;
		}
		
		if(sqlType == Types.VARCHAR 
				|| sqlType == Types.CHAR 
				//|| sqlType == Types.BLOB
				|| sqlType == Types.NVARCHAR 
				|| sqlType == Types.NCHAR) {
			if(dataValue instanceof java.lang.String) {
				System.out.println("Info: String algorithm class can be applied on the data.");
				logger.info("String algorithm class can be applied on the data.");
			} else {
				System.out.println("Info: The data type from the schema is not matching the data value.");
				logger.error("The data type from the schema is not matching the data value.");
				return null;
			}
		} else if(sqlType == Types.DOUBLE) {
			if(dataValue instanceof java.lang.Double) {
				System.out.println("Info: Double algorithm class can be applied on the data.");
				logger.info("Double algorithm class can be applied on the data.");
			} else {
				System.out.println("Info: The data type from the schema is not matching the data value.");
				logger.error("The data type from the schema is not matching the data value.");
				return null;
			}
		} else if(sqlType == Types.TINYINT
				|| sqlType == Types.SMALLINT) {
			if(dataValue instanceof java.lang.Short) {
				System.out.println("Info: Short algorithm class can be applied on the data.");
				logger.info("Short algorithm class can be applied on the data.");
			} else {
				System.out.println("Info: The data type from the schema is not matching the data value.");
				logger.error("The data type from the schema is not matching the data value.");
				return null;
			}
		} else if(sqlType == Types.DECIMAL
				|| sqlType == Types.NUMERIC) {
			if(dataValue instanceof java.math.BigDecimal) {
				System.out.println("Info: BigDecimal algorithm class can be applied on the data.");
				logger.info("BigDecimal algorithm class can be applied on the data.");
			} else {
				System.out.println("Info: The data type from the schema is not matching the data value.");
				logger.error("The data type from the schema is not matching the data value.");
				return null;
			}
		} else if(sqlType == Types.DATE) {
			if(dataValue instanceof java.sql.Date) {
				System.out.println("Info: Date algorithm class can be applied on the data.");
				logger.info("Date algorithm class can be applied on the data.");
			} else {
				System.out.println("Info: The data type from the schema is not matching the data value.");
				logger.error("The data type from the schema is not matching the data value.");
				return null;
			}
		} else if(sqlType == Types.TIMESTAMP) {
			if(dataValue instanceof java.sql.Timestamp) {
				System.out.println("Info: Timestamp algorithm class can be applied on the data.");
				logger.info("Timestamp algorithm class can be applied on the data.");
			} else {
				System.out.println("Info: The data type from the schema is not matching the data value.");
				logger.error("The data type from the schema is not matching the data value.");
				return null;
			}
		} else if(sqlType == Types.TIME) {
			if(dataValue instanceof java.sql.Time) {
				System.out.println("Info: Time algorithm class can be applied on the data.");
				logger.info("Time algorithm class can be applied on the data.");
			} else {
				System.out.println("Info: The data type from the schema is not matching the data value.");
				logger.error("The data type from the schema is not matching the data value.");
				return null;
			}
		} else if(sqlType == Types.INTEGER) {
			if(dataValue instanceof java.lang.Integer) {
				System.out.println("Info: Time algorithm class can be applied on the data.");
				logger.info("Integer algorithm class can be applied on the data.");
			} else {
				System.out.println("Info: The data type from the schema is not matching the data value.");
				logger.error("The data type from the schema is not matching the data value.");
				return null;
			}
		} else if(sqlType == Types.LONGVARBINARY) {
			if(dataValue instanceof byte[]) {
				System.out.println("Info: byte array algorithm class can be applied on the data.");
				logger.info("byte array algorithm class can be applied on the data.");
			} else {
				System.out.println("Info: The data type from the schema is not matching the data value.");
				logger.error("The data type from the schema is not matching the data value.");
				return null;
			}
		} else {
			System.out.println("Error: No algorithm is not prepared for the data type of the value.");
			logger.error("No algorithm is not prepared for the data type of the value.");
			return null;
		}
		
		Class<?> klass = null;
		Algorithm myAlgorithm = null;
		try {
			klass = Thread.currentThread().getContextClassLoader().loadClass(algorithm);
			myAlgorithm = (Algorithm)klass.newInstance();
		} catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
			System.out.println("Error: the algorithm class cannot be loaded.");
			logger.error("the algorithm class cannot be loaded.");
		}
		
		return myAlgorithm.init(dataValue);
		
	}
	
	@Override
	/**
	   * Update the target data column value with the new value after applying on data pii algorithm.
	   * @param List of PiiDataSourceModel values
	   */
	public void updatePiiDataWithAlgorithm(List<PiiDataSourceModel> piiDataSourceList) {
		for(PiiDataSourceModel piiDataSourceModel : piiDataSourceList) {
			String tableName = piiDataSourceModel.getTableName();
			String fieldName = piiDataSourceModel.getFieldName();
			String algorithm = piiDataSourceModel.getAlgorithm();
			String restriction = piiDataSourceModel.getRestriction();
			int sqlType = piiDataSourceModel.getSqlType();
			
			boolean isCascadedUpdateNeeded = piiDataSourceModel.getIsCascadeUpdateNeeded();
			
			List<Map<String, Object>> updatingList = piiDataSourceModel.getCompositeKeyList();
			for(Map<String, Object> updateMap : updatingList) {
				
				Object originalValue = null;
				Object updatedValue = null;
				for (Map.Entry<String, Object> entry : updateMap.entrySet()) {
		            String columnName = entry.getKey();
		            Object value = entry.getValue();
		            
		            if(columnName != null && columnName.equalsIgnoreCase(fieldName)) {
		            	if(value == null) {
		            		break;
		            	}
		            	originalValue = value;
		            	
		            	// Qian comments here
						// the idea is to parse T value to String type and update back into database.
						// However, here we might need to think about and manually enumerate all of the possible data type returned from T updatedValue
						// and update the value back to the database in the correct way of writing sql statement.
						// T value can be
						// * String (implemented)
						// * Double (implemented)
						// * Integer (implemented)
						// * Date (implemented)
						// * Decimal (implemented)
						// * Time (not implemented yet)
						// * Timestamp (not implemented yet)
		            	updatedValue = applySecurityAlgorithm(algorithm, value, sqlType);
		            	break;
		            }
				}
				
				// if the original value of the column is null, then we dont work on any Data Pii algorithm implement on it.
				if(originalValue == null) {
					break;
				}
				
				// if the original value of the column is not null, but the updated value after the algorithm implement is null.
				// there is most likely something wrong with the algorithm and it is not recommended.
				if(updatedValue == null) {
					System.out.println("Error: updated column value is not recommended to be null. possible error in the algorithm method.");
					logger.error("Logic Error: updated column value is not recommended to be null. possible error in the algorithm method.");
					break;
				}
				
				if(isCascadedUpdateNeeded) {
					CascadeHandler cascadeHandler = new CascadeHandler(getRdbms());
					CascadeService cascadeService = cascadeHandler.initCascadeHandler(tableName, fieldName, originalValue, updatedValue, updateMap);
					
//					CascadeSqlServerService cascadeSqlServerService = new CascadeSqlServerService(tableName, fieldName, originalValue, updatedValue, updateMap);
					
					// cascade update in the following steps
					// 1. create a brand new record on the root reference table with a new value on the given column.
					cascadeService.createNewReferenceRecord();
					
					// 2. update all of the foreign keys with the new record including the given table and column name.
					cascadeService.updateForeignKeys();
					
					// 3. remove the old record.
					cascadeService.deleteOldReferenceRecord();
				}
				else {
					String sql = "Update " + tableName 
							+ " Set " + fieldName + " = '" + updatedValue.toString() + "'" 
							+ " Where " + fieldName + " = '" + originalValue + "'";
					
					if (restriction != null) {
						sql += " AND " + restriction;
					}
					
					try {
						int execuable = JdbcSQLOperator.execute(sql);
						
						if(execuable > 0) {
							System.out.println("Info: update sql statement is executed : " + execuable);
							logger.info("Info: update sql statement is executed.");
						}
								
					} catch(SQLException se){
						//Handle errors for JDBC
						se.printStackTrace();
					} finally{
						try {
							JdbcSQLOperator.closeConn();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}
				}
			}
			// outside end updating list
		}
		
		System.out.println("finish updating all data set.");
		// outside end all pii data list
	}

}
