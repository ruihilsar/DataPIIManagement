package data.cascade;

import java.util.Map;

import data.cascade.oracle.CascadeOracleService;
import data.cascade.sqlserver.CascadeSqlServerService;

public class CascadeHandler {
	private String rdbms = null;

	public CascadeHandler(String rdbms) {
		this.rdbms = rdbms;
	}
	
	public String getRdbms() {
		return this.rdbms;
	}
	
	public void setRdbms(String rdbms) {
		this.rdbms = rdbms;
	}
	
	/**
	 * To initiate a cascade update service, and triage to a different service based on the type of the database.
	 * 		now. we only implement SQL Server and Oracle database. we can add on other database services as a entry from here.
	 * @param tableName
	 * @param fieldName
	 * @param oldValue
	 * @param newValue
	 * @param compositeKeyMap
	 * @return CascadeService
	 */
	public CascadeService initCascadeHandler(String tableName, String fieldName, Object oldValue, Object newValue, Map<String, Object> compositeKeyMap) {
		if("sqlserver".equalsIgnoreCase(this.rdbms)) {
			CascadeService cascadeService = new CascadeSqlServerService(tableName, fieldName, oldValue, newValue, compositeKeyMap);
			return cascadeService;
		}
		else if("oracle".equalsIgnoreCase(this.rdbms)) {
			CascadeService cascadeService = new CascadeOracleService(tableName, fieldName, oldValue, newValue, compositeKeyMap);
			return cascadeService;
		}
		else {
			return null;
		}
	}

}
