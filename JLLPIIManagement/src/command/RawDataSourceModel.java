package command;

import java.util.List;
import java.util.Map;

public class RawDataSourceModel {
	private String tableName;
	private String fieldName;
	private String restriction;
	private int sqlType;
	private List<Map<String, Object>> compositeKeyList;
	
	public RawDataSourceModel() {
		
	}
	
	public RawDataSourceModel(String tableName, String fieldName, String restriction, int sqlType, List<Map<String, Object>> compositeKeyList) {
		this.tableName = tableName;
		this.fieldName = fieldName;
		this.restriction = restriction;
		this.sqlType = sqlType;
		this.compositeKeyList = compositeKeyList;
	}
	
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public String getTableName() {
		return this.tableName;
	}
	
	public void setFieldName(String fieldName) {
		this.fieldName = fieldName;
	}
	public String getFieldName() {
		return this.fieldName;
	}
	
	public void setRestriction(String restriction) {
		this.restriction = restriction;
	}
	public String getRestriction() {
		return this.restriction;
	}
	
	public void setSqlType(int sqlType) {
		this.sqlType = sqlType;
	}
	public int getSqlType() {
		return this.sqlType;
	}
	
	public void setCompositeKeyList(List<Map<String, Object>> compositeKeyList) {
		this.compositeKeyList = compositeKeyList;
	}
	public List<Map<String, Object>> getCompositeKeyList() {
		return this.compositeKeyList;
	}
	
}
