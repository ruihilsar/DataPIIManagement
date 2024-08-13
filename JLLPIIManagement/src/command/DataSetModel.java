package command;

public class DataSetModel {
	
	private String tableName;
	private String fieldName;
	private String restriction;
	private boolean isKey;
	private String algorithm;
	private boolean enabled;
	
	public DataSetModel() {
		
	}
	
	public DataSetModel(String tableName, String fieldName, String restriction, boolean isKey, String algorithm, boolean enabled) {
		this.tableName = tableName;
		this.fieldName = fieldName;
		this.restriction = restriction;
		this.isKey = isKey;
		this.algorithm = algorithm;
		this.enabled = enabled;
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
	
	public void setIsKey(boolean isKey) {
		this.isKey = isKey;
	}
	public boolean getIsKey() {
		return this.isKey;
	}
	
	public void setAlgorithm(String algorithm) {
		this.algorithm = algorithm;
	}
	public String getAlgorithm() {
		return this.algorithm;
	}
	
	public void setEnabled(boolean enabled) {
		this.enabled = enabled;
	}
	public boolean getEnabled() {
		return this.enabled;
	}
}
