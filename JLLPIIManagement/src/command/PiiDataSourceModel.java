package command;

import java.util.List;
import java.util.Map;

public class PiiDataSourceModel extends RawDataSourceModel {
	private String algorithm;
	private boolean isCascadeUpdateNeeded;

	public PiiDataSourceModel() {
	}
	
	public PiiDataSourceModel(String tableName, String fieldName, String restriction, int sqlType, List<Map<String, Object>> compositeKeyList, String algorithm, boolean isCascadeUpdateNeeded) {
		super(tableName, fieldName, restriction, sqlType, compositeKeyList);
		this.algorithm = algorithm;
		this.isCascadeUpdateNeeded = isCascadeUpdateNeeded;
	}
	
	public void setAlgorithm(String algorithm) {
		this.algorithm = algorithm;
	}
	public String getAlgorithm() {
		return this.algorithm;
	}
	
	public void setAlgorithm(boolean isCascadeUpdateNeeded) {
		this.isCascadeUpdateNeeded = isCascadeUpdateNeeded;
	}
	public boolean getIsCascadeUpdateNeeded() {
		return this.isCascadeUpdateNeeded;
	}

}
