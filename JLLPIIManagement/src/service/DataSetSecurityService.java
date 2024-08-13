package service;

import java.util.List;

import command.DataSetModel;
import command.PiiDataSourceModel;

public interface DataSetSecurityService {
	
	/**
	   * To retrieve data from database
	   * returns a collection of the pii data source model
	   * @param input - data set from pii data default xml
	   * @return List of PiiDataSourceModel values
	   */
	public List<PiiDataSourceModel> retrieveDataSource(List<DataSetModel> input);
	
	/**
	   * To apply the security algorithm for the original value
	   * @param algorithm - algorithm
	   * @param dataValue - original data value
	   * @return T - target value after encryption, masking or different algorithm
	   */
	public Object applySecurityAlgorithm(String algorithm, Object dataValue, int sqlType);
	
	/**
	   * Update the target data column value with the new value after applying on data pii algorithm.
	   * @param List of PiiDataSourceModel values
	   */
	public void updatePiiDataWithAlgorithm(List<PiiDataSourceModel> piiDataSourceList);
}
