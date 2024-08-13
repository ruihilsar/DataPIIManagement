import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

import command.DataSetModel;
import command.PiiDataSourceModel;
import data.PiiDataDefaultFactory;
import service.DataSetSecurityService;
import service.impl.DataSetSecurityServiceImpl;

public class JLLTestSample {
	
	public static void main(String[] args) {
		
		// read pii_data_default.xml file and process the parser to create a DataSetModel list.
		PiiDataDefaultFactory piiDataDefaultFactory = null;
		try {
			piiDataDefaultFactory = new PiiDataDefaultFactory();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// obtain the dataset list.
		ArrayList<DataSetModel> datasetList = piiDataDefaultFactory.getDataSetsList();
		
		// read dataSource.properties file to get the connection driver
		// if it is sqlserver, oracle, mysql, db2 or sybase
		DataSetSecurityService dataSetSecurityService = new DataSetSecurityServiceImpl();
		
		// retrieve the current value of the given table and field.
		List<PiiDataSourceModel> datasourceList = dataSetSecurityService.retrieveDataSource(datasetList);
		
		// process the data pii update on the given table and field.
		dataSetSecurityService.updatePiiDataWithAlgorithm(datasourceList);
	}

}
