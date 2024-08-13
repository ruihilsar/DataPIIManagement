package data;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.SAXException;

import command.DataSetModel;
import service.ParserService;

public class PiiDataDefaultFactory {
	public static final String PII_DATA_DEFAULT_XML = "pii_data_default.xml";
	
	private ArrayList<DataSetModel> dataSetsList;
	
	public ArrayList<DataSetModel> getDataSetsList() {
		return this.dataSetsList;
	}
	
	public void setDataSetsList(ArrayList<DataSetModel> dataSetsList) {
		this.dataSetsList = dataSetsList;
	}
	
	public PiiDataDefaultFactory() throws FileNotFoundException {
//		this(PiiDataDefaultFactory.class.getClassLoader().getResourceAsStream(PII_DATA_DEFAULT_XML));
		this(new FileInputStream(PII_DATA_DEFAULT_XML));
	}
	
	public PiiDataDefaultFactory(InputStream in) {
		SAXParserFactory factory = SAXParserFactory.newInstance();
		try {
			// get SAX Parser.
			SAXParser saxParser = factory.newSAXParser();
			
			// initiate SAX Parser service.
			ParserService parserService = new ParserService();
			
			// execute the parser and parse the result in an input stream.
			saxParser.parse(in, parserService);
			
			//get the xml data set list.
			dataSetsList = ParserService.datasetsList;
			
		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
