package service;

import java.util.ArrayList;

import org.apache.log4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import command.DataSetModel;

public class ParserService extends DefaultHandler{
	
	private static Logger logger = Logger.getLogger(ParserService.class);
	
	private String value;
	private boolean inTableName = false;
	private boolean inFieldName = false;
	private boolean inRestriction = false;
	private boolean inIsKey = false;
	private boolean inAlgorithm = false;
	private boolean inEnabled = false;
	private DataSetModel dataSetModel = new DataSetModel();
	public static ArrayList<DataSetModel> datasetsList = new ArrayList<DataSetModel>();
	
	public void characters(char[] buffer, int start, int length) {
		this.value = new String(buffer, start, length);
	}
	
	public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException{
		this.value = "";
		
		if(qName.equalsIgnoreCase("datasets")) {
			// start tag of the entire parser XML
		} else if(qName.equalsIgnoreCase("dataset")) {
			// start tag of one dataset
		} else if(qName.equalsIgnoreCase("table_name")) {
			inTableName = true;
		} else if (qName.equalsIgnoreCase("field_name")) {
			inFieldName = true;
		} else if(qName.equalsIgnoreCase("restriction")) {
			inRestriction = true;
		} else if (qName.equalsIgnoreCase("is_key")) {
			inIsKey = true;
		} else if (qName.equalsIgnoreCase("algorithm")) {
			inAlgorithm = true;
		} else if (qName.equalsIgnoreCase("enabled")) {
			inEnabled = true;
		} else {
			System.out.println("Error: wrong xml tag detected, it is not designated with this tag " + qName);
			logger.error("Error: wrong xml tag detected, it is not designated with this tag " + qName);
			return;
		}
	}
	
	public void endElement(String uri, String localName, String qName) throws SAXException {
		if(qName.equalsIgnoreCase("datasets")) {
			
		} else if(qName.equalsIgnoreCase("dataset")) {
			datasetsList.add(dataSetModel);
			
			// recycle all of the private resources
			inTableName = false;
			inFieldName = false;
			inIsKey = false;
			inAlgorithm = false;
			inEnabled = false;
			dataSetModel =  new DataSetModel();
		} else if(qName.equalsIgnoreCase("table_name") && inTableName) {
			if (this.value.isEmpty()) {
				System.out.println("Error: The table name for dataset cannot be empty.");
				logger.error("The table name for dataset cannot be empty.");
				return;
			}
			inTableName = false;
			dataSetModel.setTableName(this.value.trim());
		} else if (qName.equalsIgnoreCase("field_name") && inFieldName) {
			if (this.value.isEmpty()) {
				System.out.println("Error: The field name for dataset cannot be empty.");
				logger.error("The field name for dataset cannot be empty.");
				return;
			}
			inFieldName = false;
			dataSetModel.setFieldName(this.value.trim());
		} else if (qName.equalsIgnoreCase("restriction") && inRestriction) {
			inRestriction = false;
			dataSetModel.setRestriction(this.value.trim());
		} else if (qName.equalsIgnoreCase("is_key") && inIsKey) {
			if (this.value.isEmpty()) {
				System.out.println("Error: is key tag value for dataset cannot be empty.");
				logger.error("is key tag value for dataset cannot be empty.");
				return;
			}
			inIsKey = false;
			dataSetModel.setIsKey(Boolean.parseBoolean(this.value.trim()));
		} else if (qName.equalsIgnoreCase("algorithm") && inAlgorithm) {
			if (this.value.isEmpty()) {
				System.out.println("Error: algorithm for dataset cannot be empty.");
				logger.error("algorithm for dataset cannot be empty.");
				return;
			}
			inAlgorithm = false;
			dataSetModel.setAlgorithm(this.value.trim());
		} else if (qName.equalsIgnoreCase("enabled") && inEnabled) {
			if (this.value.isEmpty()) {
				System.out.println("Error: enabled for dataset cannot be empty.");
				logger.error("enabled for dataset cannot be empty.");
				return;
			}
			inEnabled = false;
			dataSetModel.setEnabled(Boolean.parseBoolean(this.value.trim()));
		}
		
	}
	
}
