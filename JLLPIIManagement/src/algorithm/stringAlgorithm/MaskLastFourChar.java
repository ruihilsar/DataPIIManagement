package algorithm.stringAlgorithm;

import algorithm.Algorithm;

public class MaskLastFourChar implements Algorithm{

	public MaskLastFourChar() {
		
	}
	
	public String getName() {
		return "MaskLastFourChar";
	}
	
	public String init(String maskStr) {	
		int length = maskStr.length();
	    // Check whether or not the string contains at least four characters
	    if (length < 4) {
	    	// return all characters to X
	    	String result = "";
	    	for(int i = 0; i < length; i++) {
	    		result += "X";
	    	}
	    	return result;
	    }
	    
	    return maskStr.substring(0, length - 4) + "XXXX";
	}

	@Override
	public <T> T init(T dataValue) {
		String maskStr = (String) dataValue;
		int length = maskStr.length();
	    // Check whether or not the string contains at least four characters
	    if (length < 4) {
	    	// return all characters to X
	    	String result = "";
	    	for(int i = 0; i < length; i++) {
	    		result += "X";
	    	}
	    	return (T) result;
	    }
	    
	    return (T) (maskStr.substring(0, length - 4) + "XXXX");
	}

}
