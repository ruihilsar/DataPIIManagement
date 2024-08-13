package algorithm.stringAlgorithm;

import algorithm.Algorithm;

public class MaskExceptLastThreeChar implements Algorithm{

	public MaskExceptLastThreeChar() {
		
	}
	
	public String getName() {
		return "MaskExceptLastThreeChar";
	}
	
//	public String init(String maskStr) {
//		String result = "";
//		int length = maskStr.length();
//		
//	    for(int i = 0; i < length; i++) {
//	    	if(i + 3 < length) {
//	    		result += "X";
//	    	} else {
//	    		result += maskStr.charAt(i);
//	    	}
//	    }
//	    
//	    return result;
//	}

	@Override
	public <T> T init(T dataValue) {
		String result = "";
		String maskStr = (String) dataValue;
		int length = maskStr.length();
		
	    for(int i = 0; i < length; i++) {
	    	if(i + 3 < length) {
	    		result += "X";
	    	} else {
	    		result += maskStr.charAt(i);
	    	}
	    }
	    
	    return  (T) result;
	}

}