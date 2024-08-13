package algorithm.stringAlgorithm;

import algorithm.Algorithm;

public class CustomStringAlgorithm implements Algorithm{

	public CustomStringAlgorithm() {
		
	}
	
	public String getName() {
		return "CustomStringAlgorithm";
	}

	@Override
	public <T> T init(T dataValue) {
		// we can implement the custom algorithm here.
		return dataValue;
	}

}
