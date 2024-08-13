package algorithm.dateAlgorithm;

import algorithm.Algorithm;

public class CustomDateAlgorithm implements Algorithm{

	public CustomDateAlgorithm() {
		
	}
	
	public String getName() {
		return "CustomDateAlgorithm";
	}

	@Override
	public <T> T init(T dataValue) {
		// we can implement the custom algorithm for date here.
		return dataValue;
	}

}