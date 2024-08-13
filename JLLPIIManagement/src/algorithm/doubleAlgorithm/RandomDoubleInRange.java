package algorithm.doubleAlgorithm;

import java.util.Random;

import algorithm.Algorithm;

public class RandomDoubleInRange implements Algorithm{
	
	private static final double rangeMin = 50.00; 
	
	private static final double rangeMax = 100.00; 

	public RandomDoubleInRange() {
		
	}
	
	public String getName() {
		return "RandomDoubleInRange";
	}

	@Override
	public <T> T init(T dataValue) {
		Random r = new Random();
		Double originalvalue = (Double) dataValue;
		double randomValue = originalvalue + rangeMin + (rangeMax - rangeMin) * r.nextDouble();
		Double result = new Double(randomValue);
		
		// here, we may need to think about how to trim the data.
		
		return  (T) result;
	}

}
