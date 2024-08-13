package algorithm.bigDecimalAlgorithm;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Random;

import algorithm.Algorithm;

public class RandomBigDecimalInRange implements Algorithm{
	
	private static final double rangeMin = 1.00; 
	
	private static final double rangeMax = 10000.00; 

	public RandomBigDecimalInRange() {
		
	}
	
	public String getName() {
		return "RandomDoubleInRange";
	}

	@Override
	public <T> T init(T dataValue) {
		Random r = new Random();
		BigDecimal originalvalue = (BigDecimal) dataValue;
		double randomValue = originalvalue.doubleValue() + rangeMin + (rangeMax - rangeMin) * r.nextDouble();
		BigDecimal result = new BigDecimal(randomValue);
		result = result.setScale(2, RoundingMode.HALF_UP);
		
		// here, we may need to think about how to trim the data.
		
		return  (T) result;
	}

}
