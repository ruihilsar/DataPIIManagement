package algorithm;

public interface Algorithm {


	/**
	 * Name of the Algorithm.
	 * @return name of the Algorithm
	*/
	public String getName();

	/**
	 * initiate the algorithm and get the return value after applying the algorithm.
	 * @param <T>
	 * @param dataValue
	 * @return
	 */
	public <T> T init(T dataValue);
		  
}
