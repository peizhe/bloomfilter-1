package org.araqne.bloomfilter;

public class HashValue<T> {

	private int firstHashCode;
	private int secondHashCode;

	public HashValue(T key, HashFunction<T> firstFunction, HashFunction<T> secondFunction) {
		firstHashCode = firstFunction.hashCode(key);
		secondHashCode = secondFunction.hashCode(key);
	}

	public int getFirstHashCode() {
		return firstHashCode;
	}

	public int getSecondHashCode() {
		return secondHashCode;
	}

}
