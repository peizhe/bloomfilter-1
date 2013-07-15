package org.araqne.bloomfilter;

public class HashValue<T> {

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + firstHashCode;
		result = prime * result + secondHashCode;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		HashValue other = (HashValue) obj;
		if (firstHashCode != other.firstHashCode)
			return false;
		if (secondHashCode != other.secondHashCode)
			return false;
		return true;
	}

	public int firstHashCode;
	public int secondHashCode;

	public HashValue(T key, HashFunction<T> firstFunction, HashFunction<T> secondFunction) {
		firstHashCode = firstFunction.hashCode(key);
		secondHashCode = secondFunction.hashCode(key);
	}

	public HashValue(int i, int j) {
		firstHashCode = i;
		secondHashCode = j;
	}

	public int getFirstHashCode() {
		return firstHashCode;
	}

	public int getSecondHashCode() {
		return secondHashCode;
	}

}
