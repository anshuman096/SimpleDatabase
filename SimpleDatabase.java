package org.simpledatabase;

/**
 * A publicly accessible interface that, theoretically, will encapsulate the implementation of the database
 * from users and act as a UI
 */
public interface SimpleDatabase<K,V> {

	/**
	 * Set adds a <Key, ValueTuple> pair into the HashMap of the 
	 * current transaction block
	 * 
	 * @param key: the name of the variable to be added
	 * @param value: the value, that will be converted into a ValueTuple to be added
	 */
	public abstract void set(K key, V value);

	/**
	 * Get prints out the most recent value that the given variable maps to
	 * @param key: the variable that we are looking for
	 */
	public abstract V get(K key);

	/**
	 * 
	 * 
	 * @param key: The variable we are trying to "unset"
	 */
	public abstract void unset(K key);

	public abstract void begin();

	public abstract void rollBack();

	public abstract void commit();

	public abstract int numEqualTo(V value);

}