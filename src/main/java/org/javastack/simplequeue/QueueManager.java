package org.javastack.simplequeue;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public interface QueueManager<T> {
	/**
	 * Get Message From Queue
	 * 
	 * @param qname
	 * @param timeout
	 * @param unit
	 * @param ta
	 * @return
	 * @throws InterruptedException
	 * @throws TimeoutException
	 */
	T get(final String qname, final long timeout, final TimeUnit unit, final TransactionAdapter<T> ta)
			throws InterruptedException, TimeoutException;

	/**
	 * Check if qname is valid
	 * 
	 * @param qname
	 * @return true if good, false if bad
	 */
	boolean checkQName(final String qname);

	/**
	 * Put Message to Queue
	 * 
	 * @param qname
	 * @param element
	 * @param timeout
	 * @param unit
	 * @throws InterruptedException
	 * @throws TimeoutException
	 */
	void put(final String qname, final T element, final long timeout, final TimeUnit unit)
			throws InterruptedException, TimeoutException;

	/**
	 * Get Size of Queue
	 * 
	 * @param qname
	 * @return
	 */
	int size(final String qname) throws InterruptedException;

	/**
	 * Initialize Queue Manager
	 * 
	 * @throws InterruptedException
	 */
	void init() throws InterruptedException;

	/**
	 * Destroy Queue Manager and free resources
	 * 
	 * @throws InterruptedException
	 */
	void destroy() throws InterruptedException;

	public interface TransactionAdapter<T> {
		boolean canCommit(final String qname, final T data);
	}
}
