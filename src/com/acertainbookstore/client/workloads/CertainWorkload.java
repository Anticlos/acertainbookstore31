/**
 * 
 */
package com.acertainbookstore.client.workloads;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.acertainbookstore.business.CertainBookStore;
import com.acertainbookstore.business.ImmutableStockBook;
import com.acertainbookstore.business.StockBook;
import com.acertainbookstore.client.BookStoreHTTPProxy;
import com.acertainbookstore.client.StockManagerHTTPProxy;
import com.acertainbookstore.interfaces.BookStore;
import com.acertainbookstore.interfaces.StockManager;
import com.acertainbookstore.utils.BookStoreConstants;
import com.acertainbookstore.utils.BookStoreException;

/**
 * 
 * CertainWorkload class runs the workloads by different workers concurrently.
 * It configures the environment for the workers using WorkloadConfiguration
 * objects and reports the metrics
 * 
 */
public class CertainWorkload {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int numConcurrentWorkloadThreads = 10;
		String serverAddress = "http://localhost:8081";
		boolean localTest = true;
		List<WorkerRunResult> workerRunResults = new ArrayList<WorkerRunResult>();
		List<Future<WorkerRunResult>> runResults = new ArrayList<Future<WorkerRunResult>>();

		// Initialize the RPC interfaces if its not a localTest, the variable is
		// overriden if the property is set
		String localTestProperty = System
				.getProperty(BookStoreConstants.PROPERTY_KEY_LOCAL_TEST);
		localTest = (localTestProperty != null) ? Boolean
				.parseBoolean(localTestProperty) : localTest;

		BookStore bookStore = null;
		StockManager stockManager = null;
		if (localTest) {
			CertainBookStore store = new CertainBookStore();
			bookStore = store;
			stockManager = store;
		} else {
			stockManager = new StockManagerHTTPProxy(serverAddress + "/stock");
			bookStore = new BookStoreHTTPProxy(serverAddress);
		}

		// Generate data in the bookstore before running the workload
		initializeBookStoreData(bookStore, stockManager);

		ExecutorService exec = Executors
				.newFixedThreadPool(numConcurrentWorkloadThreads);

		for (int i = 0; i < numConcurrentWorkloadThreads; i++) {
			WorkloadConfiguration config = new WorkloadConfiguration(bookStore,
					stockManager);
			Worker workerTask = new Worker(config);
			// Keep the futures to wait for the result from the thread
			runResults.add(exec.submit(workerTask));
		}

		// Get the results from the threads using the futures returned
		for (Future<WorkerRunResult> futureRunResult : runResults) {
			WorkerRunResult runResult = futureRunResult.get(); // blocking call
			workerRunResults.add(runResult);
		}

		exec.shutdownNow(); // shutdown the executor

		// Finished initialization, stop the clients if not localTest
		if (!localTest) {
			((BookStoreHTTPProxy) bookStore).stop();
			((StockManagerHTTPProxy) stockManager).stop();
		}

		reportMetric(workerRunResults);
	}

	/**
	 * Computes the metrics and prints them
	 * 
	 * @param workerRunResults
	 */
	public static void reportMetric(List<WorkerRunResult> workerRunResults) {
		List<Double> latencyResults = new ArrayList<>();
		List<Double> throughputResults = new ArrayList<>();

		for (WorkerRunResult workerRunResult: workerRunResults){
			double seconds = (double)workerRunResult.getElapsedTimeInNanoSecs()/1000000000.0;
			latencyResults.add((double) (workerRunResult.getSuccessfulFrequentBookStoreInteractionRuns()/workerRunResults.size()));
			throughputResults.add(workerRunResult.getSuccessfulFrequentBookStoreInteractionRuns()/seconds);
		}

		Double aggThroughput = throughputResults.stream().mapToDouble(f -> f).sum();
		Double latency = latencyResults.stream().mapToDouble(val -> val).average().orElse(0.0);
		System.out.println(aggThroughput);
		System.out.println(latency);
	}

	/**
	 * Generate the data in bookstore before the workload interactions are run
	 *
	 * Ignores the serverAddress if its a localTest
	 *
	 */
	public static void initializeBookStoreData(BookStore bookStore,
											   StockManager stockManager) throws BookStoreException {
		Set<StockBook> generatedBooks = new HashSet<>();
		Random rand = new Random();
		for(int i = 0; i < 5000; i++){
			int isbn = rand.nextInt(Integer.MAX_VALUE) + 1;
			int numCopies = 50;

			String alphaNumericString = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
					+ "0123456789"
					+ "abcdefghijklmnopqrstuvxyz";

			char[] bookTitle = new char[50];
			char[] author = new char[50];

			for (int j = 0; j < bookTitle.length; j++)
			{
				bookTitle[j] = alphaNumericString.charAt(rand.nextInt(alphaNumericString.length()));
			}

			for (int j = 0; j < author.length; j++)
			{
				author[j] = alphaNumericString.charAt(rand.nextInt(alphaNumericString.length()));
			}

			int price = rand.nextInt(100) + 1;
			int numSalesMisses = rand.nextInt(100) + 1;
			int numTimesRated = rand.nextInt(100) + 1;
			int totalRating = rand.nextInt(100) + 1;
			boolean editorPick = rand.nextBoolean();
			generatedBooks.add(new ImmutableStockBook
					(isbn, new String(bookTitle), new String(author), (float) price,
							numCopies, numSalesMisses, numTimesRated, totalRating, editorPick));
		}

		stockManager.addBooks(generatedBooks);
	}
}
