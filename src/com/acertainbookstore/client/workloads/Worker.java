/**
 * 
 */
package com.acertainbookstore.client.workloads;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import com.acertainbookstore.business.*;
import com.acertainbookstore.interfaces.BookStore;
import com.acertainbookstore.interfaces.StockManager;
import com.acertainbookstore.utils.BookStoreException;

/**
 * 
 * Worker represents the workload runner which runs the workloads with
 * parameters using WorkloadConfiguration and then reports the results
 * 
 */
public class Worker implements Callable<WorkerRunResult> {
    private WorkloadConfiguration configuration = null;
    private int numSuccessfulFrequentBookStoreInteraction = 0;
    private int numTotalFrequentBookStoreInteraction = 0;



    public Worker(WorkloadConfiguration config) {
	configuration = config;
    }

    /**
     * Run the appropriate interaction while trying to maintain the configured
     * distributions
     * 
     * Updates the counts of total runs and successful runs for customer
     * interaction
     * 
     * @param chooseInteraction
     * @return
     */
    private boolean runInteraction(float chooseInteraction) {
	try {
	    float percentRareStockManagerInteraction = configuration.getPercentRareStockManagerInteraction();
	    float percentFrequentStockManagerInteraction = configuration.getPercentFrequentStockManagerInteraction();

	    if (chooseInteraction < percentRareStockManagerInteraction) {
		runRareStockManagerInteraction();
	    } else if (chooseInteraction < percentRareStockManagerInteraction
		    + percentFrequentStockManagerInteraction) {
		runFrequentStockManagerInteraction();
	    } else {
		numTotalFrequentBookStoreInteraction++;
		runFrequentBookStoreInteraction();
		numSuccessfulFrequentBookStoreInteraction++;
	    }
	} catch (BookStoreException ex) {
	    return false;
	}
	return true;
    }

    /**
     * Run the workloads trying to respect the distributions of the interactions
     * and return result in the end
     */
    public WorkerRunResult call() throws Exception {
	int count = 1;
	long startTimeInNanoSecs = 0;
	long endTimeInNanoSecs = 0;
	int successfulInteractions = 0;
	long timeForRunsInNanoSecs = 0;

	Random rand = new Random();
	float chooseInteraction;

	// Perform the warmup runs
	while (count++ <= configuration.getWarmUpRuns()) {
	    chooseInteraction = rand.nextFloat() * 100f;
	    runInteraction(chooseInteraction);
	}

	count = 1;
	numTotalFrequentBookStoreInteraction = 0;
	numSuccessfulFrequentBookStoreInteraction = 0;

	// Perform the actual runs
	startTimeInNanoSecs = System.nanoTime();
	while (count++ <= configuration.getNumActualRuns()) {
	    chooseInteraction = rand.nextFloat() * 100f;
	    if (runInteraction(chooseInteraction)) {
		successfulInteractions++;
	    }
	}
	endTimeInNanoSecs = System.nanoTime();
	timeForRunsInNanoSecs += (endTimeInNanoSecs - startTimeInNanoSecs);
	return new WorkerRunResult(successfulInteractions, timeForRunsInNanoSecs, configuration.getNumActualRuns(),
		numSuccessfulFrequentBookStoreInteraction, numTotalFrequentBookStoreInteraction);
    }

    /**
     * Runs the new stock acquisition interaction
     * 
     * @throws BookStoreException
     */
    private void runRareStockManagerInteraction() throws BookStoreException {
		StockManager manager = configuration.getStockManager();
		List<StockBook> listBooks = manager.getBooks();

		BookSetGenerator generator = configuration.getBookSetGenerator();
		Set<StockBook> randomlistBooks = generator.nextSetOfStockBooks(configuration.getNumBooksToAdd());

		Set<StockBook> addedBooks = new HashSet<>();
		for (StockBook book  : randomlistBooks) {
			boolean flag = false ;

			for (StockBook book1 :listBooks ) {
				if (book.getISBN()==book1.getISBN()){
					flag = true;
					break;
				}

				if (flag = false) {
					addedBooks.add(book);

				}
			}
		}



		manager.addBooks(addedBooks);





    	// TODO: Add code for New Stock Acquisition Interaction


    }

    /**
     * Runs the stock replenishment interaction
     * 
     * @throws BookStoreException
     */
    private void runFrequentStockManagerInteraction() throws BookStoreException {


		StockManager manager = configuration.getStockManager();
		List<StockBook> listBooks = manager.getBooks();
		int k  = configuration.getNumBooksWithLeastCopies();
		int numcopies  = configuration.getNumAddCopies();

		List<StockBook> listofsmallestquantbooks = listBooks.stream().sorted(Comparator.comparing(StockBook::getNumCopies)).limit(k).collect(Collectors.toList());
		Set<BookCopy> addedBookCopies = new HashSet<>();

		for (StockBook book : listofsmallestquantbooks) {
			addedBookCopies.add(new BookCopy(book.getISBN(),numcopies));

		}

		manager.addCopies(addedBookCopies);

	// TODO: Add code for Stock Replenishment Interaction
    }

    /**
     * Runs the customer interaction
     * 
     * @throws BookStoreException
     */
    private void runFrequentBookStoreInteraction() throws BookStoreException {
		BookStore bookStore = configuration.getBookStore();
		List<Book> books = bookStore.getEditorPicks(configuration.getNumEditorPicksToGet());

		Set<Integer> bookISBNs = new HashSet<>();

		for(Book book : books){
			bookISBNs.add(book.getISBN());
		}

		BookSetGenerator generator = configuration.getBookSetGenerator();
		Set<Integer> booksToBuy = generator.sampleFromSetOfISBNs(bookISBNs, configuration.getNumBooksToBuy());

		Set<BookCopy> bookCopies = new HashSet<>();

		for(Integer isbn: booksToBuy){
			bookCopies.add(new BookCopy(isbn,configuration.getNumBookCopiesToBuy()));
		}

		bookStore.buyBooks(bookCopies);
	}

}
