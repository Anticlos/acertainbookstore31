package com.acertainbookstore.client.workloads;

import java.nio.charset.Charset;
import java.util.*;


import com.acertainbookstore.business.ImmutableStockBook;
import com.acertainbookstore.business.StockBook;

/**
 * Helper class to generate stockbooks and isbns modelled similar to Random
 * class
 */
public class BookSetGenerator {

	public BookSetGenerator() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * Returns num randomly selected isbns from the input set
	 * 
	 * @param num
	 * @return
	 */
	public Set<Integer> sampleFromSetOfISBNs(Set<Integer> isbns, int num) {
   // check if num is bigger than list
		if (num >= isbns.size()){
			return isbns ;
		}else {
			int numToRemove = isbns.size() - num;
			Set<Integer> isbnsNews = isbns;
			for (int i = 0; i < numToRemove; i++) {
				Random random = new Random();
				List<Integer> isbnsList = new ArrayList<>(isbnsNews);

				int index = random.nextInt(isbnsList.size());
				int value = isbnsList.get(index);
				isbnsNews.remove(value);
			}
			return isbnsNews;
		}

	}


	/**
	 * Return num stock books. For now return an ImmutableStockBook
	 * 
	 * @param num
	 * @return
	 */
	public Set<StockBook> nextSetOfStockBooks(int num) {
		Random random = new Random();
		Set<StockBook> generatedbooks = new HashSet<StockBook>();
	    for (int i = 0; i < num; i++) {
			int randISBN = random.nextInt(1000);
			byte[] array = new byte[10]; // length is bounded by 7
			new Random().nextBytes(array);
			String alphaNumericString = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
					+ "0123456789"
					+ "abcdefghijklmnopqrstuvxyz";
			char[] generatedTitle = new char[random.nextInt(100) + 1];
			char[] generatedAuthor = new char[random.nextInt(100) + 1];

			for (int j = 0; j < generatedTitle.length; j++)
			{
				generatedTitle[j] = alphaNumericString.charAt(random.nextInt(alphaNumericString.length()));
			}

			for (int j = 0; j < generatedAuthor.length; j++)
			{
				generatedAuthor[j] = alphaNumericString.charAt(random.nextInt(alphaNumericString.length()));
			}

			int randPRICE = random.nextInt(100);
			int randCOPIES= random.nextInt(200);
			int randSALESMISS= random.nextInt(200);
			int randTOTALRATING= random.nextInt(200);
			int randTIMESRATED= random.nextInt(200);
			boolean randBoolean = random.nextBoolean();



			generatedbooks.add(new ImmutableStockBook(randISBN, new String(generatedTitle), new String(generatedAuthor),
					(float) randPRICE, randCOPIES, randSALESMISS, randTIMESRATED, randTOTALRATING, randBoolean));

		}

		return generatedbooks;
	}

}
