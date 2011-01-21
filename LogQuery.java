package com.plugtree.solr;

/**
 * This class represents a query read from a log file.
 *
 */
public class LogQuery {
	
	/** The query string. */
	private String q;
	
	/** True if the query string matches at least one document, false otherwise. */
	private boolean hasResults;
	
	/**
	 * Constructor.
	 * 
	 * @param q The query string.
	 * @param hasResults True if the query string matches at least one document, false otherwise.
	 */
	public LogQuery(String q, boolean hasResults) {
		this.q = q;
		this.hasResults = hasResults;
	}

	public String getQ() {
		return q;
	}

	public void setQ(String q) {
		this.q = q;
	}

	public boolean hasResults() {
		return hasResults;
	}

	public void setHasResults(boolean hasResults) {
		this.hasResults = hasResults;
	}
}