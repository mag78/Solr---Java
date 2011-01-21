package com.plugtree.solr;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrLogImporter {
	
	private static Logger log = LoggerFactory.getLogger(SolrLogImporter.class);
	
	private Collection<LogQuery> queries = new LinkedList<LogQuery>();
	
	private SolrServer solrServer;
	
	/**
	 * Constructor.
	 * 
	 * @param url URL of the Solr server used to check if a query matches at least one document
	 * @throws MalformedURLException
	 */
	public SolrLogImporter(String url) throws MalformedURLException {
		solrServer = new CommonsHttpSolrServer(url);
	}
	
	/**
	 * Populates the collection of queries from a log file.
	 *  
	 * @param filename the name of the log file
	 * @param handler the path to the request handler, for example, '/solr/core/browser'
	 * @throws IOException if an error occurs reading the file
	 * @throws SolrServerException if an error occurs checking if a query matches at least one document
	 */
	private void loadLog(String filename, String handler) throws IOException, SolrServerException {
		BufferedReader in = new BufferedReader(new FileReader(filename));
		String s;
		Pattern p = Pattern.compile(".*GET " + handler + "\\?q=([^& ]*).*");

		s = in.readLine();
		while(s != null) {
			Matcher m = p.matcher(s);
			if (m.matches()) {
				String query = m.group(1);
				query = URLDecoder.decode(query, "UTF-8");
				queries.add(new LogQuery(query, hasResults(query)));
			}
			s = in.readLine();
		}
		
		in.close();
	}
	
	/**
	 * Use this method to check if a query matches at least one document.
	 * 
	 * @param q the query
	 * @return true if the query matches at least one document, false otherwise
	 * @throws SolrServerException if an error occurs while executing the query
	 */
	private boolean hasResults(String q) throws SolrServerException {
		SolrQuery solrQuery = new SolrQuery();
        solrQuery.setQuery(q);
        solrQuery.setQueryType("dismax");
        
        QueryResponse solrResponse = solrServer.query(solrQuery);
        SolrDocumentList results = solrResponse.getResults();
        
        return results.size()>0;
	}
	
	/**
	 * Updates the database with the data loaded from the log.
	 * 
	 * @throws SQLException if an error occurs searching or inserting into the database
	 * @throws ClassNotFoundException if an error occurs loading the SQL driver 
	 */
	private void updateDatabase() throws SQLException, ClassNotFoundException {
		Class.forName("com.mysql.jdbc.Driver");
		Connection con = DriverManager.getConnection ("jdbc:mysql://localhost/solr","root", "");
		PreparedStatement querySt = con.prepareStatement(
				"SELECT contador, id FROM autosuggest WHERE query = ?");
		PreparedStatement updateSt = con.prepareStatement(
				"UPDATE autosuggest SET contador = ?, resultado = ? WHERE id = ?");
		PreparedStatement insertSt = con.prepareStatement(
				"INSERT INTO autosuggest (id, query, resultado, contador) VALUES (?, ?, ?, ?)");
		
		for(LogQuery q: queries) {
			querySt.setString(1, q.getQ());
			ResultSet rs = querySt.executeQuery();
			
			if(rs.first()) {
				updateSt.setInt(1, rs.getInt(1)+1);
				updateSt.setInt(2, q.hasResults() ? 1 : 0);
				updateSt.setString(3, rs.getString(2));
				updateSt.executeUpdate();
			} else {
				/* TODO Usar ID entero que se autoincremente */
				insertSt.setString(1, UUID.randomUUID().toString());
				insertSt.setString(2, q.getQ());
				insertSt.setInt(3, q.hasResults() ? 1 : 0);
				insertSt.setInt(4, 1);
				insertSt.executeUpdate();
			}
		}
	}
	
	/**
	 * Use this method to index values from database into autosuggest for use it.
	 * 
	 * @param url to call data import handler that index values
	 * @throws MalformedURLException
	 * @throws IOException
	 */
	private void updateIndex (String url) throws MalformedURLException, IOException {
		URL dir =  new URL(url);
		InputStream response = dir.openStream();
		BufferedReader reader = new BufferedReader(new InputStreamReader(response));
		reader.close();
	}
	
	public static void main(String[] args) {
		try {
			SolrLogImporter solrLogImporter = new SolrLogImporter("http://localhost:8983/solr/core1/");
			
			solrLogImporter.loadLog(args[0], "/solr/core1/browse");
			
			try {
				solrLogImporter.updateDatabase();
				try {
					solrLogImporter.updateIndex ("http://localhost:8983/solr/core2/indexer/autosuggest?command=full-import");
				} catch (MalformedURLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} catch(SQLException ex) {
				log.error("Unable to update database", ex);
			} catch(ClassNotFoundException ex) {
				log.error("Unable to load SQL driver", ex);
			}
		} catch(IOException ex) {
			log.error("Unable to read log file", ex);
		} catch(SolrServerException ex) {
			log.error("Unable to query Solr server", ex);
		}
	}
}