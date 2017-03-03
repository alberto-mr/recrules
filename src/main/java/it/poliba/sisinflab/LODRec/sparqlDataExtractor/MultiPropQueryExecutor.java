package it.poliba.sisinflab.LODRec.sparqlDataExtractor;

import gnu.trove.map.hash.THashMap;
import gnu.trove.map.hash.TObjectCharHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import it.poliba.sisinflab.LODRec.fileManager.ItemFileManager;
import it.poliba.sisinflab.LODRec.fileManager.TextFileManager;
import it.poliba.sisinflab.LODRec.itemManager.ItemTree;
import it.poliba.sisinflab.LODRec.itemManager.PropertyIndexedItemTree;
import it.poliba.sisinflab.LODRec.tree.NNode;
import it.poliba.sisinflab.LODRec.tree.NTree;
import it.poliba.sisinflab.LODRec.utils.SynchronizedCounter;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFactory;
import com.hp.hpl.jena.rdf.model.Model;

/**
 * This class is part of the LOD Recommender
 * 
 * This class is used by RDFTripleExtractor for multi-threading RDF triples extraction 
 * 
 * @author Vito Mastromarino
 */
public class MultiPropQueryExecutor implements Runnable {
	
	private String uri; // uri resource
	private NTree props; // properties map
	private TObjectIntHashMap<String> props_index; // properties index
	private String graphURI; // graph uri
	private String endpoint; // endpoint address
	private SynchronizedCounter counter; // synchronized counter for metadata index
	private TObjectIntHashMap<String> metadata_index; // metadata index
	private TextFileManager textWriter; 
	private Model model; // local dataset model
	private ItemTree itemTree; // item tree
	private boolean inverseProps; // directed property
	private ItemFileManager fileManager;
	private boolean caching;
	
	private static Logger logger = LogManager.getLogger(MultiPropQueryExecutor.class.getName());
	
	
	/**
	 * Constuctor
	 */
	public MultiPropQueryExecutor(String uri, int uri_id,  
			NTree props, 
			TObjectIntHashMap<String> props_index, String graphURI, String endpoint, 
			SynchronizedCounter counter, TObjectIntHashMap<String> metadata_index, 
			TextFileManager textWriter, ItemFileManager fileManager, boolean inverseProps, boolean caching){
		this.uri = uri;
		this.props = props;
		this.props_index = props_index;
		this.graphURI = graphURI;
		this.endpoint = endpoint;
		this.counter = counter;
		this.textWriter = textWriter;
		this.metadata_index = metadata_index;
		this.model = null;
		this.fileManager = fileManager;
		
		this.inverseProps = inverseProps;
		this.itemTree = new PropertyIndexedItemTree(uri_id);
		this.caching = caching;
		
	}
	
	/**
	 * Constuctor for local dataset query
	 */
	public MultiPropQueryExecutor(String uri, int uri_id, NTree props, 
			TObjectIntHashMap<String> props_index, String graphURI, String endpoint, 
			SynchronizedCounter counter, TObjectIntHashMap<String> metadata_index, 
			TextFileManager textWriter, ItemFileManager fileManager, boolean inverseProps, 
			boolean caching, Model model){

		this(uri, uri_id, props, props_index, graphURI, endpoint, counter, metadata_index,
				textWriter, fileManager, inverseProps, caching);
		this.model = model;
	}
	
	/**
	 * Start RDF triple extraction
	 */
	public void run(){
		
		logger.info(uri + ": start data extraction");
		
		long start = System.currentTimeMillis();
		
		NNode root = props.getRoot();
		
		// execute query
		if(caching)
			execWithCaching(root, "", uri);
		else
			exec(root, "", uri);
		
		if(itemTree.size()>0){
			
			// text file writing
			if(textWriter != null)
				textWriter.write(itemTree.serialize()); 
			
			// binary file writing
			if(fileManager != null)
				fileManager.write(itemTree);
		}
		
		long stop = System.currentTimeMillis();
		logger.info(uri + ": data extraction terminated in [sec]: " 
				+ ((stop - start) / 1000));
		
	}
	
	
	/**
	 * Execute RDF triple extraction
	 */
	private void exec(NNode node, String list_props, String uri){
		
		if(node.hasChilds()){
			
			ArrayList<String> props = new ArrayList<String>();
			
			for (NNode children : node.getChilds())
				props.add(children.getValue());
			
			THashMap<String, TObjectCharHashMap<String>> res = 
					new THashMap<String, TObjectCharHashMap<String>>();
			res.putAll(runQuery(uri, props));
			
			if(res.size()>0){
				
				for(NNode n : node.getChilds()){
					
					String p = n.getValue();
					
					if(res.containsKey(p)){
						
						String p_index;
						
						for(String uri_res : res.get(p).keySet()){
							
							p_index = String.valueOf(props_index.get(p));
							
							if(inverseProps){
								if(res.get(p).get(uri_res)=='s')
									p_index = String.valueOf(props_index.get("inv_" + p));
							}
							
							if(list_props.length()>0){
								itemTree.addBranches(list_props + "-" + p_index, extractKey(uri_res));
								exec(n, list_props + "-" + p_index, uri_res);
							}
							else{
								itemTree.addBranches(p_index, extractKey(uri_res));
								exec(n, p_index, uri_res);
							}
							
						}
						
					}
					
				}
			}
			
		}
	}
	
	
	/**
	 * Execute RDF triple extraction with caching
	 */
	private void execWithCaching(NNode node, String list_props, String uri){
		
		if(node.hasChilds()){
			
			ArrayList<String> props = new ArrayList<String>();
			
			THashMap<String, TObjectCharHashMap<String>> res = 
					new THashMap<String, TObjectCharHashMap<String>>();
			
			for (NNode children : node.getChilds()){
				
				String id_prop = String.valueOf(props_index.get(children.getValue()));
				
				if(!RDFTripleExtractor.cache.containsKey(uri))
					props.add(children.getValue());
				
				// get prop results from cache
				else{
					
					logger.debug("Cache: " + uri);
					
					if(RDFTripleExtractor.cache.get(uri).containsKey(id_prop)){
						for(String uri_res : RDFTripleExtractor.cache.get(uri).get(id_prop)){
							if(list_props.length()>0){
								itemTree.addBranches(list_props + "-" + id_prop, extractKey(uri_res));
								execWithCaching(children, list_props + "-" + id_prop, uri_res);
							}
							else{
								itemTree.addBranches(id_prop, extractKey(uri_res));
								execWithCaching(children, id_prop, uri_res);
							}
								
						}
					}
					
					if(inverseProps){
						
						id_prop = String.valueOf(props_index.get("inv_" + children.getValue()));
						
						if(RDFTripleExtractor.cache.get(uri).containsKey(id_prop)){
							for(String uri_res : RDFTripleExtractor.cache.get(uri).get(id_prop)){
								if(list_props.length()>0){
									itemTree.addBranches(list_props + "-" + id_prop, extractKey(uri_res));
									execWithCaching(children, list_props + "-" + id_prop, uri_res);
								}
								else{
									itemTree.addBranches(id_prop, extractKey(uri_res));
									execWithCaching(children, id_prop, uri_res);
								}
							}
						}
					}
				}
			}
			
			
			if(props.size()>0){
			
				res.putAll(runQuery(uri, props));
				
				if(res.size()>0){
					
					RDFTripleExtractor.cache.putIfAbsent(uri, 
							new ConcurrentHashMap<String, CopyOnWriteArrayList<String>>());
					
					for(NNode n : node.getChilds()){
						
						String p = n.getValue();
						
						if(res.containsKey(p)){
							
							String p_index;
							
							for(String uri_res : res.get(p).keySet()){
								
								p_index = String.valueOf(props_index.get(p));
								
								if(inverseProps){
									if(res.get(p).get(uri_res)=='s')
										p_index = String.valueOf(props_index.get("inv_" + p));
								}
								
								RDFTripleExtractor.cache.get(uri).putIfAbsent(p_index, 
										new CopyOnWriteArrayList<String>());
								RDFTripleExtractor.cache.get(uri).get(p_index).add(uri_res);
								
								if(list_props.length()>0){
									itemTree.addBranches(list_props + "-" + p_index, extractKey(uri_res));
									execWithCaching(n, list_props + "-" + p_index, uri_res);
								}
								else{
									itemTree.addBranches(p_index, extractKey(uri_res));
									execWithCaching(n, p_index, uri_res);
								}
								
							}
							
						}
						
					}
				}
			}
		}
	}
	
	/**
	 * Run SPARQL query 
	 * @param     uri  uri resource
	 * @param     props	list of properties
	 * @return    results map: uri-s (if uri is a subject), uri-o (if uri is an object)
	 */
	private THashMap<String, TObjectCharHashMap<String>> runQuery(String uri, ArrayList<String> props){
		
		THashMap<String, TObjectCharHashMap<String>> results = new THashMap<String, TObjectCharHashMap<String>>();
		
		Query query;
		String q;
		
		q = "SELECT * WHERE { ";
		
		for(String p : props)
			q += "{{?s <" + p + "> <" + uri + ">. FILTER isIRI(?s). } UNION " +
							"{<" + uri + "> <" + p + "> ?o. FILTER isIRI(?o). } BIND (<" + p + "> AS ?p).} UNION ";
		
		q = q.substring(0, q.length()-6) + "}";
		
		logger.debug(q);
		
		try 
		{	
			query = QueryFactory.create(q);
			results = executeQuery(query);
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		}
		

		return results;
	}
	
	/**
	 * Execute SPARQL query 
	 * @param     query  sparql query
	 * @param     p  property
	 * @return    results map: prop[uri-s] (if uri is a subject), prop[uri-o] (if uri is an object)
	 */
	private THashMap<String, TObjectCharHashMap<String>> executeQuery(Query query) {
		
		THashMap<String, TObjectCharHashMap<String>> results = new THashMap<String, TObjectCharHashMap<String>>();
		QueryExecution qexec = null;
		
		if(model==null){
			if(graphURI == null)
				qexec = QueryExecutionFactory.sparqlService(endpoint, query); // remote query
			else
				qexec = QueryExecutionFactory.sparqlService(endpoint, query, graphURI); // remote query
		}	
		else
			qexec = QueryExecutionFactory.create(query, model); // local query
			
		try{
		
			ResultSet res = ResultSetFactory.copyResults(qexec.execSelect()) ;
			
			QuerySolution qs;
			String n;
			String p;
			
			while (res.hasNext()) {
				
				qs = res.next();
				p = qs.get("p").toString();
				results.putIfAbsent(p, new TObjectCharHashMap<String>());
				
				if (qs.get("o") == null) 
				{
					// get subject
					n = qs.get("s").toString();
					
					// consider only the type "yago"
					if (!p.contains("type"))
						results.get(p).put(n, 's'); // target as subject
					else 
					{
						if (n.contains("yago"))
							results.get(p).put(n, 's'); // target as subject
					}
					
				}
				else
				{
					// get object
					n = qs.get("o").toString();
					
					// consider only the type "yago"
					if (!p.contains("type"))
						results.get(p).put(n, 'o'); // target as object
					else 
					{
						if (n.contains("yago"))
							results.get(p).put(n, 'o'); // target as object
					}
				}
				
			}
			
		}
		
		catch(Exception e){
			e.printStackTrace();
		}
		finally{
			qexec.close();
		}
		
		return results;
		
	}
	
	/**
	 * Extract key from metadata index
	 * @param     s  string to index
	 * @return    index of s
	 */
	private int extractKey(String s) {
		synchronized (metadata_index) {
			
			if(metadata_index.containsKey(s)){
				return metadata_index.get(s);
			}
			else{
				int id = counter.value();
				metadata_index.put(s, id);
				return id;
			}

		}

	}

}
