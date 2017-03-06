package it.polito.elite.recrules.owlDataExtractor;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.semanticweb.owlapi.model.AxiomType;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLAxiom;
import org.semanticweb.owlapi.model.OWLNamedIndividual;
import org.semanticweb.owlapi.model.OWLObjectProperty;
import org.semanticweb.owlapi.model.OWLObjectPropertyAssertionAxiom;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.reasoner.OWLReasoner;
import org.semanticweb.owlapi.search.EntitySearcher;
import org.semanticweb.owlapi.util.DefaultPrefixManager;

import gnu.trove.map.hash.TObjectByteHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import it.poliba.sisinflab.LODRec.fileManager.ItemFileManager;
import it.poliba.sisinflab.LODRec.fileManager.TextFileManager;
import it.poliba.sisinflab.LODRec.itemManager.ItemTree;
import it.poliba.sisinflab.LODRec.itemManager.PropertyIndexedItemTree;
import it.poliba.sisinflab.LODRec.sparqlDataExtractor.QueryExecutor;
import it.poliba.sisinflab.LODRec.tree.NNode;
import it.poliba.sisinflab.LODRec.tree.NTree;
import it.poliba.sisinflab.LODRec.utils.SynchronizedCounter;

public class OWLQueryExecutor implements Runnable {
	
	private String uri; // uri resource
	private NTree props; // properties map
	private TObjectIntHashMap<String> props_index; // properties index
	private OWLOntology ontology; // ontology endpoint
	@SuppressWarnings("unused")
	private OWLReasoner reasoner; // ontology endpoint reasoner
	private DefaultPrefixManager  prefix; //ontology default prefix
	private SynchronizedCounter counter; // synchronized counter for metadata index
	private TObjectIntHashMap<String> metadata_index; // metadata index
	private TextFileManager textWriter; 
	private ItemTree itemTree; // item tree
	private boolean inverseProps; // directed property
	private ItemFileManager fileManager;
	private boolean caching;
	
	private static Logger logger = LogManager.getLogger(QueryExecutor.class.getName());

	
	/**
	 * Constuctor
	 */
	public OWLQueryExecutor(String uri, int uri_id, NTree props, 
			TObjectIntHashMap<String> props_index, OWLOntology ontology, OWLReasoner reasoner, 
			SynchronizedCounter counter, TObjectIntHashMap<String> metadata_index, 
			TextFileManager textWriter, ItemFileManager fileManager, boolean inverseProps, 
			boolean caching){

		this.uri = uri;
		this.props = props;
		this.props_index = props_index;
		this.ontology = ontology;
		this.reasoner = reasoner;
		this.counter = counter;
		this.textWriter = textWriter;
		this.metadata_index = metadata_index;
		this.fileManager = fileManager;
		this.inverseProps = inverseProps;
		this.itemTree = new PropertyIndexedItemTree(uri_id);
		this.caching = caching;
		
		this.prefix = new DefaultPrefixManager(null, null,
				this.ontology.getOntologyID().getOntologyIRI().get().toString() + "#");
	}
	
	/**
	 * Start OWL triple extraction for selected uri
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
	 * Execute OWL triple extraction
	 */
	private void exec(NNode node, String list_props, String uri){
		
		if(node.hasChilds()){
		
			String p;
			
			for (NNode children : node.getChilds()) {
	            
				p = children.getValue();
				String p_index;
				
				TObjectByteHashMap<String> result = new TObjectByteHashMap<String>();
				
				result.putAll(runQuery(uri, p));
					
				if(result.size()>0){
					
					for(String uri_res : result.keySet()){
						
						p_index = String.valueOf(props_index.get(p));
							
						if(inverseProps){
							if(result.get(uri_res) == (byte) 1)
								p_index = String.valueOf(props_index.get("inv_" + p));
						}
							
						if(list_props.length()>0){
							itemTree.addBranches(list_props + "-" + p_index, extractKey(uri_res));
							exec(children, list_props + "-" + p_index, uri_res);
						} else{
							itemTree.addBranches(p_index, extractKey(uri_res));
							exec(children, p_index, uri_res);
						}
					}
				}
	        }
		}
	}
	
	
	/**
	 * Execute OWL triple extraction with caching
	 */
	private void execWithCaching(NNode node, String list_props, String uri){
		
		if(node.hasChilds()){
		
			String p;
			
			for (NNode children : node.getChilds()) {
	            
				p = children.getValue();
				
				String p_index = String.valueOf(props_index.get(p));
				
				TObjectByteHashMap<String> result = new TObjectByteHashMap<String>();
				
				if(!OWLTripleExtractor.cache.containsKey(uri) || 
						!OWLTripleExtractor.cache.get(uri).containsKey(p_index)){
					
					result.putAll(runQuery(uri, p));
					
					if(result.size()>0){
						
						OWLTripleExtractor.cache.putIfAbsent(uri, 
								new ConcurrentHashMap<String, CopyOnWriteArrayList<String>>());
						
						for(String uri_res : result.keySet()){
							
							p_index = String.valueOf(props_index.get(p));
							
							if(inverseProps){
								if(result.get(uri_res) == (byte) 1)
									p_index = String.valueOf(props_index.get("inv_" + p));
							}
							
							OWLTripleExtractor.cache.get(uri).putIfAbsent(p_index, 
									new CopyOnWriteArrayList<String>());
							OWLTripleExtractor.cache.get(uri).get(p_index).add(uri_res);
							
							if(list_props.length()>0){
								itemTree.addBranches(list_props + "-" + p_index, extractKey(uri_res));
								execWithCaching(children, list_props + "-" + p_index, uri_res);
							} else {
								itemTree.addBranches(p_index, extractKey(uri_res));
								execWithCaching(children, p_index, uri_res);
							}				
						}
					}
					
				}
				
				// uri in cache
				else{
					
					logger.debug("Cache: " + uri);
					
					for(String uri_res : OWLTripleExtractor.cache.get(uri).get(p_index)){
						if(list_props.length()>0){
							itemTree.addBranches(list_props + "-" + p_index, extractKey(uri_res));
							execWithCaching(children, list_props + "-" + p_index, uri_res);
						} else {
							itemTree.addBranches(p_index, extractKey(uri_res));
							execWithCaching(children, p_index, uri_res);
						}
							
					}
					
					if(inverseProps){
						
						p_index = String.valueOf(props_index.get("inv_" + p));
						
						if(OWLTripleExtractor.cache.get(uri).containsKey(p_index)){
							for(String uri_res : OWLTripleExtractor.cache.get(uri).get(p_index)){
								if(list_props.length()>0){
									itemTree.addBranches(list_props + "-" + p_index, extractKey(uri_res));
									execWithCaching(children, list_props + "-" + p_index, uri_res);
								} else{
									itemTree.addBranches(p_index, extractKey(uri_res));
									execWithCaching(children, p_index, uri_res);
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
	 * @param     uri  resource uri
	 * @param     p  property
	 * @return    results map: uri-s (if uri is a subject), uri-o (if uri is an object)
	 */
	private TObjectByteHashMap<String> runQuery(String uri, String p){
		
		TObjectByteHashMap<String> results = new TObjectByteHashMap<String>();
		
		try {	
			OWLNamedIndividual individual = this.ontology.getOWLOntologyManager().getOWLDataFactory().getOWLNamedIndividual(IRI.create(prefix.getDefaultPrefix() + uri));
			OWLObjectProperty objectProperty = this.ontology.getOWLOntologyManager().getOWLDataFactory().getOWLObjectProperty(IRI.create(prefix.getDefaultPrefix() + p));
			results = executeQuery(individual, objectProperty);
		} 
		catch (Exception e) {
			e.printStackTrace();
		}

		return results;
	}
	
	/**
	 * Execute SPARQL query 
	 * @param     query  sparql query
	 * @param     p  property
	 * @return    results map: uri-s (if uri is a subject), uri-o (if uri is an object)
	 */
	private TObjectByteHashMap<String> executeQuery(OWLNamedIndividual individual, OWLObjectProperty property) {
		
		TObjectByteHashMap<String> results = new TObjectByteHashMap<String>();
		Collection<OWLAxiom> axioms = EntitySearcher.getReferencingAxioms(individual, ontology);				

		try{
			for(OWLAxiom axiom : axioms){
				if(axiom.getAxiomType().equals(AxiomType.OBJECT_PROPERTY_ASSERTION)){
					OWLObjectPropertyAssertionAxiom paxiom = (OWLObjectPropertyAssertionAxiom)axiom;
					if(paxiom.getSubject().equals(individual)){
						// get subject
						results.put(paxiom.getSubject().asOWLNamedIndividual().getIRI().toString() , (byte) 1);
					}
					else if(paxiom.getObject().equals(individual)){
						// get object
						results.put(paxiom.getObject().asOWLNamedIndividual().getIRI().toString() , (byte) 1);
					}
				}
			}
		}
		
		catch(Exception e){
			e.printStackTrace();
		}
		finally{
			//qexec.close();
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
			} else {
				int id = counter.value();
				metadata_index.put(s, id);
				return id;
			}
		}
	}
	
}
