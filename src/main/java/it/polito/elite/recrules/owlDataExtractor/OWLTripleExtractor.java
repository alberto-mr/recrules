package it.polito.elite.recrules.owlDataExtractor;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.semanticweb.HermiT.ReasonerFactory;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.reasoner.ConsoleProgressMonitor;
import org.semanticweb.owlapi.reasoner.InferenceType;
import org.semanticweb.owlapi.reasoner.OWLReasoner;
import org.semanticweb.owlapi.reasoner.SimpleConfiguration;

import gnu.trove.map.hash.TObjectIntHashMap;
import it.poliba.sisinflab.LODRec.fileManager.ItemFileManager;
import it.poliba.sisinflab.LODRec.fileManager.TextFileManager;
import it.poliba.sisinflab.LODRec.sparqlDataExtractor.RDFTripleExtractor;
import it.poliba.sisinflab.LODRec.tree.NTree;
import it.poliba.sisinflab.LODRec.utils.SynchronizedCounter;
import it.poliba.sisinflab.LODRec.utils.TextFileUtils;
import it.poliba.sisinflab.LODRec.utils.XMLUtils;

public class OWLTripleExtractor {
	private String workingDir; // working directory
	private int nThreads; // threads number
	
	private String endpoint; // endpoint ontology URI
	private boolean inMemory;
	private OWLOntology ontology;
	private OWLReasoner reasoner;
	
	private String inputFile; // input filename
	private String propsFile; // properties filename
	private boolean outputBinaryFormat; // output metadata format
	private String metadataFile; // output metadata filename
	private boolean outputTextFormat; // output text format
	private String textFile; // metadata text file
	
	private boolean inverseProps; // directed property
	private boolean caching; // caching
	private boolean append; // append to previous extraction
	
	private TObjectIntHashMap<String> URI_ID;
	private TObjectIntHashMap<String> props_index; // properties index
	private NTree props; // properties map
	private TObjectIntHashMap<String> metadata_index; // metadata index
		
	private String uriIdIndexFile; // uri-id index file
	private String propsIndexFile; // properties index file
	private String metadataIndexFile; // metadata index file

	private SynchronizedCounter counter; // synchronized counter for metadata index
	public static ConcurrentHashMap<String, ConcurrentHashMap<String, CopyOnWriteArrayList<String>>> cache;
	private static Logger logger = LogManager
			.getLogger(RDFTripleExtractor.class.getName());
	
	
	
	public OWLTripleExtractor(String workingDir, String itemMetadataFile,
			String inputItemURIsFile, String endpoint, Boolean inMemory, Boolean inverseProps,
			Boolean outputTextFormat, Boolean outputBinaryFormat,
			String propsFile, Boolean caching, Boolean append, int nThreads,
			boolean jenatdb) throws OWLOntologyCreationException {
		
		this.workingDir=workingDir;
		this.metadataFile = itemMetadataFile;
		this.append = append;
		this.caching = caching;
		this.endpoint = endpoint;
		this.inMemory = inMemory;
		this.inputFile = inputItemURIsFile;
		this.outputBinaryFormat = outputBinaryFormat;
		this.outputTextFormat = outputTextFormat;
		this.propsFile = propsFile;
		this.inverseProps = inverseProps;
		this.nThreads = nThreads;
		
		init();
	}
	
	private void init() throws OWLOntologyCreationException {

		this.textFile = this.metadataFile;
		this.propsIndexFile = this.workingDir + "props_index";
		this.metadataIndexFile = metadataFile + "_index";
		this.uriIdIndexFile = workingDir + "input_uri_id";
		
		// load input uri file
		loadInputFile();
		// load properties file
		loadProps();
		// load metadata index
		loadMetadataIndex();
		//load ontology endpoint
		loadOntology();
		// if caching is true initialize cache hash map
		if (caching) {
			cache = new ConcurrentHashMap<String, ConcurrentHashMap<String, CopyOnWriteArrayList<String>>>();
			logger.debug("Caching enabled.");
		}

	}
	
	
	/**
	 * Load properties from XML file
	 */
	private void loadProps(){

		props = new NTree();
		props_index = new TObjectIntHashMap<String>();

		try {
			// load properties map from XML file
			XMLUtils.parseXMLFile(propsFile, props_index, props, inverseProps);
			logger.debug("Properties tree loading.");

			// write properties index file
			TextFileUtils.writeData(propsIndexFile, props_index);

		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

	}

	/**
	 * Load input items file
	 */
	private void loadInputFile(){

		URI_ID = new TObjectIntHashMap<String>();
		// load [uri: id] from input file
		TextFileUtils.loadInputURIs(inputFile, URI_ID, append, uriIdIndexFile);
		logger.debug("Input items loading: " + URI_ID.size() + " URIs loaded.");
	}

	/**
	 * Load metadata index
	 */
	private void loadMetadataIndex(){

		metadata_index = new TObjectIntHashMap<String>();

		if(append){
			TextFileUtils.loadIndex(metadataIndexFile, metadata_index);
			logger.debug("Metadata index loading: " + metadata_index.size() 
				+ " metadata loaded.");
		}

		counter = new SynchronizedCounter(metadata_index.size());

	}
	
	/**
	 * Load ontology endpoint
	 * @throws OWLOntologyCreationException 
	 */
	private void loadOntology() throws OWLOntologyCreationException{
		logger.debug("Load the ontology");
		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
		if(inMemory){
			File endpointFile = new File(endpoint);
			this.ontology = manager.loadOntologyFromOntologyDocument(endpointFile);
		}
		else
			this.ontology = manager.loadOntologyFromOntologyDocument(IRI.create(endpoint));	
		logger.debug("Creation of the reaoner");
		this.reasoner = new ReasonerFactory().createReasoner(ontology, new SimpleConfiguration(new ConsoleProgressMonitor()));
		reasoner.precomputeInferences(InferenceType.values());
		logger.debug("Ontology " +  manager.getOntologyDocumentIRI(this.ontology) + " loaded.");
	}
	

	/**
	 * Run RDF triple extraction
	 */
	public void run(){

		//nThreads = 4;
		logger.debug("Threads number: " + nThreads);

		ExecutorService executor;
		executor = Executors.newFixedThreadPool(nThreads);

		logger.info("Resources to be queried: " + this.URI_ID.size());

		try{
			TextFileManager textWriter = null;
			if(outputTextFormat)
				textWriter = new TextFileManager(textFile, append);

			ItemFileManager fileManager = null;
			if(outputBinaryFormat){
				if(append)
					fileManager = new ItemFileManager(metadataFile, ItemFileManager.APPEND);
				else
					fileManager = new ItemFileManager(metadataFile, ItemFileManager.WRITE);
			}

			for (String uri : this.URI_ID.keySet()) {

				Runnable worker;

				// create worker thread
				worker = new OWLQueryExecutor(uri, URI_ID.get(uri), props, 
						props_index, ontology, reasoner, counter, metadata_index, 
						textWriter, fileManager, inverseProps, caching);
				
				executor.execute(worker);

			}

			// This will make the executor accept no new threads
			// and finish all existing threads in the queue
			executor.shutdown();
			// Wait until all threads are done
			executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

			if(textWriter!=null)
				textWriter.close();

			if(fileManager!=null)
				fileManager.close();

		}
		catch(Exception e){
			e.printStackTrace();
		}

		// write metadata index file
		TextFileUtils.writeData(metadataIndexFile, metadata_index);

	}
}
