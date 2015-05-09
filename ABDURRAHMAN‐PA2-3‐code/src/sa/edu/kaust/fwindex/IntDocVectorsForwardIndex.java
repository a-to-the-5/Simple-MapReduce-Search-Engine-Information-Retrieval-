/*
 * Ivory: A Hadoop toolkit for Web-scale information retrieval
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package sa.edu.kaust.fwindex;


import ivory.tokenize.GalagoTokenizer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Hashtable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import sa.edu.kaust.io.PostingWritable;
import sa.edu.kaust.io.TermDF;

import edu.umd.cloud9.collection.DocnoMapping;
import edu.umd.cloud9.collection.trec.TrecDocnoMapping;
import edu.umd.cloud9.io.array.ArrayListWritable;



/**
 * Object providing an index into one or more <code>SequenceFile</code>s
 * containing {@link IntDocVector}s, providing random access to the document
 * vectors.
 * 
 * @see BuildIntDocVectorsForwardIndex
 * 
 * @author Jimmy Lin
 */
public class IntDocVectorsForwardIndex {

	//Number of documents. The value is set in main method.
	private static int N;
	private static ArrayList<TermDF> keys;
	private static ArrayList<ArrayListWritable<PostingWritable>> values;
	
	
	private static final Logger sLogger = Logger.getLogger(IntDocVectorsForwardIndex.class);
	{
		sLogger.setLevel (Level.WARN);
	}

	private static final NumberFormat sFormatW5 = new DecimalFormat("00000");

	private FileSystem mFs;
	private Configuration mConf;

	private Hashtable<String, Long> mPositions;

	private String mOrigIndexPath;

	private int mCount;

	/**
	 * Creates an <code>IntDocVectorsIndex</code> object.
	 * 
	 * @param indexPath
	 *            location of the index file
	 * @param fs
	 *            handle to the FileSystem
	 * @throws IOException
	 */
	public IntDocVectorsForwardIndex(String origIndexPath, String fwindexPath, FileSystem fs) throws IOException {
		mFs = fs;
		mConf = fs.getConf();

		mOrigIndexPath = origIndexPath;
		sLogger.debug ("mPath: " + mOrigIndexPath);

		String forwardIndexPath = fwindexPath;
		sLogger.debug ("forwardIndexPath: " + forwardIndexPath);
		FSDataInputStream posInput = fs.open (new Path (forwardIndexPath));

		mCount = 0;

		mPositions = new Hashtable<String, Long>();
		while(true) {
			//Terms are retireved until end of file is reached.
			try{
				//The two values (term and position) are written in a single string (see 
				//BuildIntDocVectorsForwardIndex.MyReducer.reduce). Here they are retrieved. 
				String[] inp = posInput.readUTF().split("\t");
				String k = inp[0];
				long l = Long.parseLong(inp[1]);
				mPositions.put(k, l);
			}catch(Exception e){
				break;
			}
			mCount++;
		}
		sLogger.info("mCount: "+mCount);
	}

	/**
	 * Sets the values of variables 'keys' and 'values' to the TermDFs and PostingsLists of the
	 * terms in the 'terms' array.
	 * 
	 * @param terms The array of terms.
	 * @throws IOException
	 */
	public void getValue(String[] terms) throws IOException {
		//Reset 'keys' and 'values' as this is a new query.
		keys = new ArrayList<TermDF>();
		values = new ArrayList<ArrayListWritable<PostingWritable>>();
		
		for(String term : terms)
			getValue(term);
	}
	
	/**
	 * Reads the TermDF and PostingsList of a single term, and appends the values to 'keys'
	 * and 'values' respectively.
	 * 
	 * @param term The term to retrieve for.
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	public void getValue(String term) throws IOException {
		//Get position of the term.
		Long pos = mPositions.get(term);

		//If not found, return.
		if(pos==null) {
			return;
		}
		
		int fileNo = (int) (pos / BuildIntDocVectorsForwardIndex.BigNumber);
		pos = pos % BuildIntDocVectorsForwardIndex.BigNumber;

		SequenceFile.Reader reader = new SequenceFile.Reader(mFs, new Path(mOrigIndexPath + "/part-"
				+ sFormatW5.format(fileNo)), mConf);

		TermDF key = new TermDF();
		ArrayListWritable<PostingWritable> value;

		try {
			value = (ArrayListWritable<PostingWritable>) reader.getValueClass().newInstance();
		} catch (Exception e) {
			throw new RuntimeException("Unable to instantiate key/value pair!");
		}

		reader.seek(pos);
		reader.next(key, value);

		if (!key.getK_gram()[0].equals(term)) {
			sLogger.error("unable to doc vector for term " + term + ": found term" + key
					+ " instead");
			return;
		}

		reader.close();
		keys.add(key);
		values.add(value);
	}
	
	/**
	 * Ranks the documents of the current query using tf-idf. The terms of the query are 
	 * stored in 'keys',  while the postings lists are stored in 'values'. 
	 * 
	 * @return The list of document IDs, sorted by tf-idf.
	 */
	public int[] rank(){
		//The scores of the documents
		ArrayList<DocScore> scores= new ArrayList<DocScore>();
		if(keys!=null)
		//for each term
		for(int i = 0; i<keys.size(); i++){
			if(keys.get(i)==null) continue;
			//for each document containing this term.
			for(PostingWritable posting: values.get(i)){
				DocScore score = new DocScore(posting.getDocNo(), 0);
				//fetch the score of this document (see DocScore.equals for explanation)
				int index = scores.indexOf(score);
				//if the score is not present, initialize it
				if(index == -1){
					scores.add(score);
					index = scores.size()-1;
				}else score = scores.get(index);
				
				//accumulate to the score
				score.score+= (1+Math.log(posting.getTf()))*Math.log10(N/keys.get(i).getDf());
			}
		}
		//sort scores (see DocScore.compareTo for details)
		Collections.sort(scores);
		
		//restrict results to a maximum of 10
		int [] ret = new int[Math.min(10,scores.size())];
		for (int i = 0; i < ret.length; i++) {
			ret[i] = scores.get(i).docId;
		}
		return ret;
	}

	/**
	 * The main method. Iterates asking the user for queries, and presents results for each.
	 * 
	 * @param args 2 (and 1 optional):
	 * <ul>
	 * The inverted index directory
	 * The forward index file 
	 * [optional] The mapping file so as to display doc ids instead of numbers
	 * </ul> 
	 * 
	 * <br><br>Usage:
	 * <pre>
	 * hadoop jar cloud9.jar sa.edu.kaust.fwindex.IntDocVectorsForwardIndex \
	 * ./term_index_dir/ \
	 * ./fwrd_index_dir/fwrd_index \
	 * [./mapping_file.mapping]
	 * </pre>
	 */
	public static void main(String[] args) throws Exception {
		if (args.length < 2 || args.length>3) {
			System.out.println("usage: [term_index_dir] [fwrd_index]");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();
		DocnoMapping mDocMapping = null;
		
		//optional input: mapping file, to present user with doc IDs as results
		if (args.length>2) {
			mDocMapping = (DocnoMapping) TrecDocnoMapping.class
					.newInstance();
			
				FileSystem fs = FileSystem.get(conf);
				String mappingFile = args[2];
				mDocMapping.loadMapping(new Path(mappingFile), fs);
			
//				DistributedCache.addCacheFile(new URI(args[2]), conf);
//				Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
//				mDocMapping.loadMapping(localFiles[0],
//						FileSystem.getLocal(conf));
		}
		
		IntDocVectorsForwardIndex index = 
				new IntDocVectorsForwardIndex(args[0], args[1], FileSystem.get(conf));

		//get the number of documents, which is stored as the document frequency of the term " " 
		index.getValue(new String[]{" "});
		N = keys.get(0).getDf();
		//sLogger.info("N = "+N);
		
		//Tokenizer to tokenize and preprocess user input
		GalagoTokenizer tokenizer = new GalagoTokenizer();
		
		System.out.println("Welcome to The Abdo Search Engine.\nPlease type a query of" +
				" one or two words.\nType an empty query to terminate ...");
		
		String term = null;
		BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
		System.out.print("Look up postings query > ");
		while ((term = stdin.readLine()) != null) {
			term = term.trim();
			
			//check if empty query
			if(term.length()==0) break;
			
			//The original terms of the query. needed to find the actual number of 
			//tokens that the user entered, as some may be dropped by tokenizer.
			String[] origQ = term.split("\\s+");
			
			//The terms returned by the tokenizer
			String[] q = tokenizer.processContent(term);
			
			if(origQ.length==1 || origQ.length==2){
				//load the keys and values
				index.getValue(q);
				System.out.print(term + ": ");
				
				//if cannot map: print the doc numbers
				int[] res = index.rank();
				if(mDocMapping==null){
					if(res.length>0)
						System.out.print(Arrays.toString(res));
					else
						System.out.print("No results ...");
				}else{
					//else print the doc IDs
					for (int docno : res) {
						System.out.print(mDocMapping.getDocid(docno)+" ");
					}
					if(res.length==0)
						System.out.print("No results ...");
				}
				System.out.println();
				
			}else break;
			System.out.print("Look up postings query > ");
		}
	}
}

/**
 * A class for storing a document's score.
 * @author Abdurrahman
 */
class DocScore implements Comparable<DocScore>{
	int docId;
	double score;
	
	public DocScore(int docId, double score) {
		super();
		this.docId = docId;
		this.score = score;
	}

	
	@Override
	public int hashCode() {
		return docId;
	}
	
	/**
	 * A 'hacky' way to get DocScores of the same document to be equal. This is needed to get
	 * the index of the score of a document in a collection.
	 * Note that this is inconsistent with the compareTo function. I.e.:
	 * s1.equals(s2)
	 * does not necessarily mean
	 * s1.compareTo(s2) == 0
	 */
	@Override
	public boolean equals(Object obj) {
		if(! (obj instanceof DocScore)) return false;
		return ((DocScore)obj).docId == docId;
	}
	
	/**
	 * Used to sort DocScores according to score.
	 */
	@Override
	public int compareTo(DocScore o) {
		return (int)Math.ceil(o.score-score);
	}
	
	@Override
	public String toString() {
		return "<"+docId+", "+score+">";
	}
}
