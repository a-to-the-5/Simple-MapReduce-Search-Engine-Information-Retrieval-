package sa.edu.kaust.indexing;

import ivory.tokenize.GalagoTokenizer;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import sa.edu.kaust.io.PostingWritable;
import sa.edu.kaust.io.TermDF;

import edu.umd.cloud9.collection.DocnoMapping;
import edu.umd.cloud9.collection.trec.TrecDocument;
import edu.umd.cloud9.collection.trec.TrecDocumentInputFormat;
import edu.umd.cloud9.io.array.ArrayListWritable;

/**
 * <p>
 * A Hadoop MapReduce job that creates an term-k-gram inverted index for a 
 * collection of documents.
 * </p>
 * 
 * <ul>
 * <li>[K] k-gram parameter (1 for single term, 2 for bigram, etc)
 * <li>[input] path to the document collection
 * <li>[output-dir] path to the output directory
 * <li>[mappings-file] path to the docno mappings file
 * </ul>
 * 
 * <p>
 * Here's a sample invocation:
 * </p>
 * 
 * <blockquote>
 * 
 * <pre>
 * hadoop jar cloud9.jar sa.edu.kaust.indexing.TermKGramDocIndexer \
 * 2 \
 * ./data/sample-trec-small.xml \
 * ./output/ \
 * ./num_trec_doc_output/docno.mapping
 * </pre>
 * 
 * </blockquote>
 * 
 * @author Abdurrahman Ahmed (adapted from Jimmy Lin's DemoCountTrecDocuments)
 */
@SuppressWarnings("deprecation")
public class TermKGramDocIndexer extends Configured implements Tool {

	private static final Logger sLogger = Logger.getLogger(TermKGramDocIndexer.class);

	private static enum Count {
		DOCS
	};

	private static class MyMapper extends MapReduceBase implements
			Mapper<LongWritable, TrecDocument, TermDF, ArrayListWritable<PostingWritable>> {

		private DocnoMapping mDocMapping;
		//used to count the number of documents
		private static final TermDF DocCounter = new TermDF(new String[]{" "}, 0); 
		private TermDF current;
		private String[] kGram;
		private PostingWritable posting = new PostingWritable(0, 0);
		private ArrayListWritable<PostingWritable> postingslist = 
				new ArrayListWritable<PostingWritable>();
		private int docno;
		private int k;

		public void configure(JobConf job) {
			// load the docid to docno mappings
			try {
				mDocMapping = (DocnoMapping) Class.forName(job.get("DocnoMappingClass"))
				.newInstance();
				// Detect if we're in standalone mode; if so, we can't us the
				// DistributedCache because it does not (currently) work in
				// standalone mode...
				if (job.get("mapred.job.tracker").equals("local")) {
					FileSystem fs = FileSystem.get(job);
					String mappingFile = job.get("DocnoMappingFile");
					mDocMapping.loadMapping(new Path(mappingFile), fs);
				} else {
					Path[] localFiles = DistributedCache.getLocalCacheFiles(job);
					mDocMapping.loadMapping(localFiles[0], FileSystem.getLocal(job));
				}
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException("Error initializing DocnoMapping!");
			}
			postingslist.add(posting);
			k = Integer.parseInt(job.get("k"));
			kGram = new String[k];
			current = new TermDF(kGram, 1);
		}

		public void map(LongWritable key, TrecDocument doc,
				OutputCollector<TermDF, ArrayListWritable<PostingWritable>> output, 
				Reporter reporter) throws IOException {
			reporter.incrCounter(Count.DOCS, 1);
			docno = mDocMapping.getDocno(doc.getDocid());
			
			//To count the number of documents - each document emits this once
			output.collect(DocCounter, postingslist);
			
			//Tokenize the document
			String[] tokens = new GalagoTokenizer().processContent(doc.getContent());
			
			//set the posting object with this document's number
			posting.setDocNo(docno);
			posting.setTf(1);
						
			int i = 0;	
			
			//initialize the k-gram with the first k tokens
			for (i = 0; i < k && i<tokens.length; i++) {
				kGram[i] = tokens[i];
			}
			
			//If the document is shorter than k tokens, then nothing to do
			if(i<k) return;
			
			output.collect(current, postingslist);
			
			//for each new token
			for(;i<tokens.length;i++){
				//shift all tokens to the left
				for (int j = 0; j < k-1; j++) {
					kGram[j] = kGram[j+1];
				}
				//add the new token at the end
				kGram[k-1]=tokens[i];
				
				//collect this single k-gram (postingslist contains posting,
				//which in turn has kGram as its k_gram array)
				output.collect(current, postingslist);
			}
		}
	}

	private static class MyReducer extends MapReduceBase implements Reducer<TermDF,ArrayListWritable<PostingWritable>, 
		TermDF, ArrayListWritable<PostingWritable>>{
		
		
		@Override
		public void reduce(TermDF term, Iterator<ArrayListWritable<PostingWritable>> postingslists,
			OutputCollector<TermDF, ArrayListWritable<PostingWritable>> output,
			Reporter reporter) throws IOException {
			
			ArrayListWritable<PostingWritable> arr = new ArrayListWritable<PostingWritable>();
			
			//if this is the document counter
			if(term.getK_gram()[0].equals(" ")){
				while(postingslists.hasNext()){
					arr.addAll(postingslists.next());					
				}
				//store the number of documents in the document frequency 
				term.setDf(arr.size());
				output.collect(term, arr);
				return;
			}
			
			
			ArrayListWritable<PostingWritable> res = 
					new ArrayListWritable<PostingWritable>();
			
			while(postingslists.hasNext())
				res.addAll(postingslists.next());
			
			Collections.sort(res, new Comparator<PostingWritable>() {
				@Override
				public int compare(PostingWritable o1, PostingWritable o2) {
					return o1.getDocNo()-o2.getDocNo();
				}
			});
			
			ArrayListWritable<PostingWritable> res2 = 
					new ArrayListWritable<PostingWritable>();
			
			for(int i = 0; i<res.size(); i++){
				int sum = res.get(i).getTf();
				int j;
				for(j = i+1; j<res.size() && res.get(j).getDocNo()==res.get(i).getDocNo(); j++){
					sum+=res.get(j).getTf();
				}
				res2.add(new PostingWritable(res.get(i).getDocNo(), sum));
				i = j-1;
			}
			Collections.sort(res2);
			output.collect(term, res2);
		}
	}
	
	public TermKGramDocIndexer() {
	}

	
	private static int printUsage() {
		System.out.println("usage: [K] [input] [output-dir] [mappings-file]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	
	public int run(String[] args) throws Exception {
		sLogger.info("ARGS.LENGTH : "+args.length);
		if (args.length != 4) {
			printUsage();
			return -1;
		}

		String inputPath = args[1];
		String outputPath = args[2];
		String mappingFile = args[3];

		sLogger.info("input: " + inputPath);
		sLogger.info("output dir: " + outputPath);
		sLogger.info("docno mapping file: " + mappingFile);

		JobConf conf = new JobConf(TermKGramDocIndexer.class);
		conf.setJobName("TermKGramDocIndexer");
		conf.set("k", args[0]);

		conf.setNumReduceTasks(10);

		// Pass in the class name as a String; this is makes the mapper general
		// in being able to load any collection of Indexable objects that has
		// docid/docno mapping specified by a DocnoMapping object
		conf.set("DocnoMappingClass", "edu.umd.cloud9.collection.trec.TrecDocnoMapping");

		// put the mapping file in the distributed cache so each map worker will
		// have it
		//DistributedCache.addCacheFile(new URI(mappingFile), conf);
		if (conf.get("mapred.job.tracker").equals("local")) {
			conf.set("DocnoMappingFile", mappingFile);
		} else {
			DistributedCache.addCacheFile(new URI(mappingFile), conf);
		}


		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		FileOutputFormat.setCompressOutput(conf, false);

		conf.setInputFormat(TrecDocumentInputFormat.class);
		TrecDocumentInputFormat .setInputPaths(conf, new Path(inputPath));
		conf.setOutputKeyClass(TermDF.class);
		conf.setOutputValueClass(ArrayListWritable.class);

		conf.setMapperClass(MyMapper.class);
		conf.setCombinerClass(MyReducer.class);
		conf.setReducerClass(MyReducer.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);

		// delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		JobClient.runJob(conf);

		return 0;
	}

	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new TermKGramDocIndexer(), args);
		System.exit(res);
	}
}
