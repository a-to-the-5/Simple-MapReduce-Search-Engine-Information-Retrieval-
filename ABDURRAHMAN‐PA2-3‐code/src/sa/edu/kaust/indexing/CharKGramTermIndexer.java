package sa.edu.kaust.indexing;


import ivory.tokenize.GalagoTokenizer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


import edu.umd.cloud9.collection.trec.TrecDocument;
import edu.umd.cloud9.collection.trec.TrecDocumentInputFormat;
import edu.umd.cloud9.io.array.ArrayListWritable;

/**
 * <p>
 * A Hadoop MapReduce job that creates an character-k-gram inverted index for terms
 * in a collection of documents.
 * </p>
 * 
 * <ul>
 * <li>[K] k-gram parameter (1 for single term, 2 for bigram, etc)
 * <li>[input] path to the document collection
 * <li>[output-dir] path to the output directory
 * </ul>
 * 
 * <p>
 * Here's a sample invocation:
 * </p>
 * 
 * <blockquote>
 * 
 * <pre>
 * hadoop jar cloud9.jar sa.edu.kaust.indexing.CharKGramTermIndexer \
 * 2 \
 * ./data/sample-trec-small.xml \
 * ./output/ \
 * </pre>
 * 
 * </blockquote>
 * 
 * @author Abdurrahman Ahmed (adapted from Jimmy Lin's DemoCountTrecDocuments)
 */
@SuppressWarnings("deprecation")
public class CharKGramTermIndexer extends Configured implements Tool {

	private static final Logger sLogger = Logger.getLogger(CharKGramTermIndexer.class);

	private static enum Count {
		DOCS
	};

	private static class MyMapper extends MapReduceBase implements
			Mapper<LongWritable, TrecDocument, Text, ArrayListWritable<Text>> {

		private final static Text charGram = new Text();
		private Hashtable<String, HashSet<String>> map = 
				new Hashtable<String, HashSet<String>>();
		private OutputCollector<Text, ArrayListWritable<Text>> output;
		private ArrayListWritable<Text> list = new ArrayListWritable<Text>();
		private int k;

		public void configure(JobConf job) {
			k = Integer.parseInt(job.get("k"));
		}

		public void map(LongWritable key, TrecDocument doc,
				OutputCollector<Text, ArrayListWritable<Text>> output, Reporter reporter) throws IOException {
			reporter.incrCounter(Count.DOCS, 1);
			
			//save the output collector to use it in the close method
			this.output = output;
			
			//tokenize the document
			String[] tokens = new GalagoTokenizer().processContent(doc.getContent());
			
			for(String token:tokens){
				token = '$'+token+'$';
				for (int i = 0; i < token.length()-k+1; i++) {
					//get the ith k-gram from this term
					String gram = token.substring(i,i+k);
					//See if this k-gram appearse before (has a set in the map)
					HashSet<String> h = map.get(gram);
					if(h==null) h = new HashSet<String>();
					//add this token to the set
					h.add(token.substring(1, token.length()-1));
					map.put(gram, h);
				}
			}
		}
		
		@Override
		public void close() throws IOException {
			//In mapper combining. The contents of the map are emitted here.
			
			//for each k-gram:
			for(String key : map.keySet()){
				charGram.set(key);
				list.clear();
				//Add all its tokens to the list
				for (String s : map.get(key)) {
					list.add(new Text(s));
				}
				//emit the k-gram with the list.
				output.collect(charGram,list);
			}
			super.close();
		}
	}
	
	private static class MyReducer extends MapReduceBase implements 
		Reducer<Text, ArrayListWritable<Text>, Text, ArrayListWritable<Text>>{
		
		@Override
		public void reduce(Text key, Iterator<ArrayListWritable<Text>> values,
				OutputCollector<Text, ArrayListWritable<Text>> output,
				Reporter reporter) throws IOException {
			/*
			 * This method will attempt to merge the lists of Terms.
			 * The merging will be done in an iterable fashion while using
			 * the same complexity as the recursive implementation.
			 */
			ArrayList<ArrayListWritable<Text>> result, result2;
			while(true){
				result = 
					new ArrayList<ArrayListWritable<Text>>();
				while(values.hasNext()){
					//Here we try to merge every two consecutive lists
					
					//Get first list
					ArrayListWritable<Text> temp = values.next();
					if(!values.hasNext()){
						//If this is the last one, add to result and break
						result.add(temp);
						break;
					}
					//Get second list
					ArrayListWritable<Text> temp2 = values.next();
					
					//merge both lists
					result.add(merge(temp,temp2));
				}
				//if only one list remains, then all is merged; we are done!
				if(result.size()==1)break;
				//we now need to merge again the lists created in this iteration 
				result2 = result;
				values = result2.iterator();
			}
			output.collect(key, result.get(0));
		}
		
		private ArrayListWritable<Text> merge(
				ArrayListWritable<Text> temp,
				ArrayListWritable<Text> temp2) {
			ArrayListWritable<Text> result = 
					new ArrayListWritable<Text>();

			Iterator<Text> a = temp.iterator();
			Iterator<Text> b = temp2.iterator();
			Text t1 = a.next(), t2 = b.next();
			
			//Looping over the two lists
			while(a.hasNext() && b.hasNext()){
				//add the smaller posting to the result
				if(t1.toString().compareTo(t2.toString())<0){
					result.add(t1);
					t1 = a.next();
				}else if(t1.toString().compareTo(t2.toString())>0){
					result.add(t2);
					t2 = b.next();
				}else{ 
					result.add(t1);
					t1 = a.next();
					t2 = b.next();
				}
			}
			result.add(t1);
			result.add(t2);
			//Empty what remains of temp
			while(a.hasNext()){
				result.add(a.next());
			}
			//Empty what remains of temp2
			while(b.hasNext()){
				result.add(b.next());
			}
			return result;
		}
		
	}

	/**
	 * Creates an instance of this tool.
	 */
	public CharKGramTermIndexer() {
	}

	private static int printUsage() {
		System.out.println("usage: [input] [output-dir] [mappings-file]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	/**
	 * Runs this tool.
	 */
	public int run(String[] args) throws Exception {
		sLogger.info("ARGS.LENGTH : "+args.length);
		if (args.length != 3) {
			printUsage();
			return -1;
		}

		String inputPath = args[1];
		String outputPath = args[2];

		sLogger.info("input: " + inputPath);
		sLogger.info("output dir: " + outputPath);

		JobConf conf = new JobConf(CharKGramTermIndexer.class);
		conf.setJobName("CharKGramTermIndexer");
		conf.set("k", args[0]);

		conf.setNumReduceTasks(10);

		// Pass in the class name as a String; this is makes the mapper general
		// in being able to load any collection of Indexable objects that has
		// docid/docno mapping specified by a DocnoMapping object
		conf.set("DocnoMappingClass", "edu.umd.cloud9.collection.trec.TrecDocnoMapping");
		
		FileInputFormat.setInputPaths(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		FileOutputFormat.setCompressOutput(conf, false);

		conf.setInputFormat(TrecDocumentInputFormat.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(ArrayListWritable.class);

		conf.setMapperClass(MyMapper.class);
		conf.setReducerClass(MyReducer.class);

		// delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		JobClient.runJob(conf);

		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the
	 * <code>ToolRunner</code>.
	 */
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new CharKGramTermIndexer(), args);
		System.exit(res);
	}
}
