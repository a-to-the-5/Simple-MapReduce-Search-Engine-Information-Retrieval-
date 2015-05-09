/*
 * Cloud9: A MapReduce Library for Hadoop
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

package sa.edu.kaust.indexing;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

import sa.edu.kaust.io.TermDF;

import edu.umd.cloud9.collection.DocnoMapping;
import edu.umd.cloud9.collection.trec.TrecDocument;
import edu.umd.cloud9.collection.trec.TrecDocumentInputFormat;

/**
 * <p>
 * Simple demo program that counts all the documents in the TREC collection.
 * This provides a skeleton for MapReduce programs to process the collection.
 * The program takes three command-line arguments:
 * </p>
 * 
 * <ul>
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
 * hadoop jar cloud9.jar edu.umd.cloud9.collection.trec.DemoCountTrecDocuments \
 * /umd/collections/trec/trec4-5_noCR.xml \
 * /user/jimmylin/count-tmp \
 * /user/jimmylin/docno.mapping
 * </pre>
 * 
 * </blockquote>
 * 
 * @author Jimmy Lin
 */
public class DemoCountTrecDocuments extends Configured implements Tool {

	private static final Logger sLogger = Logger.getLogger(DemoCountTrecDocuments.class);

	private static enum Count {
		DOCS
	};

	private static class MyMapper extends MapReduceBase implements
			Mapper<LongWritable, TrecDocument, Text, IntWritable> {

		private final static Text sText = new Text();
		private final static IntWritable sInt = new IntWritable(1);
		private DocnoMapping mDocMapping;

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
		}

		public void map(LongWritable key, TrecDocument doc,
				OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			reporter.incrCounter(Count.DOCS, 1);

			sText.set(doc.getDocid());
			sInt.set(mDocMapping.getDocno(doc.getDocid()));
			output.collect(sText, sInt);
		}
	}
	
	private static class MyReducer extends MapReduceBase implements
	Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text docid, Iterator<IntWritable> docno,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			int max = -1;
			while(docno.hasNext())
				max = Math.max(max, docno.next().get());
			output.collect(new Text(), new IntWritable(max));
		}
		
	}

	/**
	 * Creates an instance of this tool.
	 */
	public DemoCountTrecDocuments() {
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
		if (args.length != 3) {
			printUsage();
			return -1;
		}

		String inputPath = args[0];
		String outputPath = args[1];
		String mappingFile = args[2];

		sLogger.info("input: " + inputPath);
		sLogger.info("output dir: " + outputPath);
		sLogger.info("docno mapping file: " + mappingFile);

		JobConf conf = new JobConf(DemoCountTrecDocuments.class);
		conf.setJobName("DemoCountTrecDocuments");

		conf.setNumReduceTasks(0);

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
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(MyMapper.class);

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
		int res = ToolRunner.run(new Configuration(), new DemoCountTrecDocuments(), args);
		System.exit(res);
	}
}
