/*
 * Ivory: A Hadoop toolkit for web-scale information retrieval
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

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.MapRunnable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import sa.edu.kaust.io.PostingWritable;
import sa.edu.kaust.io.TermDF;

import edu.umd.cloud9.io.array.ArrayListWritable;

/**
 * <p>
 * A Hadoop MapReduce job that creates a forward index for an inverted index for a 
 * collection of documents.
 * </p>
 * 
 * <ul>
 * <li>[inv-index] path to the inverted index directory
 * <li>[output-file] path to the output file
 * </ul>
 * 
 * <p>
 * Here's a sample invocation:
 * </p>
 * 
 * <blockquote>
 * 
 * <pre>
 * hadoop jar cloud9.jar sa.edu.kaust.fwindex.BuildIntDocVectorsForwardIndex \
 * ./inv_index_output/ \
 * ./output_dir/forward
 * </pre>
 * 
 * </blockquote>
 * 
 * @author Abdurrahman Ahmed (adapted)
 */
@SuppressWarnings("deprecation")
public class BuildIntDocVectorsForwardIndex extends Configured implements Tool {

	private static final Logger sLogger = Logger.getLogger(BuildIntDocVectorsForwardIndex.class);

	protected static enum Dictionary {
		Size
	};

	private static class MyMapRunner implements
			MapRunnable<TermDF, ArrayListWritable<PostingWritable>, TermDF, Text> {

		private String mInputFile;
		private Text outputValue = new Text();

		public void configure(JobConf job) {
			mInputFile = job.get("map.input.file"); // get the filename of the split
		}

		public void run(RecordReader<TermDF, ArrayListWritable<PostingWritable>> input,
				OutputCollector<TermDF, Text> output, Reporter reporter) throws IOException {
			TermDF key = input.createKey();
			ArrayListWritable<PostingWritable> value = input.createValue();
			int fileNo = Integer.parseInt(mInputFile.substring(mInputFile.lastIndexOf("-") + 1)); // get file no

			long pos = input.getPos(); // get curent position
			while (input.next(key, value)) {
				outputValue.set(fileNo + "\t" + pos);

				output.collect(key, outputValue);
				reporter.incrCounter(Dictionary.Size, 1);

				pos = input.getPos();
			}
			sLogger.info("last termid: " + key + "(" + fileNo + ", " + pos + ")");
		}
	}

	public static final long BigNumber = 1000000000;

	private static class MyReducer extends MapReduceBase implements
			Reducer<TermDF, Text, Text, Text> {

		FSDataOutputStream mOut;


		public void configure(JobConf job) {
			FileSystem fs;
			try {
				fs = FileSystem.get(job);
			} catch (Exception e) {
				throw new RuntimeException("Error opening the FileSystem!");
			}

			String forwardIndexPath = job.get("ForwardIndexPath");

			try {
				mOut = fs.create (new Path (forwardIndexPath), true);
			} catch (Exception e) {
				throw new RuntimeException("Error in creating files!");
			}

		}

		public void reduce(TermDF key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String[] s = values.next().toString().split("\\s+");

			if (values.hasNext())
				throw new RuntimeException("There shouldn't be more than one value, key=" + key);

			int fileNo = Integer.parseInt(s[0]);
			long filePos = Long.parseLong(s[1]);
			long pos = BigNumber * fileNo + filePos;

			//Atomically write both term and position to avoid intermingled writes
			//by different reducers.
			mOut.writeUTF(key.getK_gram()[0]+"\t"+pos);
		}

		public void close() throws IOException {
			mOut.close();
		}
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
		if (args.length != 2) {
			printUsage();
			return -1;
		}
		String inPath = args[0];
		String outPath = args[1];
		
		JobConf conf = new JobConf(getConf(), BuildIntDocVectorsForwardIndex.class);
		FileSystem fs = FileSystem.get(conf);

		int mapTasks = 10;
		sLogger.info("Tool: BuildIntDocVectorsIndex");

		String intDocVectorsPath = inPath;
		String forwardIndexPath = outPath;

		if (!fs.exists(new Path(intDocVectorsPath))) {
			sLogger.info("Error: IntDocVectors don't exist!");
			return 0;
		}

		if (fs.exists (new Path (forwardIndexPath))) {
			sLogger.info ("IntDocVectorsForwardIndex already exists: skipping!");
			return 0;
		}

		conf.set("ForwardIndexPath", forwardIndexPath);
		
		conf.setJobName("BuildIntDocVectorsForwardIndex");

		Path inputPath = new Path(intDocVectorsPath);
		FileInputFormat.setInputPaths(conf, inputPath);

		conf.setNumMapTasks(mapTasks);
		conf.setNumReduceTasks(1);

		conf.set("mapred.child.java.opts", "-Xmx2048m");

		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setMapOutputKeyClass(TermDF.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setOutputFormat(NullOutputFormat.class);

		conf.setMapRunnerClass(MyMapRunner.class);
		conf.setReducerClass(MyReducer.class);

		JobClient.runJob(conf);

		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new BuildIntDocVectorsForwardIndex(), args);
		System.exit(res);
	}
}
