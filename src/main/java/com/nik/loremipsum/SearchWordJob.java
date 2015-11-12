package com.nik.loremipsum;

import java.net.URI;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *  hadoop jar target/hadoop-drdobbs-1.0.0.jar com.nik.loremipsum.SearchWordJob -D mapred.reduce.tasks=4 (may output 4 files)
 *  hadoop fs -cat spark_output/part-r-00000
 * */
public class SearchWordJob extends Configured implements Tool {
	@Override
	public int run(String[] arg0) throws Exception {
		//final URI INPUT_URI = new URI("hdfs://localhost:54310/user/hduser/spark_data/PrideAndPrejudice.txt");
		//final URI INPUT_URI = new URI("hdfs://localhost:54310/user/hduser/spark_data/Ulysses.txt");
		//final URI INPUT_URI = new URI("hdfs://localhost:54310/user/hduser/spark_data/loremipsum.txt");
		final URI INPUT_URI = new URI("hdfs://localhost:54310/user/hduser/spark_data/simple.txt");
		
		final URI OUTPUT_URI = new URI("hdfs://localhost:54310/user/hduser/spark_output");
		
		Job job = new Job(getConf());
		job.setJarByClass(getClass());
		job.setJobName(getClass().getSimpleName());

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(SearchWordMapper.class);
		job.setCombinerClass(LoremipSumReducer.class);
		job.setReducerClass(LoremipSumReducer.class);

		FileInputFormat.addInputPath(job, new Path(INPUT_URI));
		
		if (FileSystem.get(getConf()).exists(new Path(OUTPUT_URI))) {
			FileSystem.get(getConf()).delete(new Path(OUTPUT_URI), true); // Delete output directory if exists
		}
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_URI));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int rc = ToolRunner.run(new SearchWordJob(), args);
		System.exit(rc);
	}
}
