package com.nik.loremipsum;

import java.io.IOException;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *  hadoop jar target/hadoop-drdobbs-1.0.0.jar com.nik.loremipsum.WordCount -D mapred.reduce.tasks=4
 *  hadoop fs -cat spark_output/part-r-00000
 * */
public class WordCount {

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				context.write(word, one);
			}
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		//final URI INPUT_URI = new URI("hdfs://localhost:54310/user/hduser/spark_data/PrideAndPrejudice.txt");
		//final URI INPUT_URI = new URI("hdfs://localhost:54310/user/hduser/spark_data/Ulysses.txt");
		//final URI INPUT_URI = new URI("hdfs://localhost:54310/user/hduser/spark_data/loremipsum.txt");
		final URI INPUT_URI = new URI("hdfs://localhost:54310/user/hduser/spark_data/simple.txt");
		
		final URI OUTPUT_URI = new URI("hdfs://localhost:54310/user/hduser/spark_output");
		
		Configuration conf = new Configuration();

		Job job = new Job(conf, "wordcount");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(INPUT_URI));
		
		if (FileSystem.get(conf).exists(new Path(OUTPUT_URI))) {
			FileSystem.get(conf).delete(new Path(OUTPUT_URI), true); // Delete output directory if exists
		}
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_URI));

		job.waitForCompletion(true);
	}
}