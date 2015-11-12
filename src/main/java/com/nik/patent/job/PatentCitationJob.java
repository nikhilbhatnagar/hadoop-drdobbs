package com.nik.patent.job;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.nik.patent.mapreduce.PatentCitationMapper;
import com.nik.patent.mapreduce.PatentCitationReducer;

/**
 * 
 * hadoop fs -mkdir data_patent output_patent
 * hadoop fs -put cite75_99.txt data_patent/cite75_99.txt
 * 
 * Run locally: data_patent output_patent
 * 
 * Run on HDFS: hadoop jar target/hadoop-drdobbs-1.0.0.jar \
					com.nik.patent.job.PatentCitationJob \
					-D mapred.reduce.tasks=4 \
					data_patent output_patent
					
 * hadoop fs -cat output_patent/part-r-00000 | head -20					
 * */
public class PatentCitationJob extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Job job = new Job(getConf());
	    job.setJarByClass(getClass());
	    job.setJobName(getClass().getSimpleName());
		
	    job.setMapperClass(PatentCitationMapper.class);
	    job.setReducerClass(PatentCitationReducer.class);
	    
	    job.setInputFormatClass(KeyValueTextInputFormat.class);
	    
	    job.setOutputFormatClass(TextOutputFormat.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    
	    System.out.println("\nWorking Directory : "+FileSystem.get(getConf()).getWorkingDirectory());
	    
	    if(FileSystem.get(getConf()).exists(new Path(args[1]))) {
	    	FileSystem.get(getConf()).delete(new Path(args[1]), true); // Delete output directory if exists	
	    }
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
	    int rc = ToolRunner.run(new PatentCitationJob(), args);
	    System.exit(rc);
	  }
}
