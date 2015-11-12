package com.nik.patent.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class  PatentCitationMapper  extends Mapper<Text, Text, Text, Text> { 
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
     String[] citation = key.toString().split( "," );
     Text cited = new Text(citation[1]); 
     Text citing = new Text(citation[0]);
      
     context.write(cited, citing);
    }
  }