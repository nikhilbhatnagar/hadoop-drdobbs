package com.nik.patent.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PatentCitationReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)  throws  IOException, InterruptedException {
    String csv = "";
    for (Text value : values) {
      if (csv.length() >  0) { 
        csv +=  ","; 
    }
    csv += value.toString();
   }
   context.write(key,  new  Text(csv));
}
}
