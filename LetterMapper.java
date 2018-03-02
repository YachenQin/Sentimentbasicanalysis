package stubs;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LetterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private  Boolean caseSenstive;
  public void setup(Context context){
	  Configuration conf=context.getConfiguration();
	  caseSenstive=conf.getBoolean("paramname",true);
  }
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	    String line = value.toString();
	    if(caseSenstive){
	    	for (String word : line.split("\\W+")) {
	    		if (word.length() > 0) {
	    			int n=word.length();
	        	    char c=word.charAt(0);
	        	    String letter=String.valueOf(c);
	    		    context.write(new Text(letter), new IntWritable(n));
	    		    }		
	    		}
	    	}
	    else{
	    	String lines=line.toLowerCase();
	    	for (String word : lines.split("\\W+")) {
	    		if (word.length() > 0) {
	    			int n=word.length();
	        	    char c=word.charAt(0);
	        	    String letter=String.valueOf(c);
	    		    context.write(new Text(letter), new IntWritable(n));
	    		    }		
	    		}
	    }
  }
}
