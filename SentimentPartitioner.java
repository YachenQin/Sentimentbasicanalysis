package stubs;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;


public class SentimentPartitioner extends Partitioner<Text, IntWritable> implements
    Configurable {
   private Configuration configuration;
   Set<String> positive = new HashSet<String>();
   Set<String> negative = new HashSet<String>();
  @Override
  public void setConf(Configuration configuration) {
	this.configuration=configuration;
	 try {
			FileReader f0 = new FileReader("positive-words.txt");
	        FileReader f1 = new FileReader("negative-words.txt");
	        BufferedReader f00=new BufferedReader(f0);
	        BufferedReader f11=new BufferedReader(f1);
	        String words=null;
	        StringBuffer p=new StringBuffer();
	        StringBuffer n=new StringBuffer();
	    	while((words=f00.readLine())!=null){
	    		if(words.substring(0,1)!=";")
	    		{
	    		p.append(words);
	    		this.positive.add(words);
	    		}
	    		}
	    	while((words=f11.readLine())!=null){
	    		if(words.substring(0,1)!=";")
	    		{
	    		n.append(words);
	            this.negative.add(words);
	    		}
	    	}
	        f00.close();
	        f11.close();
	      }
	      catch (IOException e) {
	    	  e.printStackTrace();
	    	  }
  }

  /**
   * Implement the getConf method for the Configurable interface.
   */
  @Override
  public Configuration getConf() {
    return configuration;
  }

  /**
   * You need to implement the getPartition method for a partitioner class.
   * This method receives the words as keys (i.e., the output key from the mapper.)
   * It should return an integer representation of the sentiment category
   * (positive, negative, neutral).
   * 
   * For this partitioner to work, the job configuration must have been
   * set so that there are exactly 3 reducers.
   */
  public int getPartition(Text key, IntWritable value, int numReduceTasks) {
	 if(positive.contains(key.toString())){
		 return 0;
	 }
	 else if(negative.contains(key.toString())){
		 return 1;
	 }
	 else{
		 return 2;
	 }

	 
    /*
     * TODO implement
     * Change the return 0 statement below to return the number of the sentiment 
     * category; use 0 for positive words, 1 for negative words, and 2 for neutral words. 
     * Use the sets of positive and negative words to find out the sentiment.
     *
     * Hint: use positive.contains(key.toString()) and negative.contains(key.toString())
     * If a word appears in both lists assume it is positive. That is, once you found 
     * that a word is in the positive list you do not need to check if it is in the 
     * negative list. 
     */
     //return 0;
  }
}

