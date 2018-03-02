package stubs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.junit.Test;

public class SentimentPartitionTest {

	SentimentPartitioner mpart;
	@Test
	public void testSentimentPartition() {
		mpart = new SentimentPartitioner();
		mpart.setConf(new Configuration());
		int result1,result2,result3;		
		IntWritable a=new IntWritable(3);
		Text b1=new Text("love");
		result1=mpart.getPartition(b1,a,3);
		System.out.println(result1);
		Text b2=new Text("deadly");
		result2=mpart.getPartition(b2,a,3);
		System.out.println(result2);;
		Text b3=new Text("zodiac");
		result3=mpart.getPartition(b3,a,3);
		System.out.println(result3);
		/*
		 * Test the words "love", "deadly", and "zodiac". 
		 * The expected outcomes should be 0, 1, and 2. 
		 */        
		
	}

}

