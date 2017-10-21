package task1;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class JsonReducer extends Reducer<LongWritable,IntWritable,LongWritable,IntWritable> {
	private IntWritable result = new IntWritable();
	
	public void reduce(LongWritable key, Iterable<IntWritable> values, 
            Context context) throws IOException, InterruptedException {
			int sum = 0;
			
			for (IntWritable val : values) {
			 sum += val.get();
		}
			
	result.set(sum);	
	context.write(key, result);	
	}
}
