package task1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JsonMapper extends Mapper<LongWritable, Text, LongWritable, IntWritable> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
	
		//context.write(new Text(value), new Text(""));

		String[] records = value.toString().split(",");
					 
		String ElevationTuple = records[8];
		 
		String[] Elevation = ElevationTuple.trim().split(":");
		
		IntWritable One =new IntWritable(1);
		
		LongWritable lo=new LongWritable(Long.parseLong(Elevation[1].trim().replaceAll("\"","")));
		
		context.write(lo,One);

	}
}
