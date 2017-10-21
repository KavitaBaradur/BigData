package task1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JasonToCommaSeparatedFile {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf = new Configuration();

		conf.set("jsoninput.start", "{");
		conf.set("jsoninput.end", "}");
		conf.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");

		Job job = new Job(conf, "Json Parsing");

		FileInputFormat.setInputPaths(job, args[0]);
		job.setJarByClass(JasonToCommaSeparatedFile.class);
		job.setMapperClass(JsonMapper.class);
		job.setReducerClass(JsonReducer.class)	;									
		job.setInputFormatClass(JsonInputFormat.class);	 //set Input Format
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		Path outPath = new Path(args[1]);
		
		FileOutputFormat.setOutputPath(job, outPath);
		
		FileSystem dfs = FileSystem.get(outPath.toUri(), conf);
		
		if (dfs.exists(outPath))
		{
			dfs.delete(outPath, true);
		}

		job.waitForCompletion(true);

	
	}
}
