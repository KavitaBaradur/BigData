package task;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class task1 {
    static String window = null;

    public static class task1Mapper extends Mapper<Object, Text, IntWritable, Text> {
        private Text word = new Text();
        static int x1, x2, y1, y2;

        protected void setup(Context context) {
            if (window != null) {
                String[] limit = window.split(",");
                x1 = Integer.parseInt(limit[0]);
                y1 = Integer.parseInt(limit[1]);
                x2 = Integer.parseInt(limit[2]);
                y2 = Integer.parseInt(limit[3]);
            }
        }


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            int bottom, up = 0;
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                String[] s = word.toString().split(",");
                if (s.length == 2) {
                    if (window != null) {
                        int point_x = Integer.parseInt(s[0]);
                        int point_y = Integer.parseInt(s[1]);
                        if (point_x < x1 || point_x > x2 || point_y < y1 || point_y > y2)
                            continue;
                    }
                    context.write(new IntWritable(Integer.parseInt(s[1])), word);
                }
                else {
                    if (window != null) {
                        int rec_x = Integer.parseInt(s[1]);
                        int rec_y = Integer.parseInt(s[2]);
                        int rec_x2 = Integer.parseInt(s[3]);
                        int rec_y2 = Integer.parseInt(s[4]);
                        if (rec_x < x1 || rec_x2 > x2 || rec_y < y1 || rec_y2 > y2)
                            continue;
                    }
                    bottom = Integer.parseInt(s[2]);
                    up = Integer.parseInt(s[4]);
                    int i = bottom;
                    while (i <= up) {
                        context.write(new IntWritable(i), word);
                        i++;
                    }
                }
            }
        }
    }

    public static class task1Reducer extends Reducer<IntWritable, Text, Text, Text> {

        Comparator<String> comppoint = new Comparator<String>() {
            public int compare(String a, String b) {
                String[] sta = a.split(",");
                String[] stb = b.split(",");
                Integer x1 = Integer.parseInt(sta[0]);
                Integer x2 = Integer.parseInt(stb[0]);
                return x2.compareTo(x1);
            }
        };
        Comparator<String> comprec = new Comparator<String>() {
            public int compare(String a, String b) {
                String[] sta = a.split(",");
                String[] stb = b.split(",");
                Integer x1 = Integer.parseInt(sta[2]);
                Integer x2 = Integer.parseInt(stb[2]);
                Integer w1 = Integer.parseInt(sta[4]);
                Integer w2 = Integer.parseInt(stb[4]);
                if (x1.equals(x2))
                    return w2.compareTo(w1);
                else
                    return x2.compareTo(x1);
            }
        };

        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            ArrayList<String> rects = new ArrayList<String>();
            ArrayList<String> points = new ArrayList<String>();

            // Initiate rects and points
            for (Text value : values) {
                String[] s = value.toString().split(",");
                if (s.length == 2) {
                    points.add(value.toString());
                }
                if (s.length == 5) {
                    rects.add(value.toString());
                }
            }

            Collections.sort(points, comppoint);
            Collections.sort(rects, comprec);

            // Join points with rects
            int start = 0;
            for (String rect : rects) {
                String[] r = rect.split(",");
                int rx = Integer.parseInt(r[1]);
                int rx2 = Integer.parseInt(r[3]);
                int ry = Integer.parseInt(r[2]);
                int ry2 = Integer.parseInt(r[4]);
                for (int i = start; i < points.size(); i++) {
                    String point = points.get(i);
                    String[] p = point.split(",");
                    int px = Integer.parseInt(p[0]);
                    int py = Integer.parseInt(p[1]);
                    if (px > rx) {
                        start = i;
                        continue;
                    }
                    if (px < rx2) {
                        break;
                    }
                    if (py < ry || py > ry2)
                        continue;
                    context.write(new Text(String.format("%-8s", r[0])), new Text("(" + point + ")"));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 2 && args.length != 3) {
            System.err.println("Usage: task1 <HDFS input file> <HDFS output file> <optional-W(x1,y1,x2,y2)>");
            System.exit(2);
        }
        Job job = new Job(conf, "task1");
        job.setJarByClass(task1.class);
        job.setMapperClass(task1Mapper.class);
        job.setReducerClass(task1Reducer.class);
        //job.setNumReduceTasks(2);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        if (args.length == 3)
            window = args[2];
        FileInputFormat.addInputPath(job, new Path(args[0]));
       FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}