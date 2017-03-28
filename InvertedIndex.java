
import java.io.IOException;
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.*;

/**
 *
 * @author Nick Taglianetti
 */
public class InvertedIndex {

    public static class IndexMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
            String line = value.toString();
            // remove unwanted characters
            line = line.replaceAll("[\\p{P}+~$`^=|<>~'$^+=|<>]", "");
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                // as key, write word concatenated to filename with delimiter ":"
                word.set(itr.nextToken().toLowerCase() + ":" + filename);
                context.write(word, one);
            }
        }
    }

    public static class IndexReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            // as value, write sum of number of occurances of the word in the file
            result.set(sum);
            context.write(key, result);
        }
    }
    
        public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Inverted Index");
        job.setJarByClass(InvertedIndex.class);
        
        job.setMapperClass(IndexMapper.class);
        job.setCombinerClass(IndexReducer.class);
        job.setReducerClass(IndexReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
