
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
    public class IndexMapper extends Mapper<Object, Text, Text, IntWritable> {

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
