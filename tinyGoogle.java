
import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;

/**
 *
 * @author Jacob Hershey
 */
public class tinyGoogle {

    public static int last;

    public static class RankMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private Text word = new Text();
        private String query;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            query = conf.get("queryString");
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] itr = value.toString().split(":");
            String[] queryArray = query.split(" ");
            for (int i = 0; i < queryArray.length; i++) {
                if (queryArray[i].equals(itr[0])) {
                    String[] tmp = itr[1].split("\\t", -1);
                    word.set(tmp[0]);
                    IntWritable numOccurences = new IntWritable(Integer.parseInt(tmp[1]));
                    context.write(word, numOccurences);
                    i = queryArray.length;
                }
            }
        }
    }

    public static class OrderMapper
            extends Mapper<Object, Text, IntWritable, Text> {

        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] itr = value.toString().split("\\t");
            word.set(itr[0]);
            IntWritable numOccurences = new IntWritable(Integer.parseInt(itr[1]));
            context.write(numOccurences, word);
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class OrderReducer
            extends Reducer<Text, IntWritable, IntWritable, Text> {

        public void reduce(IntWritable key, Text values, Context context) throws IOException, InterruptedException {
            context.write(key, values);
        }
    }

    public static void Index() throws Exception {
        System.out.println("\nPlease enter the HDFS path of the file(s) to index.: ");
        Scanner sc = new Scanner(System.in);
        String inputPath = sc.nextLine();

        //Begin mapReduce section
        Job invertedIndex = new Job();
        invertedIndex.setJarByClass(tinyGoogle.class);
        invertedIndex.setJobName("Inverted Index");
        FileInputFormat.addInputPath(invertedIndex, new Path(inputPath));
        FileOutputFormat.setOutputPath(invertedIndex, new Path("output"));
        invertedIndex.setMapperClass(IndexMapper.class);
        invertedIndex.setReducerClass(IndexReducer.class);
        invertedIndex.setOutputKeyClass(Text.class);
        invertedIndex.setOutputValueClass(IntWritable.class);
        invertedIndex.waitForCompletion(true);
    }

    public static void Rank() throws Exception {
        System.out.println("\nPlease enter a query, each keyword seperated by a space.: ");
        Scanner sc = new Scanner(System.in);
        String queryInput = sc.nextLine();//.split(" ");
        last = queryInput.split(" ").length;

        //Begin mapReduce section
        Configuration conf = new Configuration();
        conf.set("queryString", queryInput.toLowerCase());
        Job job = Job.getInstance(conf, "Rank");
        Job order = new Job();
        order.setJarByClass(tinyGoogle.class);
        job.setJarByClass(tinyGoogle.class);
        job.setMapperClass(RankMapper.class);
        order.setMapperClass(OrderMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        order.setReducerClass(OrderReducer.class);
        order.setOutputKeyClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        order.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("output"));
        FileOutputFormat.setOutputPath(job, new Path("rankedOutput"));
        FileInputFormat.addInputPath(order, new Path("rankedOutput"));
        FileOutputFormat.setOutputPath(order, new Path("results"));
        Path output = FileOutputFormat.getOutputPath(job);
        job.waitForCompletion(true);
        order.waitForCompletion(true);

        System.out.println(output);
    }

    public static void main(String[] args) throws Exception {
        int userInput = 0;
        Scanner sc = new Scanner(System.in);
        boolean invertedIndexExists = false;
        do {
            System.out.println("Welcome to tiny-Google."
                    + "\nEnter 1 to index document(s) in input path"
                    + "\nEnter 2 to enter a query to search for in the indexed documents."
                    + "\nEnter 3 to quit the program.\n");
            userInput = sc.nextInt();
            if (userInput == 1 && !invertedIndexExists) {
                Index();
                invertedIndexExists = true;
            }
            if (userInput == 2) {
                if (invertedIndexExists) {
                    Rank();
                } else {
                    System.out.println("Please index a document first to run RnR.");
                }
            } else if (userInput != 1 || userInput != 2 || userInput != 3) {
                System.out.println("Invalid tiny-Google menu option.");
            }
        } while (userInput != 3);
    }
}
