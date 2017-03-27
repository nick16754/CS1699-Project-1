import java.io.IOException;
import java.util.StringTokenizer;
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

public class tinyGoogle {
	public String[] queryInput = new String[5];
	
	public static class RankMapper
		extends Mapper<Object, Text, Text, IntWritable>
		{
			private Text word = new Text();

			public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
			{
				String[] itr = value.toString().split(",");
				for(int i = 0; i<queryInput.length(); i++)
				{
					if(queryInput[i].equals(itr[0]))
					{
						String[] tmp = itr[1].split(",");
						word.set(tmp[0]);
						IntWritable numOccurences = new IntWritable(Integer.parseInt(tmp[1]));
						context.write(word, numOccurences);
						i = queryInput.length();
					}
				}
			}
		}

	public static class IntSumReducer
		extends Reducer<Text,IntWritable,Text,IntWritable> 
		{
			private IntWritable result = new IntWritable();

			public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
			{
				int sum = 0;
				for (IntWritable val : values) {
					sum += val.get();
				}
				result.set(sum);
				context.write(key, result);
			}
		}

	public static void main(String[] args) throws Exception 
	{
		int userInput = 0;
		Scanner sc = new Scanner(System.in);
		do
		{
			System.out.println("Welcome to tiny-Google.\nEnter 1 to a document to index\nEnter 2 to enter a query to search for in the indexed documents.\nEnter 3 to quit the program.");
			//Scanner sc = new Scanner(System.in);
			userInput = sc.nextInt();
			if(userInput == 2)
				Rank();
		} while(userInput != 3);
		
		/*Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);*/
	}
	
	public static void Rank()
	{
		System.out.println("Please enter a query, each keyword seperated by a space.: ");
		Scanner sc = new Scanner(System.in);
		queryInput = sc.nextLine().split(" ");
		//Begin mapreduce section//
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Rank");
		job.setJarByClass(tinyGoogle.class);
		job.setMapperClass(RankMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path("output"));
		FileOutputFormat.setOutputPath(job, new Path("rankedOutput"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}