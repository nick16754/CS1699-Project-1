import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Scanner;
import java.util.HashMap;
import java.io.BufferedReader;
import java.io.InputStreamReader;


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

public class tinyGoogle {
	//public static String queryInput;
	public static int last;
	public static HashMap<String, Integer> rankMap = new HashMap();
	
	public static class RankMapper
		extends Mapper<Object, Text, Text, IntWritable>
		{
			private Text word = new Text();
			private String query;
			@Override
			protected void setup(Context context) throws IOException, InterruptedException{
				Configuration conf = context.getConfiguration();
				query = conf.get("queryString");
			}
			public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
			{
				String[] itr = value.toString().split(":");
				String[] queryArray = query.split(" ");
				for(int i = 0; i<queryArray.length; i++)
				{
						if(queryArray[i].equals(itr[0]))
						{
							String[] tmp = itr[1].split("\\t", -1);
							word.set(tmp[0]);
							IntWritable numOccurences = new IntWritable(Integer.parseInt(tmp[1]));
							context.write(word, numOccurences);
							i = queryArray.length;
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
				rankMap.put(key.toString(), Integer.parseInt(result.toString()));
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
	
	public static void Rank() throws Exception
	{
		System.out.println("Please enter a query, each keyword seperated by a space.: ");
		Scanner sc = new Scanner(System.in);
		String queryInput = sc.nextLine();//.split(" ");
		last = queryInput.split(" ").length;
		//Begin mapreduce section//
		Configuration conf = new Configuration();
		conf.set("queryString", queryInput);
		Job job = Job.getInstance(conf, "Rank");
		job.setJarByClass(tinyGoogle.class);
		job.setMapperClass(RankMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path("output"));
		FileOutputFormat.setOutputPath(job, new Path("rankedOutput"));
		Path output = FileOutputFormat.getOutputPath(job);
		job.waitForCompletion(true);
		
		//now we use the HashMap rankMap to retrieve the data.
		/*try{
		FileSystem fs = FileSystem.get(conf);
		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(new Path("rankedOutput"))));
		String line;
		line = br.readLine();
		while(line != null)
		{
			System.out.println(line);
			line=br.readLine();
		}
		}
		catch (Exception e){
		}*/
		/*FileSystem hdfs =FileSystem.get(conf);
		BufferedReader bfr=new BufferedReader(new InputStreamReader(hdfs.open(new Path("hdfs:rankedOutput"))));
		String str = null;
		while ((str = bfr.readLine())!= null)
		{
			System.out.println(str);
		}*/
			try{
            //Path pt=new Path(output);//Location of file in HDFS
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(output)));
            String line;
            line=br.readLine();
            while (line != null){
                System.out.println(line);
                line=br.readLine();
            }
			}catch(Exception e){
				System.out.println("Something went wrong while reading the file.");
			}

		
	}
}