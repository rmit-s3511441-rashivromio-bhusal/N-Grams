package au.rmit.bde;



import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class WordCountJobConf extends Configured implements Tool {

	// check the logs here
	private static final Logger log = LoggerFactory.getLogger(WordCountJobConf.class);

	// Map Class
	
	static public class WordCountMapper extends
	Mapper<LongWritable, Text, Text, Text>{

		final private static LongWritable ONE = new LongWritable(1);
		@Override
		protected void map(LongWritable offset, Text text, Context context)
				throws IOException, InterruptedException {

			//FORMAT OF GOOGLEBOOKS N-GRAM DATASET
			//eg. for the 5-Gram phrase "lets get out of here"
			//
			// nGram					year	occurrences		pages	no.of books	
			//
			//lets get out of here		1998	235				235		235

			//checking the logs for the input text
			log.info("input text: "+text);
			String[] nGram = text.toString().split("\\s+");
				int year =  Integer.parseInt(nGram[1]);
				Text mapResult = new Text();
				Text mapKey = new Text();
				
				//television,internet,radio,newspaper,mobilephones
					if(nGram[0].equalsIgnoreCase("television")
							|| nGram[0].equalsIgnoreCase("internet") 
							|| nGram[0].equalsIgnoreCase("radio")
							|| nGram[0].equalsIgnoreCase("newspaper") 
							|| nGram[0].equalsIgnoreCase("mobilephones")
							)

					{
						// check the year from 1808 to 1908 which is 100 years
						//log.info("year:"+nGram[1]);
						if(year >= 1808 && year <  1908)
						{
						mapKey = new Text("1808-to-1908 == 100-years");
						mapResult = new Text(new LongWritable(Long.parseLong(nGram[2]))+" ##"+new LongWritable(Long.parseLong(nGram[3]))+ 
								"##" +new LongWritable(Long.parseLong(nGram[4])));
						}
						// check the year from 1908 to 2008 which is 100 years
						else if(year >= 1908 && year <  2008)
						{
							//key 
							mapKey = new Text("1908-to-2008 == next-100-years");
							//value
							mapResult =new Text(new LongWritable(Long.parseLong(nGram[2]))+" ##"+new LongWritable(Long.parseLong(nGram[3]))+ "##" + new LongWritable(Long.parseLong(nGram[4])));
							
						}
						context.write(new Text(nGram[0]+"--"+mapKey),mapResult);
				}
							}
	}
	// Reducer
	//static public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> 
	static public class WordCountReducer extends Reducer<Text, Text, Text, Text> 

	{
		@Override
		protected void reduce(Text token, Iterable<Text> counts, Context context) throws IOException, InterruptedException
		{
			// Calculate sum of counts
			Text t = new Text();
			long occ = 0;
			long noOfBooks = 0;
			long pages =0;
			for(Text count :counts){
				//check if it is empty or not
				if(!count.toString().isEmpty()){
				String[] values = count.toString().split("##");
				// sum here
				occ = occ+Long.parseLong(values[0].trim());
				pages = pages+Long.parseLong(values[1].trim());
				noOfBooks =noOfBooks+Long.parseLong(values[2].trim());
				}
			}
			t.set(occ + "##" + pages + "##" + noOfBooks );
			context.write(token, t);
		}
	}



	public int run(String[] args) throws Exception {
		Configuration configuration = getConf();

		// Initialising Map Reduce Job
		Job job = new Job(configuration, "Information flow");

		// Set Map Reduce main jobconf class
		job.setJarByClass(WordCountJobConf.class);

		// Set Mapper class
		job.setMapperClass(WordCountMapper.class);

		// Set Combiner class
		job.setCombinerClass(WordCountReducer.class);

		// set Reducer class
		job.setReducerClass(WordCountReducer.class);
		
		// set Input Format
		job.setInputFormatClass(SequenceFileInputFormat.class);

		//job.setInputFormatClass(TextInputFormat.class);

		// set Output Format
		job.setOutputFormatClass(TextOutputFormat.class);

		// set Output key class
		job.setOutputKeyClass(Text.class);

		// set Output value class
		job.setOutputValueClass(Text.class);
		
		//job.setOutputValueClass(LongWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : -1;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new WordCountJobConf(), args));
	}
}
