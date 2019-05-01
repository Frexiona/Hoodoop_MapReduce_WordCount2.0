import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.io.IOException; 
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Set;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Text fileName = new Text();
    // Define lists for frequency counting
    public final static List<String> meList = Arrays.asList("me", "my", "mine");
    public final static List<String> weList = Arrays.asList("us", "our", "ours", "we");
    public final static List<String> politicsList = Arrays.asList("justice", "citizen", "war", "hegemony", "nationality");
    Set<String> stopwordsList;

    public void setup(Context context) throws FileNotFoundException{
    	stopwordsList = new HashSet<String>();
	Scanner stopword = new Scanner(new File("/home/frexiona/Documents/JavaFile/voyant_taporware.txt"));
	while(stopword.hasNext()){
		stopwordsList.add(stopword.next().trim());
	}
    }

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String words = value.toString();
      
      // Remove all the punctuation
      words = words.replaceAll("\\p{P}", "");
      // Lowercase the word
      words = words.toLowerCase();
      // Remove all the numbers
      words = words.replaceAll("[\\pN]", "");
      // Remove all the single letters
      words = words.replaceAll("(?:^| )[a-hj-z](?= |$)", "");
      // Remove all the non-alphabetic items
      words = words.replaceAll("[^a-zA-Z ]", "");
      
      StringTokenizer itr = new StringTokenizer(words);
      /*
      // Task 4: Get the most frequent ones from the documents
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
	if (meList.contains(word.toString())){
		context.write(new Text("me"), one);
	}
	else if (weList.contains(word.toString())){
		context.write(new Text("we"), one);
	}
      }
      */

      /*
      // Task 5: Find the words only appear in one document
      String filePathString = ((FileSplit) context.getInputSplit()).getPath().toString();
      while (itr.hasMoreTokens()){
      	word.set(itr.nextToken());
	fileName.set(localname);
	context.write(word, localname);
      }
      */
      /*
      // Task 6: Remove all the stop words in the documents
      while (itr.hasMoreTokens()){
        word.set(itr.nextToken());
        if (Arrays.asList(stopwordsList).contains(word.toString())){
                word.set(itr.nextToken());
        }
        else{
		word.set(itr.nextToken());
                context.write(word, one);
        }
      }
      */

      // Task 7: Find the top 5 frequent words after some political concepts
      while (itr.hasMoreTokens()){
      	if (Arrays.asList(stopwordsList).contains(word.toString())){
		word.set(itr.nextToken());
	}
	else if (politicsList.contains(word.toString())){
		word.set(itr.nextToken());
                context.write(word, one);
	}
	else{
		word.set(itr.nextToken());
	}
      }
      /*
      while (itr.hasMoreTokens()){
	      word.set(itr.nextToken());
	      context.write(word, one);
      }
      */
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      /*
      // Task 3: Only count the words which frequency more than 4 times (including 4 times)
      if (sum >= 4){
	      result.set(sum);
	      context.write(key, result);
      }
      */
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
