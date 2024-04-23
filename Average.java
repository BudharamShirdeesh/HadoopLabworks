//The input to this program is a dataset that is pre generated. It contains Bill Information of a certain outlet having different branches.The data we deal is the bill data which contains the Branch ID and Bill Amount.The output of the MapReduce program is a set of key value pairs where the key is the branch ID and the value is the average bill amount for that branch.Each record is split into tokens using comma. Used Map Reduce Method.



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

public class Average
{

  public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, IntWritable>
  {

    private final static IntWritable bid1 = new IntWritable(1);
    private final static IntWritable bill_amt1 = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
    	String[] result = value.toString().split(",");
    	int bid = Integer.parseInt(result[0]);
    	int bill_amt = Integer.parseInt(result[1]);
    	
    	bid1.set(bid);
    	bill_amt1.set(bill_amt);
    	
    	context.write(bid1, bill_amt1);
    	
    }
  }

  public static class IntSumReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      int count = 0;

      for (IntWritable val : values) {
        sum += val.get();
        count++;
      }

      int average = sum / count;
      result.set(average);
      context.write(key, result);
    }
  }
  

  public static void main(String[] args) throws Exception
  {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(Average.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
