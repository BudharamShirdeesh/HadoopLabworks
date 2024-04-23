import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Sort {

    public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

        private final static IntWritable bid1 = new IntWritable(1);
        private final static IntWritable bill_amt1 = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] result = value.toString().split(",");
            int bid = Integer.parseInt(result[0]);
            int bill_amt = Integer.parseInt(result[1]);

            bid1.set(bid);
            bill_amt1.set(bill_amt);

            context.write(bid1, bill_amt1);
        }
    }

    public static class IntSumReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {

        private Text result = new Text();

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;
            for (IntWritable val : values) {
                sum += val.get();
                count++;
            }
            int average = sum / count;
            result.set(Integer.toString(average));
            context.write(key, result);
        }
    }

    public static class SortMapper extends Mapper<IntWritable, Text, IntWritable, Text> {
        @Override
        public void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "average calculation");
    job.setJarByClass(Average.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1] + "_temp"));

    if (job.waitForCompletion(true)) {
      Job sortJob = Job.getInstance(conf, "sorting");
      sortJob.setJarByClass(Average.class);
      sortJob.setMapperClass(Mapper.class); // Use default identity mapper
      sortJob.setReducerClass(SortingReducer.class);
      sortJob.setOutputKeyClass(IntWritable.class);
      sortJob.setOutputValueClass(IntWritable.class);
      FileInputFormat.addInputPath(sortJob, new Path(args[1] + "_temp"));
      FileOutputFormat.setOutputPath(sortJob, new Path(args[1]));

      System.exit(sortJob.waitForCompletion(true) ? 0 : 1);
    } else {
      System.exit(1);
    }
  }
}
