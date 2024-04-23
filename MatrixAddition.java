

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MatrixAddition{

    public static class MatrixMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable matrixValue = new IntWritable();
        private Text matrixKey = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
           

            String[] result = value.toString().split("@");
   	    String k = result[0];
   	    int val = Integer.parseInt(result[1]);
            matrixValue.set(val);
	    matrixKey.set(k);
            context.write(matrixKey, matrixValue);
        }
    }

    public static class MatrixReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
       Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Matrix Addition");
    job.setJarByClass(MatrixAddition.class);
    job.setMapperClass(MatrixMapper.class);
    job.setCombinerClass(MatrixReducer.class);
    job.setReducerClass(MatrixReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
     TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

