import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MatrixTranspose {

    public static class MatrixMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text matrixKey = new Text();
        private IntWritable matrixValue = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] result = value.toString().split("@");
            String row = result[0];
            String col = result[1];
            int val = Integer.parseInt(result[2]); 
            matrixKey.set(col + "@" + row);
            matrixValue.set(val);
            context.write(matrixKey, matrixValue);
        }
    }

    public static class MatrixReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            
            for (IntWritable val : values) {
                context.write(key, val);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Matrix Transpose");
        job.setJarByClass(MatrixTranspose.class);
        job.setMapperClass(MatrixMapper.class);
        job.setReducerClass(MatrixReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
