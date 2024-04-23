import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class Chaining {

    public static class AverageMapper extends Mapper<Object, Text, Text, FloatWritable> {
        private Text branchId = new Text();
        private FloatWritable billAmount = new FloatWritable();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            if (tokens.length == 2) {
                branchId.set(tokens[0]);
                billAmount.set(Float.parseFloat(tokens[1]));
                context.write(branchId, billAmount);
            }
        }
    }

    public static class AverageReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        private FloatWritable result = new FloatWritable();

        @Override
        protected void reduce(Text key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {
            float sum = 0;
            int count = 0;
            for (FloatWritable value : values) {
                sum += value.get();
                count++;
            }
            if (count > 0) {
                result.set(sum / count);
                context.write(key, result);
            }
        }
    }

    public static class SortMapper extends Mapper<Object, Text, FloatWritable, Text> {
        private Text branchId = new Text();
        private FloatWritable average = new FloatWritable();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\t");
            if (tokens.length == 2) {
                branchId.set(tokens[0]);
                average.set(Float.parseFloat(tokens[1]));
                context.write(average, branchId);
            }
        }
    }

    public static class SortReducer extends Reducer<FloatWritable, Text, Text, FloatWritable> {
        @Override
        protected void reduce(FloatWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(value, key);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: Chaining <input path> <temp output path> <final output path>");
            System.exit(-1);
        }

        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Calculate Average");
        job1.setJarByClass(Chaining.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.setMapperClass(AverageMapper.class);
        job1.setReducerClass(AverageReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(FloatWritable.class);
        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Sort Average");
        job2.setJarByClass(Chaining.class);

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        job2.setMapperClass(SortMapper.class);
        job2.setReducerClass(SortReducer.class);
        job2.setMapOutputKeyClass(FloatWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(FloatWritable.class);

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}

