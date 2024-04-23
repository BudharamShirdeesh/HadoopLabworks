import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DataOperations {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

    private Text studentName = new Text();
    private Text studentGrade = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] student = value.toString().split(",");
        if (student.length >= 5) {
            // Process lines with 5 or more elements
            String grade = student[4].trim();
            String name = student[1].trim();
            studentGrade.set(grade);
            studentName.set(name);
            context.write(studentGrade, studentName);
        } else if (student.length == 4) {
            // Process lines with exactly 4 elements
            // You can handle these lines differently or skip them altogether
            // For example, you could log them or count them as invalid
            context.getCounter("Mapper", "LinesWithFourElements").increment(1);
        } else {
            // Handle lines with less than 4 elements as invalid
            context.getCounter("Mapper", "InvalidInputLines").increment(1);
        }
    }
}


    public static class SelectReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder result = new StringBuilder();
            for (Text value : values) {
                if (result.length() > 0) {
                    result.append(", ");
                }
                result.append(value.toString());
            }
            context.write(key, new Text(result.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "output lines with 'O' in grade");
        job.setJarByClass(DataOperations.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(SelectReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

