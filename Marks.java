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

public class Marks
{
    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>
    {

        private Text grade = new Text();
        private Text name = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            String[] res=value.toString().split(",");
            String naam=res[1];
            String grades=res[2];

            grade.set(grades);
            name.set(naam);

            context.write(grade,name);            
        }
    }

    public static class IntSumReducer extends Reducer<Text,Text,Text,Text>
    {
        private Text grade = new Text();
        private Text name = new Text();        

        public void reduce(Text key, Iterable<Text> values,Context context)throws IOException, InterruptedException
        {  
            
            if(!key.toString().equals("A"))
            {
                for(Text naam: values)
                {
                    context.write(key, naam);
                }
            }
            
        }
    }


    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "grade");
        job.setJarByClass(Marks.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
}
