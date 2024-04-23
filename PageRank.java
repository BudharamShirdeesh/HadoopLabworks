import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;

public class PageRank {

    public static class PageRankMapper extends Mapper<Object, Text, Text, Text> {
        
    private Text destWebsite = new Text();
    private Text srcWithDistance = new Text();
        
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().trim().split("\\s+");
        if (parts.length < 2) {
            return;
        }
        String src = parts[0];
        String[] dests = parts[1].split(",");
        for (String dest : dests) {
            destWebsite.set(dest);
            srcWithDistance.set(src + "\t1");
            context.write(destWebsite, srcWithDistance);
        }
    } 
} 



  public static class PageRankReducer extends Reducer<Text, Text, Text, Text> {
    
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int clickDistance = context.getConfiguration().getInt("clickDistance", 0);
        StringBuilder connectingNodes = new StringBuilder();
        for (Text value : values) {
            String[] parts = value.toString().split("\t");
            String src = parts[0];
            int distance = Integer.parseInt(parts[1]);
            if (distance == clickDistance - 1) {
                if (connectingNodes.length() > 0) {
                    connectingNodes.append(",");
                }
                connectingNodes.append(src);
            }
        }
        if (connectingNodes.length() > 0) {
            context.write(key, new Text("(" + connectingNodes.toString() + ")"));
        }
    }
}


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInt("clickDistance", Integer.parseInt(args[0]));
        Job job = Job.getInstance(conf, "PageRank");
        job.setJarByClass(PageRank.class);
        job.setMapperClass(PageRankMapper.class);
        job.setReducerClass(PageRankReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
