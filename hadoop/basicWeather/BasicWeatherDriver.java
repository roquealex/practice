import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BasicWeatherDriver extends Configured implements Tool {
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.printf("Usage %s [opt] <indir> <outdir>\n",getClass().getSimpleName());
      return -1;
    }
    //Job job = new Job(getConf());
    Job job = Job.getInstance(getConf());
    job.setJarByClass(BasicWeatherDriver.class);
    job.setJobName("Word count");
    //FileInputFormat.setInputPaths(job,args[0]);
    FileInputFormat.setInputPaths(job,new Path(args[0]));
    //FileOutputFormat.setOutputPath(job,args[1]);
    FileOutputFormat.setOutputPath(job,new Path(args[1]));

    job.setMapperClass(BasicWeatherMapper.class);
    job.setReducerClass(BasicWeatherReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(DoubleWritable.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(TupleWritable.class);

    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;
  }
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new Configuration(), new BasicWeatherDriver(), args);
    System.exit(exitCode);
  }
}

