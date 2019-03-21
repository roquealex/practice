import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.LongWritable;
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

// Mapper:
import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;

// Reducer:
import org.apache.hadoop.mapreduce.Reducer;

public class MapOnlyWind extends Configured implements Tool {

  public static class LocalMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private Text wordObject = new Text();

    public static final int COL_TIME = 0;
    //public static final int COL_TEMPERATUREF = 1;
    public static final int COL_WINDSPEEDMPH = 6;

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      String[] columns = line.split(",");
      if (!columns[COL_TIME].equals( "Time")) {
        //String date = columns[COL_TIME].substring(0,10);
        double windSpeedMPH = Double.parseDouble(columns[COL_WINDSPEEDMPH]);
        if (windSpeedMPH >= 20.0) {
          wordObject.set(columns[COL_TIME]);
          context.write(wordObject, new DoubleWritable(windSpeedMPH));
        }
      }
    }
  }

  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.printf("Usage %s [opt] <indir> <outdir>\n",getClass().getSimpleName());
      return -1;
    }
    Job job = Job.getInstance(getConf());
    job.setJarByClass(MapOnlyWind.class);
    job.setJobName("Map Only 'SELECT Time, WindSpeedMPH WHERE WindSpeedMPH >= 20.0'");
    FileInputFormat.setInputPaths(job,new Path(args[0]));
    FileOutputFormat.setOutputPath(job,new Path(args[1]));

    job.setMapperClass(LocalMapper.class);
    job.setNumReduceTasks(0);
    // No reducer needed:
    //job.setReducerClass(LocalReducer.class);

    /*
    // These 4 lines seem to be useful for reduce only:
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(DoubleWritable.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(TupleWritable.class);
    */

    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;
  }
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new Configuration(), new MapOnlyWind(), args);
    System.exit(exitCode);
  }
}

