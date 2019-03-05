import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

// Mapper:
import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;

// Reducer:
import org.apache.hadoop.mapreduce.Reducer;

/*
 * History:
javac -classpath `$HADOOP_HOME/bin/hadoop classpath` *.java
jar cvf timeRank.jar *.class
$HADOOP_HOME/bin/hadoop jar timeRank.jar TimeRank datasets/random/ timeRank7
hadoop fs -cat timeRank7/* > out.txt
*/
public class TimeRank extends Configured implements Tool {

  public static class LocalMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text wordObject = new Text();

    public static final int COL_TIME = 0;
    public static final int COL_TEMPERATUREF = 1;
    //public static final int COL_WINDSPEEDMPH = 6;

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      String[] columns = line.split(",");
      if (!columns[COL_TIME].equals( "Time")) {
        String date = columns[COL_TIME].substring(0,10);
        String time = columns[COL_TIME].substring(11);
        //double temp = Double.parseDouble(columns[COL_TEMPERATUREF]);
        wordObject.set(columns[COL_TIME]);
        //wordObject.set(date);
        //context.write(wordObject, new DoubleWritable(temp));
        context.write(wordObject, new Text(time));
      }
    }

  }


  public static class LocalReducer extends Reducer<Text,Text,Text,Text> {
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      //double maxTemp = -9999.0;
      //double minTemp = 9999.0;
      //String finalTime = new String();
      int rank = 0;
      for( Text time : values) {
        //wordCount += count.get();
        String currTime = time.toString();
        /*
        if (rank==0 || currTime.compareTo(finalTime) < 0 ) {
          finalTime = currTime;
        }
        context.write(key,new Text(String.format("%s rank %d",finalTime,rank)));
        */
        // It is incredible the key is still different between entries:
        //if ((rank%5)==0) context.write(key,new Text(String.format("%s rank %d",currTime,rank)));
        //Even substring would have differente results when part of the time is used.
        if ((rank%5)==0) context.write(new Text(key.toString().substring(0,10)),new Text(String.format("%s rank %d",currTime,rank)));
        rank++;
      }
      //context.write(key,new Text(String.format("Final: %s",finalTime)));
    }
  }

  public static class DateHashPartitioner extends HashPartitioner<Text,Text> {
    @Override
    public int getPartition(Text key, Text value, int numReduceTasks) {
      return super.getPartition(new Text(key.toString().substring(0,10)),value,numReduceTasks);
      //return super.getPartition(key,value,numReduceTasks);
    }
  }

  public static class DateGroupingComparator extends WritableComparator {

    protected DateGroupingComparator() {
      super(Text.class,true);
    }

    public int compare(WritableComparable k1, WritableComparable k2) {
      Text date1 = (Text)k1;
      Text date2 = (Text)k2;
      return date1.toString().substring(0,10).compareTo(date2.toString().substring(0,10));
    }
  }

  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.printf("Usage %s [opt] <indir> <outdir>\n",getClass().getSimpleName());
      return -1;
    }
    Job job = Job.getInstance(getConf());
    job.setJarByClass(TimeRank.class);
    job.setJobName("Map Only 'SELECT Time, WindSpeedMPH WHERE WindSpeedMPH >= 20.0'");
    FileInputFormat.setInputPaths(job,new Path(args[0]));
    FileOutputFormat.setOutputPath(job,new Path(args[1]));

    job.setMapperClass(LocalMapper.class);
    // No reducer needed:
    job.setReducerClass(LocalReducer.class);

    job.setPartitionerClass(DateHashPartitioner.class);
    job.setGroupingComparatorClass(DateGroupingComparator.class);

    // These 4 lines seem to be useful for reduce only:
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;
  }
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new Configuration(), new TimeRank(), args);
    System.exit(exitCode);
  }
}

