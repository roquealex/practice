import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
//import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;


import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

// Mapper:
import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;

// Reducer:
import org.apache.hadoop.mapreduce.Reducer;

import java.util.HashMap;
import java.util.Iterator;
import java.util.stream.Stream;
import java.text.ParseException;

/*
 * History:
javac -classpath `$HADOOP_HOME/bin/hadoop classpath` *.java
jar cvf timeRank.jar *.class
$HADOOP_HOME/bin/hadoop jar timeRank.jar WindDemoComb datasets/random/ timeRank7
hadoop fs -cat timeRank7/* > out.txt
*/
public class WindDemoComb extends Configured implements Tool {

  public static class LocalMapper extends Mapper<LongWritable, Text, Text, GenTupleWritable> {
    private Text wordObject = new Text();

    public static final int COL_TIME = 0;
    public static final int COL_TEMPERATUREF = 1;
    //public static final int COL_WINDSPEEDMPH = 6;

    /*
    private HashMap<String,Integer> readingsCount;

    @Override
    public void setup(Context context) {
      readingsCount = new HashMap<>();
    }
    */

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();

      if (WindUtils.looksLikeHeader(line)) {
        //System.out.printf("line %s is valid : %b\n",line,isValidHeader(line));
      } else {
        GenTupleWritable row = WindUtils.getReading(line);
        row = WindUtils.cleanupReading(row);
        if (WindUtils.isValidReading(row)) {
          //String time = line.substring(0,19);
          //wordObject.set(time);
          Text time = (Text)row.get(WindUtils.COL_TIME);
          String date = time.toString().substring(0,10);
          Writable[] val = {new IntWritable(1)};
          context.write(new Text(date), new GenTupleWritable(val));
          /*
          if (readingsCount.containsKey(date)) {
            readingsCount.put(date,readingsCount.get(date)+1);
          } else {
            readingsCount.put(date,1);
          }
          */
          context.write(time, row);
          //context.write(wordObject, row);
        }
      }
    }

    /*
    @Override
    public void cleanup(Context context) throws IOException,InterruptedException {
      //readingsCount = new HashMap<>();
      for (HashMap.Entry<String,Integer> entry : readingsCount.entrySet()) {
        Text key = new Text(entry.getKey());
        Writable[] val = {new IntWritable(entry.getValue())};
        context.write(key, new GenTupleWritable(val));
      }
      readingsCount.clear();
    }
    */

  }

  // Combiner doesn't seem to follow the grouping of my partitioner
  // so 2012-12-21 and 2012-12-21 12:15:00 will be evaluated independently
  public static class LocalCombiner extends Reducer<Text,GenTupleWritable,Text,GenTupleWritable> {
    //public  int rank = 0;
    @Override
    public void reduce(Text key, Iterable<GenTupleWritable> values, Context context) throws IOException, InterruptedException {
      //int rank = 0;
      int readingCount = 0;
      boolean readingFound = false;
      boolean countFound = false;
      //GenTupleWritable prevRow = null;
      Text countKey = null;
      reduceloop:
      //for( GenTupleWritable row : values) {
      for( Iterator<GenTupleWritable> it = values.iterator(); it.hasNext() ; ) {
        GenTupleWritable row = it.next();
        if (row.size()==1) {
          countFound = true;
          // This is a count
          int thisCount = ((IntWritable)row.get(0)).get();
          readingCount += thisCount;
          if (countKey == null) {
            //prevRow = WritableUtils.clone(row,context.getConfiguration());
            countKey = key;
          }
          // doesn't work has to be enabled from command:
          assert(!readingFound);
          //Writable arr[] = new Writable[]{new IntWritable(readingCount)};
          //context.write(countKey,new GenTupleWritable(arr));
        } else {
          readingFound = true;
          context.write(key,row);
        }
      }
      assert(!readingFound&&countFound || readingFound&&!countFound );
      // The counter of values should be evaluated in a different combiner not together with readings
      if (countFound) {
        Writable arr[] = new Writable[]{new IntWritable(readingCount)};
        //context.write(countKey==null?new Text("2009-01-00"):countKey,new GenTupleWritable(arr));
        context.write(countKey,new GenTupleWritable(arr));
      }
    }
  }



  public static class LocalReducer extends Reducer<Text,GenTupleWritable,/*Text*/ NullWritable,Text> {
    //public  int rank = 0;
    @Override
    public void reduce(Text key, Iterable<GenTupleWritable> values, Context context) throws IOException, InterruptedException {
      //int rank = 0;
      int readingCount = 0;
      boolean readingFound = false;
      GenTupleWritable prevRow = null;
      reduceloop:
      //for( GenTupleWritable row : values) {
      for( Iterator<GenTupleWritable> it = values.iterator(); it.hasNext() ; ) {
        GenTupleWritable row = it.next();
        if (row.size()==1) {
          // This is a count
          int thisCount = ((IntWritable)row.get(0)).get();
          readingCount += thisCount;
          // doesn't work has to be enabled from command:
          assert(!readingFound);
          //Writable arr[] = new Writable[]{key,new IntWritable(thisCount)};
          //GenTupleWritable tuple = new GenTupleWritable(arr);
          //context.write(NullWritable.get(),new Text(tuple.toString()));
        } else {
          readingFound = true;
          if (readingCount <100 ) break reduceloop;
          if (prevRow == null) {
            //prevRow = row;
          } else {
            //String time = ((Text)row.get(WindUtils.COL_TIME)).toString();
            //String prevTime = ((Text)prevRow.get(WindUtils.COL_TIME)).toString();
            //context.write(key,new Text(String.format("%s to %s rank %d", prevTime, time, rank)));
            try {
              Stream<GenTupleWritable> tuples = WindUtils.interpolateWindSpeedAndDir(prevRow, row);
              tuples.forEach(tuple -> {
                try {
                  //context.write(key,new Text(String.format("%s rank %d", tuple, rank)));
                  //context.write(key,new Text(tuple.toString()));
                  context.write(NullWritable.get(),new Text(tuple.toString()));
                } catch (IOException e) {
                } catch (InterruptedException e) {
                }
              } );


            } catch (ParseException e) {
            }

            //rank++;
          }
          prevRow = WritableUtils.clone(row,context.getConfiguration());
        }
        /*
        String currRow = row.toString();
        //context.write(new Text(key.toString().substring(0,10)),new Text(String.format("%s rank %d",currRow,rank)));
        context.write(key,new Text(String.format("%s rank %d",currRow,rank)));
        rank++;
        */
      }
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
    //System.out.println("Changing conf 1m");
    //conf.set("mapreduce.input.fileinputformat.split.maxsize","268435456");
    //getConf().set("mapreduce.input.fileinputformat.split.maxsize","268435456");
    // This one has an impact, find the units:
    //getConf().set("mapreduce.input.fileinputformat.split.maxsize","1048576");

    Job job = Job.getInstance(getConf());

    // Multiple files per map:
    job.setInputFormatClass(CombineTextInputFormat.class);
    
    job.setJarByClass(WindDemoComb.class);
    job.setJobName("Resample data into equally spaced");
    FileInputFormat.setInputPaths(job,new Path(args[0]));
    FileOutputFormat.setOutputPath(job,new Path(args[1]));

    job.setMapperClass(LocalMapper.class);
    job.setCombinerClass(LocalCombiner.class);
    job.setReducerClass(LocalReducer.class);

    job.setPartitionerClass(DateHashPartitioner.class);
    job.setGroupingComparatorClass(DateGroupingComparator.class);

    // These 4 lines seem to be useful for reduce only:
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(GenTupleWritable.class);

    //job.setOutputKeyClass(Text.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);

    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new Configuration(), new WindDemoComb(), args);
    System.exit(exitCode);
  }

}

