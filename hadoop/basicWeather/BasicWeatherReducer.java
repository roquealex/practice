import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class BasicWeatherReducer extends Reducer<Text,DoubleWritable,Text,TupleWritable> {
  //private IntWritable wordCountWritable = new IntWritable();
  
  //private DoubleWritable maxTempWritable = new IntWritable();
  //private TupleWritable maxTempWritable = new IntWritable();

  @Override
  public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
    double maxTemp = -9999.0;
    double minTemp = 9999.0;
    for( DoubleWritable temp : values) {
      //wordCount += count.get();
      double currTemp = temp.get();
      if (currTemp > maxTemp) {
        maxTemp = currTemp;
      }
      if (currTemp < minTemp) {
        minTemp = currTemp;
      }
    }
    //wordCountWritable.set(wordCount);
    //context.write(key,new DoubleWritable(maxTemp));
    //context.write(key,new DoubleWritable(minTemp));
    Writable[] maxMin = new Writable[2];
    maxMin[0] = new DoubleWritable(maxTemp);
    maxMin[1] = new DoubleWritable(minTemp);
    TupleWritable tuple = new GenTupleWritable(maxMin);
    context.write(key,tuple);
  }
}


