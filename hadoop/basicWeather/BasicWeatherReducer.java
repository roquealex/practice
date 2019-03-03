import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class BasicWeatherReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
  //private IntWritable wordCountWritable = new IntWritable();
  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    int wordCount = 0;
    for( IntWritable count : values) {
      wordCount += count.get();
    }
    wordCountWritable.set(wordCount);
    context.write(key,wordCountWritable);
  }
}


