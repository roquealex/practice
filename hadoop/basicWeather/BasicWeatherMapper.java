import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class BasicWeatherMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
  //private final static IntWritable one = new IntWritable(1);
  private Text wordObject = new Text();

  public static final int COL_TIME = 0;
  public static final int COL_TEMPERATUREF = 1;

  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String line = value.toString();
    String[] columns = line.split(",");
    if (!columns[COL_TIME].equals( "Time")) {
      String date = columns[COL_TIME].substring(0,10);
      double temp = Double.parseDouble(columns[COL_TEMPERATUREF]);
      wordObject.set(date);
      context.write(wordObject, new DoubleWritable(temp));
    }
    /*
    for (String word : line.split("\\W+")) {
      if (word.length() > 0 ) {
        wordObject.set(word);
        context.write(wordObject, one);
      }
    }
    */
  }
}


