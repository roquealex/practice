import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.TimeZone;
import java.util.Date;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class WindUtils {

  public static final int COL_TIME = 0;
  public static final int COL_TEMPERATUREF = 1;
  public static final int COL_DEWPOINTF = 2;
  public static final int COL_PRESSUREIN = 3;
  public static final int COL_WINDDIRECTION = 4;
  public static final int COL_WINDDIRECTIONDEGREES = 5;
  public static final int COL_WINDSPEEDMPH = 6;
  public static final int COL_WINDSPEEDGUSTMPH = 7;

  public static final String HEADER_FORMAT =
    "Time,TemperatureF,DewpointF,PressureIn,WindDirection,"+
    "WindDirectionDegrees,WindSpeedMPH,WindSpeedGustMPH";
  public static final int HEADER_FORMAT_COLS = HEADER_FORMAT.split(",").length;

  //    "2012-12-01 00:00:00,75.3,71.5,30.08,NNE,18,10.0,11.0,88,0.00,,,0.00,0,Wunderground v.1.15,2012-12-01 06:00:00",
  public static GenTupleWritable getReading(String line) {
    String[] columns = line.split(",");
    // TODO: check that array is greater than the header
    Writable[] writableArr = new Writable[HEADER_FORMAT_COLS];
    for (int i = 0 ; i < HEADER_FORMAT_COLS ; i++) {
      if (i == COL_TIME || i == COL_WINDDIRECTION) {
        writableArr[i] = new Text(columns[i].trim());
      } else {
        try {
          writableArr[i] = new DoubleWritable(Double.parseDouble(columns[i]));
        } catch(NumberFormatException e) {
          writableArr[i] = new DoubleWritable(Double.NaN);
        }
      }
    }
    return new GenTupleWritable(writableArr);
  }

  /*
  windNoBadReadingsDF = windFormattedDF \
    .withColumn(
        "WindDirectionDegrees",
        when(col("WindDirectionDegrees")==-737280,0).otherwise(col("WindDirectionDegrees"))) \
    .filter(col("WindSpeedMPH")>=0.0) \
    .filter(col("WindDirectionDegrees").between(0,360))
    */

  public static GenTupleWritable cleanupReading(GenTupleWritable reading) {
    if (((DoubleWritable)reading.get(COL_WINDDIRECTIONDEGREES)).get()==-737280.0) {
      Writable[] writableArr = new Writable[HEADER_FORMAT_COLS];
      for (int i = 0 ; i < HEADER_FORMAT_COLS ; i++) {
        writableArr[i] = (i == COL_WINDDIRECTIONDEGREES)? new DoubleWritable(0.0):reading.get(i);
      }
      return new GenTupleWritable(writableArr);
    } else {
      return reading;
    }
  }

  public static boolean isValidReading(GenTupleWritable reading) {
     return (
         ((DoubleWritable)reading.get(COL_WINDSPEEDMPH)).get() >= 0 &&
         ((DoubleWritable)reading.get(COL_WINDDIRECTIONDEGREES)).get() >= 0 &&
         ((DoubleWritable)reading.get(COL_WINDDIRECTIONDEGREES)).get() <= 360
         );
  }


  /*
def slopeExpr(x1,y1,x2,y2) :
    return (
        (y2-y1) 
        /(x2-x1)
    )

def interceptExpr(x1,y1,x2,y2) :
    return (
      (x1*y2-x2*y1)
        /(x1-x2)
    )
 */

  public static double slopeExpr(long x1, double y1, long x2, double y2) {
    return (
        (y2-y1) 
        /(x2-x1)
    );
  }

  public static double interceptExpr(long x1, double y1, long x2, double y2) {
    return (
      (x1*y2-x2*y1)
        /(x1-x2)
        );
  }

  public static double positiveAngle(double angle) {
    return ( (angle % 360.0) + 360.0 ) % 360.0;
  }

  public static double linearExpr(long x, double m , double b){
      return m*x + b;
  }

  public static Stream<GenTupleWritable> interpolateWindSpeedAndDir(GenTupleWritable start, GenTupleWritable end) throws ParseException {
    final long FIVE_MINUTES_IN_MILLIS = 5*60*1000;//millisecs
    
    TimeZone tz = TimeZone.getTimeZone("America/Merida");
    SimpleDateFormat tsFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    tsFormat.setTimeZone(tz);

    Date startTS = tsFormat.parse(start.get(COL_TIME).toString());
    Date endTS = tsFormat.parse(end.get(COL_TIME).toString());

    long startEpoch = startTS.getTime();
    long endEpoch = endTS.getTime();

    double startAngle = ((DoubleWritable)start.get(COL_WINDDIRECTIONDEGREES)).get();
    double endAngle = ((DoubleWritable)end.get(COL_WINDDIRECTIONDEGREES)).get();
    double shortestAngle = ( ( ( ( (endAngle-startAngle) % (360.0) ) + (540.0) ) % (360.0) ) - (180.0) );
    double nextAngle = startAngle + shortestAngle;

    double startWindSpeedMPH = ((DoubleWritable)start.get(COL_WINDSPEEDMPH)).get();
    double endWindSpeedMPH = ((DoubleWritable)end.get(COL_WINDSPEEDMPH)).get();

    double mWindDirectionDegrees = slopeExpr(startEpoch, startAngle, endEpoch, nextAngle);
    double bWindDirectionDegrees = interceptExpr(startEpoch, startAngle, endEpoch, nextAngle);
    double mWindSpeedMPH = slopeExpr(startEpoch, startWindSpeedMPH, endEpoch, endWindSpeedMPH);
    double bWindSpeedMPH = interceptExpr(startEpoch, startWindSpeedMPH, endEpoch, endWindSpeedMPH);

    long step = FIVE_MINUTES_IN_MILLIS;
    //LongStream.range((startEpoch+step-1)/step, endEpoch/step).map(x -> x*step).forEach(x -> System.out.println(x));

    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
    dateFormat.setTimeZone(TimeZone.getTimeZone("America/Merida"));

    Stream<GenTupleWritable> tuples = LongStream.range((startEpoch+step-1)/step, endEpoch/step)
      .map(x -> x*step)
      .mapToObj(t -> {
        String tsStr = dateFormat.format(new Date(t));
        if (t == startEpoch) {
          Writable[] arr = {
            //start.get(COL_TIME),
            //new Text(tsFormat.format(new Date(t))),
            new Text(tsStr.substring(0,10)),
            new Text(tsStr),
            new LongWritable(t/1000), // to match golden
            new DoubleWritable(positiveAngle(startAngle)),
            start.get(COL_WINDSPEEDMPH)
          };
          return new GenTupleWritable(arr);
        } else {
          Writable[] arr = {
            //new Text(tsFormat.format(new Date(t))),
            new Text(tsStr.substring(0,10)),
            new Text(tsStr),
            new LongWritable(t/1000), // to match golden
            new DoubleWritable(positiveAngle(linearExpr(t,mWindDirectionDegrees,bWindDirectionDegrees))),
            new DoubleWritable(linearExpr(t,mWindSpeedMPH,bWindSpeedMPH)),
          };
          return new GenTupleWritable(arr);
        }

      });
    return tuples;

  }

  private static boolean compareSubHeader(String header, String subHeader) {
    final int subHeaderLength = subHeader.length();
    if (subHeaderLength > header.length()) return false;
    String partHeader = header.substring(0,subHeaderLength);
    //System.out.println("Comparing header:"+partHeader);
    return partHeader.equals(subHeader);
    // potential:
    //public boolean equalsIgnoreCase(String anotherString);
  }

  public static boolean isValidHeader(String header) {
    return (compareSubHeader(header, HEADER_FORMAT));
  }

  public static boolean looksLikeHeader(String line) {
    return (compareSubHeader(line, "Time"));
  }

  /*
  public static void main(String s[]) {
    System.out.println("This is a test");
    String lines[] = {
      "Time,TemperatureF,DewpointF,PressureIn,WindDirection,WindDirectionDegrees,WindSpeedMPH,WindSpeedGustMPH,Humidity,HourlyPrecipIn,Conditions,Clouds,dailyrainin,SolarRadiationWatts/m^2,SoftwareType,DateUTC",
      "2012-12-01 00:00:00,75.3,71.5,30.08,NNE,18,10.0,11.0,88,0.00,,,0.00,0,Wunderground v.1.15,2012-12-01 06:00:00",
      "2012-12-01 00:05:00,75.3,71.5,30.08,North,9,10.0,11.0,88,0.00,,,0.00,0,Wunderground v.1.15,2012-12-01 06:05:00",
      " 2012-12-01 00:05:00 ,75.3,71.5,30.08,North,9,10.0,11.0,88,0.00,,,0.00,0,Wunderground v.1.15,2012-12-01 06:05:00",
      " 2012-12-01 00:05:00 , 75.3 ,,+30.08,North,9,10.0,11.0,88,0.00,,,0.00,0,Wunderground v.1.15,2012-12-01 06:05:00",
      "2012-06-21 23:00:00,77.8,74.6,29.77,N/A,-737280,0.0,-999.0,90,0.00,,,0.03,0,Wunderground v.1.15,2012-06-22 04:00:00",
      "2012-12-01 00:05:00,75.3,71.5,30.08,North,956,10.0,11.0,88,0.00,,,0.00,0,Wunderground v.1.15,2012-12-01 06:05:00",
      "2012-12-01 00:05:00,75.3,71.5,30.08,North,360,10.0,11.0,88,0.00,,,0.00,0,Wunderground v.1.15,2012-12-01 06:05:00",
      "2012-12-01 00:05:00,75.3,71.5,30.08,North,0,10.0,11.0,88,0.00,,,0.00,0,Wunderground v.1.15,2012-12-01 06:05:00",
      "2012-12-01 00:05:00,75.3,71.5,30.08,North,-0.1,10.0,11.0,88,0.00,,,0.00,0,Wunderground v.1.15,2012-12-01 06:05:00",
      "2012-12-01 00:05:00,75.3,71.5,30.08,North,0,0.0,11.0,88,0.00,,,0.00,0,Wunderground v.1.15,2012-12-01 06:05:00",
      "2012-12-01 00:05:00,75.3,71.5,30.08,North,0,-1.0,11.0,88,0.00,,,0.00,0,Wunderground v.1.15,2012-12-01 06:05:00",
      "Time"
    };

    for (String line : lines) {
      //boolean looks = looksLikeHeader(line);
      //boolean valid = isValidHeader(line);
      //System.out.printf("Testing %s, %b, %b\n",line,looks,valid);
      if (looksLikeHeader(line)) {
        System.out.printf("line %s is valid : %b\n",line,isValidHeader(line));
      } else {
        //System.out.printf("line %s is not header\n",line);
        GenTupleWritable row = getReading(line);
        row = cleanupReading(row);
        System.out.println(row);
        System.out.println(isValidReading(row));

      }
    }

    String start = "2012-12-24 04:10:00,58.4,53.9,30.01,ESE,118,4.0,6.0,85,0.00,,,0.00,0,Wunderground v.1.15,2012-12-24 10:10:00";
    String end =   "2012-12-24 04:20:00,58.1,53.9,30.02,ESE,122,5.0,5.0,86,0.00,,,0.00,0,Wunderground v.1.15,2012-12-24 10:20:00";

    GenTupleWritable readingStart = getReading(start);
    GenTupleWritable readingEnd = getReading(end);

    try {
      Stream<GenTupleWritable> tuples = interpolateWindSpeedAndDir(readingStart, readingEnd);

      for (java.util.Iterator<GenTupleWritable> it = tuples.iterator() ; it.hasNext() ; ) {
        System.out.println(it.next());
      }
    } catch (Exception e) {
    }
  }
    */
}
