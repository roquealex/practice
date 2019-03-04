import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.io.Writable;

/**
 * Hack of the org.apache.hadoop.mapred.join.TupleWritable to work as a generic tuple.
 *
 * Sets the written BitSet to true when the constructor is called.
 *
 */
public class GenTupleWritable extends TupleWritable {
  /**
   * Initialize tuple with storage; unknown whether any of them contain
   * &quot;written&quot; values.
   */
  public GenTupleWritable(Writable[] vals) {
    super(vals);
    written.set(0,vals.length);
  }

}
