import java.util.List;
import java.util.ArrayList;
import org.apache.hadoop.hive.ql.exec.UDF;

public class SequenceUDF extends UDF {

	// This is not an override
	//@Override
	public List<Long> evaluate(long start, long stop, long step) {
		List<Long> res = new ArrayList<>();
		// stop inclusive to match spark behavior
		for (long i = start; i <= stop; i+=step) {
			res.add(i);
		}
		return res;
	}

/*
	public static void main(String s[]) {
		List<Long> l = new SequenceUDF().evaluate(2,8,1);
		System.out.println(l);
	}
*/


}

