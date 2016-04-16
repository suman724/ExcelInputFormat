package hadoop.excelmultiout.reducer;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;

public class ExcelReducerMultipleOut extends Reducer<Text, Text, NullWritable, Text> {
	private static Logger LOG = Logger.getLogger(ExcelReducerMultipleOut.class);
	private MultipleOutputs<NullWritable, Text> multipleOutputs;
	private Text Record = new Text();
	StringBuilder sb = new StringBuilder();
	@Override
	public void reduce(final Text key, final Iterable<Text> values,
			final Context context) throws IOException, InterruptedException {
		LOG.info("Reducer Started");
		Iterator<Text> iterator = values.iterator();

		while (iterator.hasNext()) {
			sb.append(iterator.next());
			if (iterator.hasNext()) {
				sb.append("\n");
			}
		}
		Record.set(sb.toString());
		multipleOutputs.write(NullWritable.get(), Record, "TXT_" + key.toString());
		LOG.info("Reducer Completed");
	}

	@Override
	public void setup(Context context) {
		multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
	}

	@Override
	public void cleanup(final Context context) throws IOException,
			InterruptedException {
		multipleOutputs.close();
	}
}