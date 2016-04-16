package hadoop.excelmultiout.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
//import org.apache.hadoop.mapreduce.lib.input.FileSplit;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import java.util.Iterator;
//import java.util.ArrayList;
//import java.util.List;
import java.util.regex.*;

public class ExcelMapperMultipleOut extends
		Mapper<LongWritable, Text, Text, Text> {
	private static Logger LOG = Logger.getLogger(ExcelMapperMultipleOut.class);
	String filenameMap = new String();
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		LOG.info("Mapper Started");
		// List<String> strListofKeys = new ArrayList<String>();
		// List<String> strListofValues = new ArrayList<String>();
		String strKEY = null;
		String strVALUE = null;
		String strVALUEmodified = null;
		try {
			// To deal with Junk Characters...
			String line = value.toString().replaceAll("[^\\x00-\\x7F]", "");
			Matcher m = Pattern.compile("(%#@)(.*?)(&@!)").matcher(line);
			while (m.find()) {
				// strListofKeys.add(m.group());
				strKEY = m.group().replace("%#@", "").replace("&@!", "");
			}
			Matcher mn = Pattern.compile("(&@!)(.*?)(%#@)").matcher(line);
			while (mn.find()) {
				// strListofValues.add(mn.group());
				strVALUE = mn.group().replace("&@!", "").replace("%#@", "");
			}
			if (strVALUE.length() > 0) {
				strVALUEmodified = strVALUE.substring(0,
						(strVALUE.length() - 1));
			} else {
				strVALUEmodified = "";
			}
		} catch (Exception ex) {
			LOG.error("Error in Map <K,V> conversion : " + ex.getMessage(), ex);
		}
		// context.write(new Text(strListofKeys.get(0)), new
		// Text(strListofValues.get(0)));
		context.write(new Text(strKEY), new Text(strVALUEmodified));
		LOG.info("Mapper Completed");
	}

	public void run(Context context) throws IOException, InterruptedException {
		setup(context);
		while (context.nextKeyValue()) {
			map(context.getCurrentKey(), context.getCurrentValue(), context);
		}
		cleanup(context);
	}

	/*
	 * @Override protected void setup(Context context) throws IOException,
	 * InterruptedException { FileSplit fsFileSplit = (FileSplit)
	 * context.getInputSplit(); filenameMap =
	 * context.getConfiguration().get(fsFileSplit
	 * .getPath().getParent().getName()); }
	 */

}
