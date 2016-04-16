package hadoop.excelmultiout.xlsformat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.*;
import java.io.InputStream;
import org.apache.log4j.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class XLSRecordReaderMultipleOut extends
		RecordReader<LongWritable, Text> {
	private static Logger LOG = Logger
			.getLogger(XLSRecordReaderMultipleOut.class);
	private LongWritable key;
	private Text value;
	private InputStream is;
	private String[] strArrayofLines;

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		List<String> strListofFiles = new ArrayList<String>();
		String filename = null;
		try {
			Matcher MF = Pattern.compile("(?!(.*\\/))(.*?)(?=(.xls).*)")
					.matcher(genericSplit.toString());
			while (MF.find()) {
				strListofFiles.add(MF.group());
			}
			filename = strListofFiles.get(0);
		} catch (Exception ex) {
			LOG.error("Error in extracting file name : " + ex.getMessage());
		}
		FileSplit split = (FileSplit) genericSplit;
		Configuration job = context.getConfiguration();
		final Path file = split.getPath();

		FileSystem fs = file.getFileSystem(job);
		FSDataInputStream fileIn = fs.open(split.getPath());

		is = fileIn;
		String line = new XLSParser().parseXLSdata(is, filename);
		this.strArrayofLines = line.split("\n");
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {

		if (key == null) {
			key = new LongWritable(0);
			value = new Text(strArrayofLines[0]);

		} else {

			if (key.get() < (this.strArrayofLines.length - 1)) {
				long pos = (int) key.get();

				key.set(pos + 1);
				value.set(this.strArrayofLines[(int) (pos + 1)]);

				pos++;
			} else {
				return false;
			}

		}

		if (key == null || value == null) {
			return false;
		} else {
			return true;
		}

	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {

		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {

		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {

		return 0;

	}

	@Override
	public void close() throws IOException {
		if (is != null) {
			is.close();
		}

	}

}