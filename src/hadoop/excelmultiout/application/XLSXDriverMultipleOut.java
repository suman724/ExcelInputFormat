package hadoop.excelmultiout.application;

import org.apache.hadoop.conf.Configured;
import org.apache.log4j.Logger;

import hadoop.excelmultiout.mapper.ExcelMapperMultipleOut;
import hadoop.excelmultiout.reducer.ExcelReducerMultipleOut;
import hadoop.excelmultiout.xlsxformat.XLSXInputFormat;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class XLSXDriverMultipleOut extends Configured implements Tool {
	private static Logger LOG = Logger.getLogger(XLSXDriverMultipleOut.class);

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("Usage: [input] [output]");
			System.exit(-1);
		}

		Job job = Job.getInstance(getConf());
		job.setJobName("ExcelProcess");
		job.setJarByClass(XLSXDriverMultipleOut.class);

		// This is to set and match output<K,V> of the Mapper class
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(ExcelMapperMultipleOut.class);
		job.setCombinerClass(ExcelReducerMultipleOut.class);
		job.setReducerClass(ExcelReducerMultipleOut.class);

		job.setInputFormatClass(XLSXInputFormat.class);

		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		MultipleOutputs.addNamedOutput(job, "Testing", TextOutputFormat.class,
				NullWritable.class, Text.class);

		Path inputFilePath = new Path(args[0]);
		Path outputFilePath = new Path(args[1]);

		FileInputFormat.addInputPath(job, inputFilePath);
		FileOutputFormat.setOutputPath(job, outputFilePath);

		// Delete output filepath if already exists
		FileSystem fs = FileSystem.newInstance(getConf());
		if (fs.exists(outputFilePath)) {
			fs.delete(outputFilePath, true);
		}

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		LOG.info("XLSX Driver started");
		XLSXDriverMultipleOut XLSXDMO = new XLSXDriverMultipleOut();
		int res = ToolRunner.run(XLSXDMO, args);
		LOG.info("XLSX Driver ended");
		System.exit(res);
	}
}