package mapreduce.television;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//Write a Map Reduce program to calculate the total units sold for each Company.
public class TotalUnitsSoldForCompany {
	public static void main(String[] args) throws Exception {
		Configuration c = new Configuration();

		Job totalUnitsSoldJob = new Job(c, "TotalUnitsSoldForCompany");
		totalUnitsSoldJob.setJarByClass(TotalUnitsSoldForCompany.class);
		totalUnitsSoldJob.setMapperClass(MapperForTotalUnitsSoldForCompany.class);
		totalUnitsSoldJob.setReducerClass(ReducerForTotalUnitsSoldForCompany.class);
		totalUnitsSoldJob.setOutputKeyClass(Text.class);
		totalUnitsSoldJob.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(totalUnitsSoldJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(totalUnitsSoldJob, new Path(args[1]));

		System.exit(totalUnitsSoldJob.waitForCompletion(true) ? 0 : 1);
	}
}
