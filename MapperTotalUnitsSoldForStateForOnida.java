package mapreduce.television;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// Mapper class to calculate the total units sold in each state for Onida company. 
public class MapperTotalUnitsSoldForStateForOnida extends Mapper<LongWritable, Text, Text, IntWritable> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// Company Name|Product Name|Size in inches|State|Pin Code|Price
		// Onida|Lucid|18|Uttar Pradesh|232401|16200
		String line = value.toString();
		String NA = "NA";
		String ONIDA="Onida";
		String[] words = line.split("\\|");
		String companyName = words[0];

		String productName = words[1];
		if (!(companyName.equals(NA) || productName.equals(NA))) {
			if (companyName.equals(ONIDA)) {
				String stateName = words[3];
				Text outputKey = new Text(stateName.trim());
				IntWritable outputValue = new IntWritable(1);
				context.write(outputKey, outputValue);
			}

		}

	}
}
