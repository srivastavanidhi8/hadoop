
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class MaxDriver {
	public static void main(String args[])throws Exception{
		if(args.length!=2){
			System.out.println("input valid arg");
			System.exit(-1);
		}
		JobConf conf=new JobConf(MaxDriver.class);
		conf.setMapperClass(MaxMapper.class);
		conf.setReducerClass(MaxReducer.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
		
	}

}
