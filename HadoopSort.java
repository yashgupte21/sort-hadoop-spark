import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HadoopSort {

  private static final Logger logger = Logger.getLogger(HadoopSort.class);
  
  private static final int totChar = 128;


  //Mapper that maps first 10 characters to key and rest to value in a line
  public static class Mapping
       extends Mapper<Object, Text, Text, Text>{

    public void map(Object keyM, Text valueM, Context context
                    ) throws IOException, InterruptedException {
      String lineM  = valueM.toString();
      String keymap = lineM.substring(0,10);
      String valuemap = lineM.substring(11,98);
      valuemap += "\r";
      context.write(new Text(keymap),new Text(valuemap));
    }
  }


  //writting to files
  public static class SReducer
       extends Reducer<Text,Text,Text,Text> {
    public void reduce(Text keyS, Iterable<Text> valueS,
                       Context context
                       ) throws IOException, InterruptedException {
      for (Text valS : valueS) {
        context.write(keyS,valS);
      }
    }
  }

  //cPartitioner that can be used to get sorted output among the partitions
  //output to reducers based on first character of the string.
  public static class cPartitioner extends Partitioner<Text,Text>{
                public int getPartition(Text key, Text value, int numRTasks){
      int numCPerReducer = totChar/numRTasks;
      int startChar = (int)key.toString().charAt(0);
      int ite = 0;
      while(ite < numRTasks){
        int start = ite * numCPerReducer;
        int end = (ite+1) * numCPerReducer;
        if(startChar >= start && startChar < end){
          return ite;
        }
        ite++;
      }
      return ite-1;
    }
  }

  public static void main(String[] args) throws Exception {
    logger.info("Starting timer");
    long started = System.currentTimeMillis();
    Configuration config = new Configuration();
    Job jobs = Job.getInstance(config, "hadoop sort");
    jobs.setJarByClass(HadoopSort.class);
    jobs.setMapperClass(Mapping.class);
    jobs.setCombinerClass(SReducer.class);
    jobs.setPartitionerClass(cPartitioner.class);
    //Setting the number of reducers 
    jobs.setReducerClass(SReducer.class);
    jobs.setNumReduceTasks(1);
    jobs.setOutputKeyClass(Text.class);
    jobs.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(jobs, new Path(args[0]));
    FileOutputFormat.setOutputPath(jobs, new Path(args[1]));

    if(jobs.waitForCompletion(true) == true){
      long ended = System.currentTimeMillis();
            long elapsedtime = ended - started;
            logger.info("Time elapsed: \n");
            logger.info(elapsedtime);
            logger.warn("===============================================================================");
        System.exit(0);
    }
    else{
        System.exit(1);
    }
  }
}
