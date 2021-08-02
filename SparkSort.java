import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class SparkSort {
    public static void main(String[] args) {
        SparkConf spcon = new SparkConf().setAppName("Spark Sort");
        JavaSparkContext sCon = new JavaSparkContext(s);
        //get command line arguments for input and output
        String in = args[0];
        String out = args[1];
        //text file to an RDD
        JavaRDD<String> txtFile = sCon.txtFile(in);
        //returns a key-value RDD pairs
        PairFunction<String, String, String> keyValuepair =
               new PairFunction<String, String, String>() {
                    public Tuple2<String, String > call(String i) throws Exception{
                        return new Tuple2(i.substring(0,10), i.substring(11,98));
                    }
                };

        //Generate pair RDD according to KeyValuePairs Tuple and run sort
        JavaPairRDD<String, String> RDDpair = txtFile.mapToPair(keyValuepair).sortByKey(true);
        RDDpair.map(i -> i._1 + " " + i._2 + "\r").coalesce(1).saveAsTextFile(out);
        
    }

}

