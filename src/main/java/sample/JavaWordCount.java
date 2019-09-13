package sample;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class JavaWordCount
{
	public static void main(String[] args) throws Exception
	{
		SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile(args[0]);
		JavaRDD<String> words = lines.flatMap(l -> Arrays.asList(l.toLowerCase().split("[^\\w]+")).iterator());
		JavaPairRDD<Character, Integer> pairs = words.mapToPair(w -> new Tuple2<>(w.charAt(0), 1));
		JavaPairRDD<Character, Integer> counts = pairs.reduceByKey((n1, n2) -> n1 + n2);
		counts.saveAsTextFile("Output.txt");
		sc.stop();
	}
}
