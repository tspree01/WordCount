package sample;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class JavaLetterCount
{
	public static void main(String[] args) throws Exception
	{
		SparkConf conf = new SparkConf().setAppName("LetterCount").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile(args[0]);
		JavaRDD<String> words = lines.flatMap(l -> Arrays.asList(l.toLowerCase().replaceAll("[_[0-9]]+", "").split("[^\\w]+")).iterator());
		JavaRDD<String> wordsNoBlanks = words.filter(l -> ! l.isEmpty());
		JavaRDD<String> letters = wordsNoBlanks.flatMap(l -> Arrays.asList(l.split("(?<=\\w)\\w")).iterator());
		JavaPairRDD<String, Integer> lettersPairs = letters.mapToPair(w -> new Tuple2<>(w, 1));
		JavaPairRDD<String, Integer> lettersCounts = lettersPairs.reduceByKey((n1, n2) -> n1 + n2);
		lettersCounts.saveAsTextFile("OutputLetterCount.txt");
		sc.stop();
	}
}
