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
		JavaRDD<String> linesNoBlankLines = lines.filter(l -> ! l.isEmpty());
		JavaRDD<String> words = linesNoBlankLines.flatMap(l -> Arrays.asList(l.toLowerCase().replaceAll("[<>,.*:#!?_]", "").trim().split("[^\\w]+")).iterator());
		JavaRDD<String> letters = words.flatMap(l -> Arrays.asList(l.split("(?<=\\w)\\w+(\\s+)?")).iterator());
		//JavaRDD<String> noNumOrPunctuation = letters.flatMap(l -> Arrays.asList(l.split("[0-9,\\W]")).iterator());
		JavaPairRDD<String, Integer> pairs = words.mapToPair(w -> new Tuple2<>(w, 1));
		JavaPairRDD<String, Integer> counts = pairs.reduceByKey((n1, n2) -> n1 + n2);
		JavaPairRDD<String, Integer> lettersPairs = letters.mapToPair(w -> new Tuple2<>(w, 1));
		JavaPairRDD<String, Integer> lettersCounts = lettersPairs.reduceByKey((n1, n2) -> n1 + n2);
		//JavaPairRDD<String, Integer> noNumOrPunctuationPairs = noNumOrPunctuation.mapToPair(w -> new Tuple2<>(w, 1));
		//JavaPairRDD<String, Integer> noNumOrPunctuationCounts = noNumOrPunctuationPairs.reduceByKey((n1, n2) -> n1 + n2);
		//JavaPairRDD<String, Integer> letterPairs = letter.mapToPair(w -> new Tuple2<>(w, 1));
		//JavaPairRDD<String, Integer> letterCounts = letterPairs.reduceByKey((n1, n2) -> n1 + n2);
		words.saveAsTextFile("Output.txt");
		lettersCounts.saveAsTextFile("OutputLetters.txt");

		//noNumOrPunctuationCounts.saveAsTextFile("NoNumOrPunctuation.txt");
		//counts.saveAsTextFile("LetterOutput.txt");
		sc.stop();
	}
}
