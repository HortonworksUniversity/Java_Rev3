package spark;

import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class WordCount {

  @SuppressWarnings("serial")
  public static void main(String... args) {
    JavaSparkContext sc = new JavaSparkContext("yarn-standalone", "word-count");
    JavaRDD<String> file = sc.textFile("hdfs://namenode:8020/user/root/constitution.txt");
    JavaRDD<String> words = file.flatMap(new FlatMapFunction<String, String>() {
      public Iterable<String> call(String s) {
        return Arrays.asList(s.split(" "));
      }
    });
    JavaPairRDD<String, Integer> pairs = words.map(new PairFunction<String, String, Integer>() {
      public Tuple2<String, Integer> call(String s) {
        return new Tuple2<String, Integer>(s, 1);
      }
    });
    JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
      public Integer call(Integer a, Integer b) {
        return a + b;
      }
    });
    counts.saveAsTextFile("hdfs://namenode:8020/user/root/sparkwordcount");
  }
}