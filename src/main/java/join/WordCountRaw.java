/**
 * Author: Thanh-Ngoan TRIEU
 * Address: Can Tho University
 * Email: ttngoan@cit.ctu.edu.vn
 *
 */
package join;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
public class WordCountRaw {
	private static final FlatMapFunction<String, String> WORDS_EXTRACTOR = new FlatMapFunction<String, String>() {
		private static final long serialVersionUID = 1L;
		public Iterator<String> call(String s) throws Exception {
			List<String> list =  Arrays.asList(s.split(" "));
			Iterator iterator = list.iterator(); 
			return iterator;
		}
	};

	private static final PairFunction<String, String, Integer> WORDS_MAPPER = new PairFunction<String, String, Integer>() {
		private static final long serialVersionUID = 1L;
		public Tuple2<String, Integer> call(String s) throws Exception {
			return new Tuple2<String, Integer>(s, 1);
		}
	};

	private static final Function2<Integer, Integer, Integer> WORDS_REDUCER = new Function2<Integer, Integer, Integer>() {
		private static final long serialVersionUID = 1L;
		public Integer call(Integer a, Integer b) throws Exception {
			return a + b;
		}
	};

	public static void main(String[] args) {

		SparkConf conf1 = new SparkConf().setAppName("WordCount");//.setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf1);
		
		JavaRDD<String> file = sc.textFile(args[0]);
		//JavaRDD<String> file = sc.textFile("/Users/ntrieuth/Documents/input/x000.txt");
		JavaRDD<String> words0 = file.flatMap(WORDS_EXTRACTOR);
		JavaPairRDD<String, Integer> pairs0 = words0.mapToPair(WORDS_MAPPER);
		JavaPairRDD<String, Integer> counter = pairs0.reduceByKey(WORDS_REDUCER);
		
		counter.saveAsTextFile(args[1]);
		//counter.saveAsTextFile("/Users/ntrieuth/Documents/input/x000out");
		//System.out.println(counter.collect());
	}
}