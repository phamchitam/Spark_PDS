package join;

import java.util.ArrayList;
import java.util.Iterator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class Join2Raw_Cluster {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Join2RawCluster").setMaster("yarn");
		JavaSparkContext sc = new JavaSparkContext(conf);

		String inputL = "/home/ftpuser/ftp/L_2GB"; 
		String inputR = "/home/ftpuser/ftp/R_1GB"; 
		String output = "/home/ftpuser/ftp/output";
		JavaRDD<String> R = sc.textFile(inputR);
		JavaRDD<String> L = sc.textFile(inputL);

		JavaPairRDD<String, String> Rpairs = R.mapToPair(new PairFunction<String, String, String>() {
			@SuppressWarnings("unchecked")
			@Override
			public Tuple2<String, String> call(String arg0) throws Exception {
				int first = arg0.indexOf(",");
				int second = arg0.indexOf(",", first + 1);
				String key = arg0.split(",")[1];
				String value = arg0.split(",")[0] + "," + arg0.substring(second + 1);
				return new Tuple2(key, value);
			}
		});
		JavaPairRDD<String, String> Lpairs = L.mapToPair(new PairFunction<String, String, String>() {
			@SuppressWarnings("unchecked")
			public Tuple2<String, String> call(String x) {
				int first = x.indexOf(",");
				String value = x.substring(first + 1);
				return new Tuple2(x.split(",")[0], value);
			}
		});

		//Rpairs.join(Lpairs).saveAsTextFile(output1);

		JavaPairRDD<String, String> result = Rpairs.cogroup(Lpairs).flatMapToPair(
				new PairFlatMapFunction<Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>>, String, String>() {
					@Override
					public Iterator<Tuple2<String, String>> call(
							Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>> arg0) throws Exception {
						ArrayList<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();
						Iterator<String> iter1 = arg0._2._1.iterator();
						Iterator<String> iter2 = arg0._2._2.iterator();
						String str1 = "", str2 = "";
						int first, first1;
						while (iter1.hasNext()) {
							str1 = iter1.next();
							first = str1.indexOf(",");
//							iter2 = arg0._2._2.iterator();
							while (iter2.hasNext()) {
								str2 = iter2.next();
								first1 = str2.indexOf(",");
								list.add(new Tuple2(str1.split(",")[0] + "," + str2.split(",")[0],
										str1.substring(first + 1) + "," + str2.substring(first1 + 1)));
							}
						}
						return list.iterator();
					}
				});
		result.saveAsTextFile(output);

		sc.stop();
	}
}
