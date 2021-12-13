package join;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import bloom.BloomFilter;
import bloom.Key;
import scala.Tuple2;

public class Join2RawBF {
	public static final int max = 4000;
	public static void main(String[] args) {
		@SuppressWarnings("rawtypes")
		BloomFilter bloomFilter; 
		@SuppressWarnings("rawtypes")
		BloomFilter BroadCast_BF_Value;
		
		int vectorSize = max;
		int hash = 8;
		int hashType = 1;
		bloomFilter = new BloomFilter (vectorSize, hash , hashType);
		
		SparkConf conf = new SparkConf().setAppName("Join2RawBF").setMaster("local[*]");  
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		String inputL = "hdfs://master:9000/data/Left";
		String inputR = "hdfs://master:9000/data/Right";
		String output = "hdfs://master:9000/data/result";

		JavaRDD<String> R = sc.textFile(inputR);
		JavaRDD<String> L = sc.textFile(inputL);
		
		JavaRDD<String> rddBF = R.map(new Function<String, String>() {
			@Override
			public String call(String arg0) throws Exception {
				String [] fields = arg0.split(",");
				if (fields != null && 0 < fields.length  
						&& fields[0].trim().length() > 0 ) {
					return fields[0];
				}
				return "";
			}
//		}).repartition(1).filter(t -> (t.trim().length() > 0));
		}).filter(t -> (t.trim().length() > 0));
		
		List<String> BF = rddBF.collect();
		for (String record : BF) {
			bloomFilter.add(new Key(record.getBytes()));
		}
		
		@SuppressWarnings("rawtypes")
		final Broadcast<BloomFilter> varD = sc.broadcast(bloomFilter);
		BroadCast_BF_Value = varD.value();
		
		
		JavaPairRDD<String, String> Rpairs = R.mapToPair(new PairFunction<String, String, String>() {
			@SuppressWarnings("unchecked")
			@Override
			public Tuple2<String, String> call(String arg0) throws Exception {
				int first = arg0.indexOf(",");
				String key = arg0.split(",")[0];
				String value = arg0.substring(first + 1);
				return new Tuple2(key, value);
			}
		});
		JavaPairRDD<String, String> Lpairs = L.mapToPair(new PairFunction<String, String, String>() {
			@SuppressWarnings("unchecked")
			public Tuple2<String, String> call(String x) {
				int first = x.indexOf(",");
				String key = x.split(",")[0];
				if(BroadCast_BF_Value.membershipTest(new Key(key.getBytes())) == true){
					String value = x.substring(first + 1);
					return new Tuple2(x.split(",")[0], value);
				}
				return null;
				
			}
		});
			
//		JavaPairRDD<String, String> result = Rpairs.cogroup(Lpairs).flatMapToPair(new PairFlatMapFunction<Tuple2<String,Tuple2<Iterable<String>,Iterable<String>>>, String, String>() {
//				@Override
//				public Iterator<Tuple2<String, String>> call(
//						Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>> arg0)
//						throws Exception {
//					ArrayList<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();
//					Iterator<String> iter1 = arg0._2._1.iterator();
//					Iterator<String> iter2 = arg0._2._2.iterator();
//					String str1 = "", str2 =""; int first, first1;
//					while(iter1.hasNext()){
//						str1 = iter1.next();
//						first = str1.indexOf(",");
//						while(iter2.hasNext()){
//							str2 = iter2.next();
//							first1 = str2.indexOf(",");
//							list.add(new Tuple2(str1.split(",")[0]+","+str2.split(",")[0],str1.substring(first+1)+","+str2.substring(first1+1)));
//						}
//					}
//					return list.iterator();
//				}
//			});
//			result.saveAsTextFile(output);
		
		Lpairs.join(Rpairs).saveAsTextFile(output);
			
	    sc.stop();
	}
}
