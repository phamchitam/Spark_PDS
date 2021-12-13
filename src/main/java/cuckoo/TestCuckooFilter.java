package cuckoo;

import com.github.mgunlogson.cuckoofilter4j.CuckooFilter;
import com.google.common.hash.Funnels;

import java.io.IOException;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

public class TestCuckooFilter {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException {
		
		long begin = System.currentTimeMillis();
		
		@SuppressWarnings("rawtypes")
		CuckooFilter BroadCast_CF_Value;
		CuckooFilter<CharSequence> cf = new CuckooFilter.Builder<>(Funnels.stringFunnel(), 12000000).build();
		
		SparkConf conf = new SparkConf().setAppName("TestCuckooFilter");  
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		String inputL = "hdfs://172.31.16.100:9000/data/Left";
		String inputR = "hdfs://172.31.16.100:9000/data/Right";
		String output = "hdfs://172.31.16.100:9000/data/result";
		JavaRDD<String> R = sc.textFile(inputR);
		JavaRDD<String> L = sc.textFile(inputL);
				
		
		JavaRDD<String> rddCF = R.map(new Function<String, String>() {
			@Override
			public String call(String arg0) throws Exception {
				String [] fields = arg0.split(",");
				if (fields != null && 0 < fields.length  
						&& fields[0].trim().length() > 0 ) {
					return fields[0];
				}
				return "";
			}

		}).filter(t -> (t.trim().length() > 0));
	
		List<String> CF = rddCF.collect();
		for (String record : CF) {
			cf.put(record);
		}
		
		long medium = System.currentTimeMillis();

		JavaPairRDD<String, String> Rpairs = R.mapToPair(new PairFunction<String, String, String>() {
			@SuppressWarnings("unchecked")
			@Override
			public Tuple2<String, String> call(String arg0) throws Exception {
				int first = arg0.indexOf(",");
				String key = arg0.split(",")[1];
				String value = arg0.substring(0,first);
				return new Tuple2(key, value);
			}
		});

		@SuppressWarnings("rawtypes")
		final Broadcast<CuckooFilter> BroadCast_CF = sc.broadcast(cf);
		BroadCast_CF_Value = BroadCast_CF.value();
		
		JavaPairRDD<String, String> Lpairs = L.mapToPair(new PairFunction<String, String, String>() {
			@SuppressWarnings("unchecked")
			public Tuple2<String, String> call(String x) {
				int first = x.indexOf(",");
				String key = x.split(",")[0];
				
				if(BroadCast_CF_Value.mightContain(key)==true){
					String value = x.substring(first + 1);
					return new Tuple2(x.split(",")[0], value);
				}
				return null;
			}
		});
		
		
//		Lpairs.join(Rpairs).saveAsTextFile(output);
		
		Lpairs.join(Rpairs);
		
	    sc.stop();
	    
	    sc.close();
	    
		long end = System.currentTimeMillis();
		
		long total = end - begin;
		
		System.out.println("Time make filter : " + (medium - begin) + " Time join: " + (end - medium));
	}
}