package vacuum;

import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import hash.MurmurHash_QF;
import scala.Tuple2;

public class TestVacuumFilter {

	public static void main(String[] args) {
		
		int maxItems = 14000000;
		int slots = 4;
		int max_kick_steps = 1000;
		long begin = System.currentTimeMillis();
		
		VacuumFilter vf = new VacuumFilter(16); 
		vf.init(maxItems, slots, max_kick_steps);
		VacuumFilter BroadCast_VF_Value;
		
		SparkConf conf = new SparkConf().setAppName("TestVacuumFilter");  
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		String inputL = "hdfs://10.96.37.70:9000/data/VData";
		String inputR = "hdfs://10.96.37.70:9000/data/VData";
		String output = "hdfs://10.96.37.70:9000/data/result";
		JavaRDD<String> R = sc.textFile(inputR);
		JavaRDD<String> L = sc.textFile(inputL);
		
		
		JavaRDD<String> rddVF = R.map(new Function<String, String>() {
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
		

		
		List<String> VF = rddVF.collect();
		for (String record : VF) {
			vf.insert(record);
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
		final Broadcast<VacuumFilter> BroadCast_VF = sc.broadcast(vf);
		BroadCast_VF_Value = BroadCast_VF.value();
		
		JavaPairRDD<String, String> Lpairs = L.mapToPair(new PairFunction<String, String, String>() {
			@SuppressWarnings("unchecked")
			public Tuple2<String, String> call(String x) {
				int first = x.indexOf(",");
				String key = x.split(",")[0];
				
				if(BroadCast_VF_Value.lookup(key)==true){

					String value = x.substring(first + 1);
					return new Tuple2(x.split(",")[0], value);
				}
				return null;
				
			}
		});
		
		
		Lpairs.join(Rpairs).saveAsTextFile(output);
//		Lpairs.join(Rpairs);
		
	    sc.stop();
	    
	    sc.close();
	    
		long end = System.currentTimeMillis();
		
		long total = end - begin;
		
		System.out.println("Time make filter : " + (medium - begin) + " Time join: " + (end - medium));
		
	}
}
