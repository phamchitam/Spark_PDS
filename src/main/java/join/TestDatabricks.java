package join;

import java.util.ArrayList;
import java.util.Arrays;
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

public class TestDatabricks {

	public static void main(String[] args) {
		long l = 1L;
		System.out.println("Result: " + Long.toBinaryString(4L));
		System.out.println("Result: " + Long.toBinaryString(l));
	}
}
