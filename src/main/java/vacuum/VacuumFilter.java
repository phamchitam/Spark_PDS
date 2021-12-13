package vacuum;

import java.io.Serializable;
import java.util.Random;

import hash.MurmurHash;

public class VacuumFilter implements Serializable {
	public int n;
	public int m;
	public long memory_consumption;
	public int max_2_power;
	public int big_seg;
	public int[] len = new int[4];
	public int[] encode_table = new int[1<<16];
	public int[] decode_table = new int[1<<16];
    public int filled_cell;
    public int full_bucket;
    public int max_kick_steps;
    public int fp_len; // Note
    public int maxValue = Integer.MAX_VALUE;
    
    public boolean debug_flag = false;
    public boolean balance = true;
    public long[] table;
    
    public VacuumFilter() {
  	
    }
    
    public VacuumFilter(int fp_len) {
    	this.fp_len = fp_len;
    }
	
	public void init(int max_item, int _m, int _step) {
	    int _n = (int) (max_item / 0.96 / 4);  //Note : Why divide 4 ?


	    if (_n < 10000) {
	        if (_n < 256)
	            big_seg = (upperpower2(_n));
	        else
	            big_seg = (upperpower2(_n / 4));
	        _n = roundUp(_n, big_seg);
	        len[0] = big_seg - 1;
	        len[1] = big_seg - 1;
	        len[2] = big_seg - 1;
	        len[3] = big_seg - 1;
	    } else {
	        big_seg = 0;
	        big_seg = Math.max(big_seg, proper_alt_range(_n, 0, len));
	        int new_n = roundUp(_n, big_seg);
	        _n = new_n;

	        big_seg--;
	        len[0] = big_seg;
	        for (int i = 1; i < 4; i++) len[i] = proper_alt_range(_n, i, len) - 1;
	        len[0] = Math.max(len[0], 1024);
	        len[3] = (len[3] + 1) * 2 - 1;
	    }

	    this.n = _n;
	    this.m = _m;
	    this.max_kick_steps = _step;
	    this.filled_cell = 0;
	    this.full_bucket = 0;

	    long how_many_bit = (long) this.n * this.m * (fp_len - 1);
	    this.memory_consumption = roundUp(how_many_bit + 64, 8) / 8 + 8;  // how many bytes !

	    max_2_power = 1;
	    
	    for (; max_2_power * 2 < _n;) {
	    	max_2_power <<= 1;
	    }
	    
	    this.table = new long[(int) (how_many_bit/64 + 1)];

	    int index = 0;
	    for (int i = 0; i < 16; i++)
	        for (int j = 0; j < ((i == 0) ? 1 : i + 1); j++)
	            for (int k = 0; k < ((j == 0) ? 1 : j + 1); k++)
	                for (int l = 0; l < ((k == 0) ? 1 : k + 1); l++) {
	                    int plain_bit = (i << 12) + (j << 8) + (k << 4) + l;
	                    encode_table[plain_bit] = index;
	                    decode_table[index] = plain_bit;
	                    ++index;
	                }
	}
	
	int upperpower2(int x) {
		int ret = 1;
		for (; ret < x; ){
			ret = ret << 1;
		}
		return ret;
	}
	
	int roundDown(long a, int b) {
		return (int) (a - (a%b));
	}
	
	int roundUp(long a, int b) {
		return  (int) (roundDown(a+b-1,b));
	}
	
	int proper_alt_range(int M, int i, int[] len) {
	    double b = 4;      // slots per bucket
	    double lf = 0.95;  // target load factor
	    int alt_range = 8;
	    for (; alt_range < M;) {
	        double f = (4 - i) * 0.25;
	        if (balls_in_bins_max_load(f * b * lf * M, M * 1.0 / alt_range) <
	            0.97 * b * alt_range) {
	            return alt_range;
	        }
	        alt_range <<= 1;
	    }
	    return alt_range;
	}
	
	double balls_in_bins_max_load(double balls, double bins) {
	    double m = balls;
	    double n = bins;
	    if (n == 1) return m;

	    double c = m / (n * Math.log(n));
	    // A more accurate bound..
	    if (c < 5)
	    {
	        double dc = solve_equation(c);
	        double ret = (dc - 1 + 2) * Math.log(n);
	        return ret;
	    }

	    double ret = (m / n) + 1.5 * Math.sqrt(2 * m / n * Math.log(n)); //Note
	    return ret;
	}
	
	double F_d(double x, double c) { return Math.log(c) - Math.log(x); }
	
	double F(double x, double c) { return 1 + x * (Math.log(c) - Math.log(x) + 1) - c; }
	
	double solve_equation(double c) {
	    double x = c + 0.1;
	    while (Math.abs(F(x, c)) > 0.001) x -= F(x, c) / F_d(x, c);
	    return x;
	}
	
	void clearFilter(int filled_cell, long memory_consumption) {
		
	}

	
	// Note
	int fingerprint(long ele) {

	    int h = (int) (     (MurmurHash.fmix64(ele ^ 0x192837319273L) % ((1L << fp_len) - 1) +  ((1L << fp_len) - 1))% ((1L << fp_len) - 1)  + 1 );

	    return h;
	}
	
	int lookup_in_bucket(int pos, int fp){
	    // If lookup success return 1
	    // If lookup fail and the bucket is full return 2
	    // If lookup fail and the bucket is not full return 3
		


	    long[] store = new long[8];
	    get_bucket(pos, store);

	    boolean isFull = true;
	    

	    for (int i = 0; i < this.m; i++) {
	        long t = store[i];
//	       	System.out.println("Pos: " + pos + ". Store[" + i + "] in pos: " + store[i]);
	        long tmp = (long) fp;

	        if (t == tmp) {

	        	return 1;
	        }
	        
 
	        isFull = isFull && (t != 0);
	    }
	    return (isFull) ? 2 : 3;
	}
	
	int insert_to_bucket(long[] store, long fp) {
		   // if success return 0
	    // if find collision : return 1 + position
	    // if full : return 1 + 4

	    if (store[this.m] == this.m)
	        return 1 + 4;
	    else {
	        store[3] = fp; // sorted -- store[3] must be empty !

	        return 0;
	    }
	    

  
 
	}
	
	void set_bucket(int pos, long[] store) {
		
		   // 0. sort store ! descendant order >>>>>>

	    sort_pair(store,0,2);
	    sort_pair(store,1,3);
	    sort_pair(store,0,1);
	    sort_pair(store,2,3);
	    sort_pair(store,1,2);
	    // 1. compute the encode

	    long high_bit = 0;
	    long low_bit = 0;
	    
 

	    low_bit =
	        (store[3] & ((1 << (fp_len - 4)) - 1)) |
	        ((store[2] & ((1 << (fp_len - 4)) - 1)) << (1 * (fp_len - 4))) |
	        ((store[1] & ((1 << (fp_len - 4)) - 1)) << (2 * (fp_len - 4))) |
	        ((store[0] & ((1 << (fp_len - 4)) - 1)) << (3 * (fp_len - 4)));
	    

	    high_bit = ((store[3] >>> (fp_len - 4)) & ((1 << 4) - 1)) |
	               (((store[2] >>> (fp_len - 4)) & ((1 << 4) - 1)) << 4) |
	               (((store[1] >>> (fp_len - 4)) & ((1 << 4) - 1)) << 8) |
	               (((store[0] >>> (fp_len - 4)) & ((1 << 4) - 1)) << 12);
	    

	    // 2. store into memory
	    long high_encode = encode_table[(int)high_bit];

	    long all_encode = (high_encode << (4 * (fp_len - 4))) | low_bit;

	    int bucket_length = (fp_len - 1) * 4;
	    long start_bit_pos = (long) pos * bucket_length;
  	    long end_bit_pos = start_bit_pos + bucket_length - 1;
        int writing_lower_bound = (int) start_bit_pos & 63;
        int writing_upper_bound = (int) end_bit_pos & 63;

	    if (roundDown(start_bit_pos, 64) == roundDown(end_bit_pos, 64)) {
	        long unit = table[roundDown(start_bit_pos, 64) / 64];
	        if (writing_upper_bound == 63) {
			    table[roundDown(start_bit_pos, 64) / 64] =	(unit & ((1L << writing_lower_bound) - 1))|  	(all_encode  << writing_lower_bound);
	        } else {
	        	table[roundDown(start_bit_pos, 64) / 64] =	(unit & (((1L << writing_lower_bound) - 1)|(((-1L) >>>(writing_upper_bound + 1)) << (writing_upper_bound + 1)))) |  
				(all_encode  << writing_lower_bound);
	        }

	    } else {
	        long unit1 = this.table[roundDown(start_bit_pos, 64) / 64];
	        long unit2 = this.table[roundDown(start_bit_pos, 64) / 64 + 1];
	        long lower_part = all_encode & ((1L << (64 - writing_lower_bound)) - 1);
	        long higher_part = all_encode >> (64 - writing_lower_bound);
	        table[(int) roundDown(start_bit_pos, 64) / 64] = 
	            (unit1 & ((1L << writing_lower_bound) - 1)) | (lower_part << writing_lower_bound);
	        table[roundDown(start_bit_pos, 64) / 64 + 1] = 
	            (unit2 >>> (writing_upper_bound + 1) << (writing_upper_bound + 1)) | higher_part;
	    }

	}

		
	public double get_load_factor() {
		return filled_cell * 1.0 / (this.n*this.m);
	}
	
	public double get_full_bucket_factor() {
	    return full_bucket * 1.0 / this.n;
	}
	
	public long position_hash(long ele){
		return (ele % n + n) % n;
	};

	
	void get_bucket(int pos, long[] store) {
		// Default :
	    //
	    // Little Endian Store
	    // Store by uint32_t
	    // store[this -> m] = bucket number

	    // 1. read the endcoded bits from memory

	    int bucket_length = (fp_len - 1) * 4;
	    long start_bit_pos = (long) (pos * bucket_length);
	    long end_bit_pos = start_bit_pos + bucket_length - 1;
	    long result = 0;

   
        if (roundDown(start_bit_pos, 64) == roundDown(end_bit_pos, 64)) {

	    	int id = roundDown(start_bit_pos, 64) / 64;
  	        long unit = table[id];

	        int reading_lower_bound = (int) start_bit_pos & 63;
	        int reading_upper_bound = (int) end_bit_pos & 63;

	        result = (unit & ((-1L) >>> (63 - reading_upper_bound))) >>>
	                 reading_lower_bound;
			
	    } else {
	    	

	        long unit1 = table[roundDown(start_bit_pos, 64) / 64];
	        long unit2 = table[roundDown(start_bit_pos, 64) / 64 + 1];


	        int reading_lower_bound = (int) start_bit_pos & 63;
	        int reading_upper_bound = (int) end_bit_pos & 63;

	        long t1 = unit1 >>> reading_lower_bound;
	        long t2 = (unit2 & ((1L << (reading_upper_bound + 1)) - 1))
	                      << (64 - reading_lower_bound);
	        result = t1 | t2;
	    }

	    // 2. read the 4 elements from the encoded bits
	    // We use 12 bits to store the 16 most significant bits for the items in
	    // bucket, 4 bits per item the low bits are stored in the remaining bits
	    //
	    // For example, 8 bits per item , require 28 bits to store:
	    //
	    // Original :
	    //
	    // hhhh llll
	    // hhhh llll
	    // hhhh llll
	    // hhhh llll
	    //
	    // encoded :
	    //
	    //
	    // 0 - 11                       12 - 15    16 - 19  20-23   24 - 27
	    // HHHHHHHHHHHH                 llll       llll     llll    llll
	    //  encoded high bit(12 bits)   item 0     item 1   item 2  item 3
	    //
	    int decode_result = (int) decode_table[(int)(result >>> (4 * (fp_len - 4)))];

	    store[3] = (result & ((1 << (fp_len - 4)) - 1)) |
	               ((decode_result & ((1 << 4) - 1)) << (fp_len - 4));
	    store[2] = ((result >>> (1 * (fp_len - 4))) & ((1 << (fp_len - 4)) - 1)) |
	               (((decode_result >>> 4) & ((1 << 4) - 1)) << (fp_len - 4));
	    store[1] = ((result >>> (2 * (fp_len - 4))) & ((1 << (fp_len - 4)) - 1)) |
	               (((decode_result >>> 8) & ((1 << 4) - 1)) << (fp_len - 4));
	    store[0] = ((result >>> (3 * (fp_len - 4))) & ((1 << (fp_len - 4)) - 1)) |
	               (((decode_result >>> 12) & ((1 << 4) - 1)) << (fp_len - 4));

	    store[4] = 0;
	    if(store[0] != 0) store[4] += 1;
	    if(store[1] != 0) store[4] += 1;
	    if(store[2] != 0) store[4] += 1;
	    if(store[3] != 0) store[4] += 1;

	}
	
	int alternate(int pos, long fp) {
		 long fp_hash = fp * 0x5bd1e995;
	     int seg = this.len[(int)fp & 3];
	     return (int) (pos ^ (fp_hash & seg));
	}


	void sort_pair(long[] store, int i, int j) {
		long tmp;
		if (store[i] < store[j]) {
			tmp = store[i];
			store[i] = store[j];
			store[j] = tmp;
		}
	}
	

	public void clear() {
	    this.filled_cell = 0;
	    clearFilter(this.filled_cell, this.memory_consumption);
		
	}


//    public boolean insert(long ele) {
    public boolean insert(String element) {

		// If insert success return true
        // If insert fail return false
    	Random rand = new Random();
    	long elementHashCode = 0L;
//      ele = HashUtil.MurmurHash64(ele ^ 0x12891927);
    	
    	elementHashCode = MurmurHash.murMur3_64(element);
    	elementHashCode = MurmurHash.fmix64(elementHashCode ^ 0x12891927);

        long fp = this.fingerprint(elementHashCode);
        int cur1 = (int) this.position_hash(elementHashCode);
        
        int cur2 = alternate(cur1, fp);
  
        long[] store1 = new long[8];
        long[] store2 = new long[8];

        this.get_bucket(cur1, store1);
        this.get_bucket(cur2, store2);

        if (store1[this.m] <= store2[this.m]) {
            if (this.insert_to_bucket(store1, fp) == 0) {

                this.filled_cell++;
                this.set_bucket(cur1, store1);
                return true;
            }
        } else {
            if (this.insert_to_bucket(store2, fp) == 0) {
                this.filled_cell++;
                this.set_bucket(cur2, store2);
                return true;
            }
        }

        

        // randomly choose one bucket's elements to kick
        int rk = rand.nextInt(maxValue) % this.m;

        // get those item
        int cur;
        long[] cur_store;

        if ((rk & 1) == 1) {
            cur = cur1;
            cur_store = store1;
        }
        else
            {
        	cur = cur2;
        	cur_store = store2;
            }

        long tmp_fp = cur_store[rk];
        cur_store[rk] = fp;
        this.set_bucket(cur, cur_store);

        int alt = alternate(cur, tmp_fp);

        for (int i = 0; i < this.max_kick_steps; i++) {
        	for(int a = 0; a < store1.length; a++) {
        		store1[a] = 0;
        	}
//            memset(store1, 0, sizeof(store1));
            this.get_bucket(alt, store1);
            if (store1[this.m] == this.m) {
                for (int j = 0; j < this.m; j++) {
                    int nex = alternate(alt, store1[j]);
                    this.get_bucket(nex, store2);
                    if (store2[this.m] < this.m) {
                        store2[this.m - 1] = store1[j];
                        store1[j] = tmp_fp;
                        this.filled_cell++;
                        this.set_bucket(nex, store2);
                        this.set_bucket(alt, store1);
                        return true;
                    }
                }

                rk = rand.nextInt(maxValue) % this.m;
                fp = store1[rk];
                store1[rk] = tmp_fp;
                this.set_bucket(alt, store1);

                tmp_fp = fp;
                alt = alternate(alt, tmp_fp);
            } else {
                store1[this.m - 1] = tmp_fp;
                this.filled_cell++;
                this.set_bucket(alt, store1);
                return true;
            }
        }
        return false;
    }

	public boolean lookup(String element) {

		long elementHashCode = 0;
		
//	    ele = HashUtil.MurmurHash64(ele ^ 0x12891927);
		
	    elementHashCode = MurmurHash.murMur3_64(element);
	    elementHashCode = MurmurHash.fmix64(elementHashCode ^ 0x12891927);

	    int fp = fingerprint(elementHashCode);
	    int pos1 = (int) this.position_hash(elementHashCode);
	    

	    int ok1 = lookup_in_bucket(pos1, fp);
	    

	    


	    if (ok1 == 1) return true;
//	    if (ok1 == 3) return false;
	    


	    int pos2 = alternate(pos1, fp);
	    

	    

	    assert(pos1 == alternate(pos2, fp));
	    int ok2 = lookup_in_bucket(pos2, fp);

	    return ok2 == 1;
	}


	public int del_in_bucket(int pos, long fp) {
	    long[]  store = new long[8];
	    get_bucket(pos, store);

	    for (int i = 0; i < this.m; i++) {
	        long t = store[i];
	        if (t == fp) {
	            store[i] = 0;
	            --this.filled_cell;
	            set_bucket(pos, store);
	            return 1;
	        }
	    }
	    return 0;
	}
	
	public boolean del(String element) {	
		
		long elementHashCode = 0;
		elementHashCode = MurmurHash.murMur3_64(element);
		elementHashCode = MurmurHash.fmix64(elementHashCode ^ 0x12891927);
		
	    long fp = fingerprint(elementHashCode);
	    int pos1 = (int) this.position_hash(elementHashCode);

	    int ok1 = del_in_bucket(pos1, fp);

	    if (ok1 == 1) return true;
	    // if (ok1 == 3) return false;

	    int pos2 = alternate(pos1, fp);
	    int ok2 = del_in_bucket(pos2, fp);

	    return ok2 == 1;
	}
	
	
}


