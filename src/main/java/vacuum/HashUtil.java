package vacuum;

public class HashUtil {
    /**
     * @brief mix 3 32-bit values reversibly
     *
     * For every delta with one or two bits set, and the deltas of all three
     * high bits or all three low bits, whether the original value of a,b,c
     * is almost all zero or is uniformly distributed.
     *
     * If mix() is run forward or backward, at least 32 bits in a,b,c
     * have at least 1/4 probability of changing.
     *
     * If mix() is run forward, every bit of c will change between 1/3 and
     * 2/3 of the time.  (Well, 22/100 and 78/100 for some 2-bit deltas.)
     * mix() was built out of 36 single-cycle latency instructions in a 
     * structure that could supported 2x parallelism, like so:
     *    a -= b; 
     *    a -= c; x = (c>>13);
     *    b -= c; a ^= x;
     *    b -= a; x = (a<<8);
     *    c -= a; b ^= x;
     *    c -= b; x = (b>>13);
     *     ...
     *
     * Unfortunately, superscalar Pentiums and Sparcs can't take advantage 
     * of that parallelism.  They've also turned some of those single-cycle
     * latency instructions into multi-cycle latency instructions. Still,
     * this is the fastest good hash I could find. There were about 2^68
     * to choose from. I only looked at a billion or so.
     */
    static void BOBHASH_MIX(int a, int b, int c)
    { 
      a -= b; a -= c; a ^= (c>>13); 
      b -= c; b -= a; b ^= (a<<8);  
      c -= a; c -= b; c ^= (b>>13); 
      a -= b; a -= c; a ^= (c>>12); 
      b -= c; b -= a; b ^= (a<<16); 
      c -= a; c -= b; c ^= (b>>5);  
      a -= b; a -= c; a ^= (c>>3);  
      b -= c; b -= a; b ^= (a<<10); 
      c -= a; c -= b; c ^= (b>>15); 
    };

    /**
     * Every bit of the key affects every bit of the return value. 
     * Every 1-bit and 2-bit delta achieves avalanche.
     * About 6 * length + 35 instructions.
     *
     * Use for hash table lookup, or anything where one collision in 2^32 is acceptable.
     * Do NOT use for cryptographic purposes.
     */

    public static int BobHash32(String key, int key_size)
    {
        int BOBHASH_GOLDEN_RATIO = 0x9e3779b9;
        int a = BOBHASH_GOLDEN_RATIO;
        int b = BOBHASH_GOLDEN_RATIO;
        int c = 0;
        int length = key_size;

        byte[] work_key = key.getBytes();

        /* handle most of the key */
        
    	int index = 0;
        while (length >= 12)
        {

            a += (work_key[index] + ((int)work_key[index+1] << 8) + ((int)work_key[index+2] << 16) + ((int)work_key[index+3] << 24));
            b += (work_key[index+4] + ((int)work_key[index+5] << 8) + ((int)work_key[index+6] << 16) + ((int)work_key[index+7] << 24));
            c += (work_key[index+8] + ((int)work_key[index+9] << 8) + ((int)work_key[index+10] << 16)+ ((int)work_key[index+11] << 24));
            BOBHASH_MIX (a,b,c);
            index += 12; 
            length -= 12;
        }

      /* handle the last 11 bytes */
      c += key_size;
      switch (length)
      {
        case 11: c += ((int)work_key[index+10] << 24);
        case 10: c += ((int)work_key[index+9] << 16);
        case 9 : c += ((int)work_key[index+8] << 8);
        case 8 : b += ((int)work_key[index+7] << 24);
        case 7 : b += ((int)work_key[index+6] << 16);
        case 6 : b += ((int)work_key[index+5] << 8);
        case 5 : b += work_key[index+4];
        case 4 : a += ((int)work_key[index+3] << 24);
        case 3 : a += ((int)work_key[index+2] << 16);
        case 2 : a += ((int)work_key[index+1] << 8);
        case 1 : a += work_key[index];
      }

      BOBHASH_MIX (a,b,c);

      return c & (0x7fffffff); // 32-bit
    }

    static int MurmurHash32(int h)
    {
        h ^= h >>> 16;
        h *= 0x85ebca6b;
        h ^= h >>> 13;
        h *= 0xc2b2ae35;
        h ^= h >>> 16;
        return h;
    }

    static long MurmurHash64(long h)
    {
        h ^= h >>> 33;
        h *= 0xff51afd7ed558ccdL;
        h ^= h >>> 33;
        h *= 0xc4ceb9fe1a85ec53L;
        h ^= h >>> 33;   
        return h;
    }
} ;