/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hash;

import java.io.Serializable;

import org.apache.commons.codec.binary.StringUtils;

/**
 * This is a very fast, non-cryptographic hash suitable for general hash-based
 * lookup. See http://murmurhash.googlepages.com/ for more details.
 * 
 * <p>
 * The C version of MurmurHash 2.0 found at that site was ported to Java by
 * Andrzej Bialecki (ab at getopt org).
 * </p>
 */

public class MurmurHash extends Hash implements Serializable {
	private static final long serialVersionUID = 1L;
	
    private static final int C1_32 = 0xcc9e2d51;
    private static final int C2_32 = 0x1b873593;
    private static final int R1_32 = 15;
    private static final int R2_32 = 13;
    private static final int M_32 = 5;
    private static final int N_32 = 0xe6546b64;

    // Constants for 128-bit variant
    private static final long C1 = 0x87c37b91114253d5L;
    private static final long C2 = 0x4cf5ad432745937fL;
    private static final int R1 = 31;
    private static final int R2 = 27;
    private static final int R3 = 33;
    private static final int M = 5;
    private static final int N1 = 0x52dce729;
    private static final int N2 = 0x38495ab5;
    public static final int DEFAULT_SEED = 104729;
    static final int LONG_BYTES = Long.SIZE / Byte.SIZE;
    static final int INTEGER_BYTES = Integer.SIZE / Byte.SIZE;



	private static MurmurHash _instance = new MurmurHash();

	public static Hash getInstance() {
		return _instance;
	}

	public MurmurHash() {

	}

	public int hash(byte[] data, int length, int seed) {
		int m = 0x5bd1e995;
		int r = 24;

		int h = seed ^ length;

		int len_4 = length >> 2;

		for (int i = 0; i < len_4; i++) {
			int i_4 = i << 2;
			int k = data[i_4 + 3];
			k = k << 8;
			k = k | (data[i_4 + 2] & 0xff);
			k = k << 8;
			k = k | (data[i_4 + 1] & 0xff);
			k = k << 8;
			k = k | (data[i_4 + 0] & 0xff);
			k *= m;
			k ^= k >>> r;
			k *= m;
			h *= m;
			h ^= k;
		}

		// avoid calculating modulo
		int len_m = len_4 << 2;
		int left = length - len_m;

		if (left != 0) {
			if (left >= 3) {
				h ^= (int) data[length - 3] << 16;
			}
			if (left >= 2) {
				h ^= (int) data[length - 2] << 8;
			}
			if (left >= 1) {
				h ^= (int) data[length - 1];
			}

			h *= m;
		}

		h ^= h >>> 13;
		h *= m;
		h ^= h >>> 15;

		return h;

	}

	public static int murMur2_32(final byte[] data, int length, int seed) {
		int m = 0x5bd1e995;
		int r = 24;

		int h = seed ^ length;

		int len_4 = length >>> 2;

		for (int i = 0; i < len_4; i++) {
			int i_4 = i << 2;
			int k = data[i_4 + 3];
			k = k << 8;
			k = k | (data[i_4 + 2] & 0xff);
			k = k << 8;
			k = k | (data[i_4 + 1] & 0xff);
			k = k << 8;
			k = k | (data[i_4 + 0] & 0xff);
			k *= m;
			k ^= k >>> r;
			k *= m;
			h *= m;
			h ^= k;
		}

		// avoid calculating modulo
		int len_m = len_4 << 2;
		int left = length - len_m;

		if (left != 0) {
			if (left >= 3) {
				h ^= (int) data[length - 3] << 16;
			}
			if (left >= 2) {
				h ^= (int) data[length - 2] << 8;
			}
			if (left >= 1) {
				h ^= (int) data[length - 1];
			}

			h *= m;
		}

		h ^= h >>> 13;
		h *= m;
		h ^= h >>> 15;

		return h;

	}

	public static int murMur2_32(final byte[] data, int length) {
		return murMur2_32(data, length, 0x9747b28c);
	}

	
	
    public static int murMur2_32(final String text) {
        final byte[] bytes = text.getBytes(); 
        return murMur2_32(bytes, bytes.length);
    }


    public static long murMur2_64(final byte[] data, int length, int seed) {
        final long m = 0xc6a4a7935bd1e995L;
        final int r = 47;

        long h = (seed&0xffffffffl)^(length*m);

        int length8 = length/8;

        for (int i=0; i<length8; i++) {
            final int i8 = i*8;
            long k =  ((long)data[i8+0]&0xff)      +(((long)data[i8+1]&0xff)<<8)
                    +(((long)data[i8+2]&0xff)<<16) +(((long)data[i8+3]&0xff)<<24)
                    +(((long)data[i8+4]&0xff)<<32) +(((long)data[i8+5]&0xff)<<40)
                    +(((long)data[i8+6]&0xff)<<48) +(((long)data[i8+7]&0xff)<<56);
            
            k *= m;
            k ^= k >>> r;
            k *= m;
            
            h ^= k;
            h *= m; 
        }
        
        switch (length%8) {
        case 7: h ^= (long)(data[(length&~7)+6]&0xff) << 48;
        case 6: h ^= (long)(data[(length&~7)+5]&0xff) << 40;
        case 5: h ^= (long)(data[(length&~7)+4]&0xff) << 32;
        case 4: h ^= (long)(data[(length&~7)+3]&0xff) << 24;
        case 3: h ^= (long)(data[(length&~7)+2]&0xff) << 16;
        case 2: h ^= (long)(data[(length&~7)+1]&0xff) << 8;
        case 1: h ^= (long)(data[length&~7]&0xff);
                h *= m;
        };
     
        h ^= h >>> r;
        h *= m;
        h ^= h >>> r;
        


        return h;

    }
    
    
    public static long murMur2_64(final byte[] data, int length) {
    	
        return murMur2_64(data, length, 0xe17a1465);
    }
	
    
    public static long murMur2_64(final String text) {
        final byte[] bytes = text.getBytes(); 
        return murMur2_64(bytes, bytes.length);
    }
    
     
    private static long getLittleEndianLong(final byte[] data, final int index) {
        return (((long) data[index    ] & 0xff)      ) |
               (((long) data[index + 1] & 0xff) <<  8) |
               (((long) data[index + 2] & 0xff) << 16) |
               (((long) data[index + 3] & 0xff) << 24) |
               (((long) data[index + 4] & 0xff) << 32) |
               (((long) data[index + 5] & 0xff) << 40) |
               (((long) data[index + 6] & 0xff) << 48) |
               (((long) data[index + 7] & 0xff) << 56);
    }

    /**
     * Gets the little-endian int from 4 bytes starting at the specified index.
     *
     * @param data The data
     * @param index The index
     * @return The little-endian int
     */
    private static int getLittleEndianInt(final byte[] data, final int index) {
        return ((data[index    ] & 0xff)      ) |
               ((data[index + 1] & 0xff) <<  8) |
               ((data[index + 2] & 0xff) << 16) |
               ((data[index + 3] & 0xff) << 24);
    }

    /**
     * Performs the intermediate mix step of the 32-bit hash function {@code MurmurHash3_x86_32}.
     *
     * @param k The data to add to the hash
     * @param hash The current hash
     * @return The new hash
     */
    private static int mix32(int k, int hash) {
        k *= C1_32;
        k = Integer.rotateLeft(k, R1_32);
        k *= C2_32;
        hash ^= k;
        return Integer.rotateLeft(hash, R2_32) * M_32 + N_32;
    }

    /**
     * Performs the final avalanche mix step of the 32-bit hash function {@code MurmurHash3_x86_32}.
     *
     * @param hash The current hash
     * @return The final hash
     */
    public static int fmix32(int hash) {
        hash ^= (hash >>> 16);
        hash *= 0x85ebca6b;
        hash ^= (hash >>> 13);
        hash *= 0xc2b2ae35;
        hash ^= (hash >>> 16);
        return hash;
    }


    /**
     * Performs the final avalanche mix step of the 64-bit hash function {@code MurmurHash3_x64_128}.
     *
     * @param hash The current hash
     * @return The final hash
     */
    public static long fmix64(long hash) {
        hash ^= (hash >>> 33);
        hash *= 0xff51afd7ed558ccdL;
        hash ^= (hash >>> 33);
        hash *= 0xc4ceb9fe1a85ec53L;
        hash ^= (hash >>> 33);
        return hash;
    }
    
    
    
    public static int murMur3_32(final byte[] data, final int offset, final int length, final int seed) {
        int hash = seed;
        final int nblocks = length >>> 2;

        // body
        for (int i = 0; i < nblocks; i++) {
            final int index = offset + (i << 2);
            final int k = getLittleEndianInt(data, index);
            hash = mix32(k, hash);
        }


        // tail
        // ************
        // Note: This fails to apply masking using 0xff to the 3 remaining bytes.
        // ************
        final int index = offset + (nblocks << 2);
        int k1 = 0;
        switch (offset + length - index) {
        case 3:
            k1 ^= data[index + 2] << 16;
        case 2:
            k1 ^= data[index + 1] << 8;
        case 1:
            k1 ^= data[index];

            // mix functions
            k1 *= C1_32;
            k1 = Integer.rotateLeft(k1, R1_32);
            k1 *= C2_32;
            hash ^= k1;
        }

        hash ^= length;
//        return fmix32(hash);
        return hash;
    }

    public static int murMur3_32(final byte[] data, final int length, final int seed) {
        return murMur3_32(data, 0, length, seed);
    }
    
    public static int murMur3_32(final byte[] data, final int length) {
        return murMur3_32(data, 0, length, DEFAULT_SEED );
    }
    
    public static int murMur3_32(final byte[] data) {
        return murMur3_32(data, 0, data.length, DEFAULT_SEED );
    }
    
    public static int murMur3_32(final String data) {
        final byte[] bytes = StringUtils.getBytesUtf8(data);
        return murMur3_32(bytes, 0, bytes.length, DEFAULT_SEED);
    }

    public static long murMur3_64(final byte[] data, final int offset, final int length, final int seed) {
        // ************
        // Note: This fails to apply masking using 0xffffffffL to the seed.
        // ************
        long hash = seed;
        final int nblocks = length >>> 3;

        // body
        for (int i = 0; i < nblocks; i++) {
            final int index = offset + (i << 3);
            long k = getLittleEndianLong(data, index);

            // mix functions
            k *= C1;
            k = Long.rotateLeft(k, R1);
            k *= C2;
            hash ^= k;
            hash = Long.rotateLeft(hash, R2) * M + N1;
        }

        // tail
        long k1 = 0;
        final int index = offset + (nblocks << 3);
        switch (offset + length - index) {
        case 7:
            k1 ^= ((long) data[index + 6] & 0xff) << 48;
        case 6:
            k1 ^= ((long) data[index + 5] & 0xff) << 40;
        case 5:
            k1 ^= ((long) data[index + 4] & 0xff) << 32;
        case 4:
            k1 ^= ((long) data[index + 3] & 0xff) << 24;
        case 3:
            k1 ^= ((long) data[index + 2] & 0xff) << 16;
        case 2:
            k1 ^= ((long) data[index + 1] & 0xff) << 8;
        case 1:
            k1 ^= ((long) data[index] & 0xff);
            k1 *= C1;
            k1 = Long.rotateLeft(k1, R1);
            k1 *= C2;
            hash ^= k1;
        }

        // finalization
        hash ^= length;
//        hash = fmix64(hash);
        

        
        return hash;
    }

    
    public static long murMur3_64(final byte[] data, final int length, final int seed) {
        return murMur3_64(data, 0, length, seed);
    }
    
    
    public static long murMur3_64(final byte[] data, final int length) {
        return murMur3_64(data, 0, length, DEFAULT_SEED);
    }
    
    
    public static long murMur3_64(final byte[] data) {
        return murMur3_64(data, 0, data.length, DEFAULT_SEED);
    }
    
    public static long murMur3_64(final long data) {
        long hash = DEFAULT_SEED;
        long k = Long.reverseBytes(data);
        final int length = LONG_BYTES;
        // mix functions
        k *= C1;
        k = Long.rotateLeft(k, R1);
        k *= C2;
        hash ^= k;
        hash = Long.rotateLeft(hash, R2) * M + N1;
        // finalization
        hash ^= length;
//        hash = fmix64(hash);
        return hash;
    }
    
    public static long murMur3_64(final int data) {
        long k1 = Integer.reverseBytes(data) & (-1L >>> 32);
        final int length = INTEGER_BYTES;
        long hash = DEFAULT_SEED;
        k1 *= C1;
        k1 = Long.rotateLeft(k1, R1);
        k1 *= C2;
        hash ^= k1;
        // finalization
        hash ^= length;
//        hash = fmix64(hash);
        return hash;
    }
    
    public static long murMur3_64(final String data) {
        final byte[] bytes = data.getBytes();
        return murMur3_64(bytes, 0, bytes.length, DEFAULT_SEED);
    }
  
}
