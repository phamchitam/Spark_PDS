package bloom;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.BitSet;
//import join.FileCompressor;
//import join.FileDecompressor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.util.hash.Hash;

/**
 * Author: Thuong-Cang PHAN and adapted by TTTQuyen
 * Address: Can Tho University
 * Email: ptcang@cit.ctu.edu.vn and tranthitoquyen@cit.ctu.edu.vn
 *
 */
public class BloomFilter extends Filter implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static final byte[] bitvalues = new byte[] { (byte) 0x01, (byte) 0x02, (byte) 0x04, (byte) 0x08,
			(byte) 0x10, (byte) 0x20, (byte) 0x40, (byte) 0x80 };

	/** The bit vector. */
	public BitSet bits;

	/** Default constructor - use with readFields */
	public BloomFilter() {
		super();
	}

	/*	*//**
			 * contructor for read field
			 * 
			 * @param size
			 * @author tttquyen
			 */
	public BloomFilter(int size) {
		super();
		vectorSize = size;
	}

	/**
	 * Constructor
	 * 
	 * @param vectorSize
	 *            The vector size of <i>this</i> filter. // Large (Tinh toan sao
	 *            cho it ton bo nho nhat. Demo 150000 bit)
	 * @param nbHash
	 *            The number of hash function to consider. // 8 hash
	 * @param hashType
	 *            type of the hashing function (see
	 *            {@link org.apache.hadoop.util.hash.Hash}).
	 */
	public BloomFilter(int vectorSize, int nbHash, int hashType) {
		super(vectorSize, nbHash, hashType);
		bits = new BitSet(this.vectorSize);
	}

	// Cang's additional function
	public BloomFilter(BloomFilter copy) {
		super(copy.vectorSize, copy.nbHash, copy.hashType);

		bits = new BitSet(copy.vectorSize);
		bits.or(copy.bits);
	}

	// Cang's additional function
	public int getnbHash() {
		return this.nbHash;
	}
	
	/**
	 * for spark accumulator
	 * @param bf
	 * @return
	 */
	public BloomFilter add(BloomFilter bf) {
		bits.or(bf.bits);
		return this;
		
	}

	@Override
	public void add(Key key) {
		if (key == null || key.toString().length() <= 0) {
			throw new NullPointerException("key cannot be null");
		}

		int[] h = hash.hash(key);
		hash.clear();

		for (int i = 0; i < nbHash; i++) {
			bits.set(h[i]);
		}

	}
	
	public void add(int [] h){
		for (int i = 0; i < nbHash; i++) {
			bits.set(h[i]);
		}
	}



	@Override
	public boolean membershipTest(Key key) {
		if (key == null || key.toString().length() <= 0) {
			throw new NullPointerException("key cannot be null");
		}
		int[] h = hash.hash(key);
		hash.clear();
		for (int i = 0; i < nbHash; i++) {
			if (!bits.get(h[i])) {
				return false;
			}
		}
		return true;
	}
	//added
	public boolean membershipTest(int [] pos) {
		//hash.clear();
		for (int i = 0; i < nbHash; i++) {
			if (!bits.get(pos[i])) {
				return false;
			}
		}
		return true;
	}

	@Override
	public void not() {
		bits.flip(0, vectorSize - 1);
	}

	
	@Override
	public void and(Filter filter) {
		if (filter == null || !(filter instanceof BloomFilter)
		// || filter.vectorSize != this.vectorSize
				|| filter.nbHash != this.nbHash) {
			throw new IllegalArgumentException("filters cannot be and-ed");
		}

		this.bits.and(((BloomFilter) filter).bits);
	}  
	
	
	@Override
	public void or(Filter filter) {
		if (filter == null || !(filter instanceof BloomFilter)
		// || filter.vectorSize != this.vectorSize
				|| filter.nbHash != this.nbHash) {
			throw new IllegalArgumentException("filters cannot be or-ed");
		}
		bits.or(((BloomFilter) filter).bits);
	}

	@Override
	public void xor(Filter filter) {
		if (filter == null || !(filter instanceof BloomFilter)
		// || filter.vectorSize != this.vectorSize
				|| filter.nbHash != this.nbHash) {
			throw new IllegalArgumentException("filters cannot be xor-ed");
		}
		bits.xor(((BloomFilter) filter).bits);
	}

	@Override
	public String toString() {
		return bits.toString();
	}

	/**
	 * @return size of the the bloomfilter
	 */
	public int getVectorSize() {
		return this.vectorSize;
	}
	// Writable

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		byte[] bytes = new byte[getNBytes()];
		for (int i = 0, byteIndex = 0, bitIndex = 0; i < vectorSize; i++, bitIndex++) {
			if (bitIndex == 8) {
				bitIndex = 0;
				byteIndex++;
			}
			if (bitIndex == 0) {
				bytes[byteIndex] = 0;
			}
			if (bits.get(i)) {
				bytes[byteIndex] |= bitvalues[bitIndex];
			}
		}
		out.write(bytes);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		bits = new BitSet(this.vectorSize);
		byte[] bytes = new byte[getNBytes()];
		in.readFully(bytes);
		for (int i = 0, byteIndex = 0, bitIndex = 0; i < vectorSize; i++, bitIndex++) {
			if (bitIndex == 8) {
				bitIndex = 0;
				byteIndex++;
			}
			if ((bytes[byteIndex] & bitvalues[bitIndex]) != 0) {
				bits.set(i);
			}
		}
	}

	/* @return number of bytes needed to hold bit vector */
	private int getNBytes() {
		return (vectorSize + 7) / 8;
	}
	

	/**
	 * Load a Local Filter File into the bloom filter
	 * 
	 * @param localFilePathName
	 * @param conf
	 * @return
	 * @throws IOException
	 * @author tttquyen
	 */
	public boolean getLocalFilterFile(String localFilePathName, Configuration conf) throws IOException {
		boolean result = false;
		// FSDataInputStream bfFileInputStream=null;
		DataInputStream bfFileInputStream = null;

		// Opening the local filter file
		FileInputStream fileInputStream = new FileInputStream(localFilePathName);
		bfFileInputStream = new DataInputStream(fileInputStream);

		if (bfFileInputStream != null) {
			this.readFields(bfFileInputStream);
			bfFileInputStream.close();
			fileInputStream.close();
			result = true;
		}

		return result;

	}


	/**
	 * Check filter is empty or not
	 * 
	 * @return
	 * @author tttquyen
	 */
	public boolean isEmpty() {
		return bits.isEmpty();
	}

	  private void writeObject(ObjectOutputStream oos) throws IOException {
		  oos.defaultWriteObject();
		  oos.writeObject(this.vectorSize);
		  oos.writeObject(this.hash);
		  oos.writeObject(this.nbHash);
		  oos.writeObject(this.hashType);
	  }
	  
	  private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
		  ois.defaultReadObject();
		  this.vectorSize = (int) ois.readObject();
		  this.hash = (HashFunction) ois.readObject();
		  this.nbHash = (int) ois.readObject();
		  this.hashType = (int) ois.readObject();
	  }
	
}
