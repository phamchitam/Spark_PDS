package test;

public class SomeThing {

	public static void main(String[] args) {
		long all_encode = 283622572346572800L;
//		long m = fp <<4;
//		System.out.println(m);
		
		long m =	 ((1L << 4) - 1)|((-1L) >>>(63 + 1) << (4 + 1)) |  
				((all_encode & ((1L<<(63 - 4 + 1)) - 1)) << 4);

		
		long t = ((1L) <<  64) - 1 ;
		System.out.println(t);
	}

}
