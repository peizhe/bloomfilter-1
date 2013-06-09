package org.araqne.bloomfilter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.araqne.bloomfilter.BloomFilterTest.UIDGen;
import org.araqne.bloomfilter.BloomFilterTest.VMIDGen;
import org.junit.Test;
import static org.junit.Assert.*;

public class BloomFilter2Test {
	private BloomFilter2<String> filter = null;
	private Set<String> map = null;

	// failed performance : 0 - 4, 5, 6, 7, 8, 2-3
	// success : 0-1, 2, 3, 1-2, 3, 2-1, 3-1
	@SuppressWarnings("unchecked")
	public void init(int num, boolean doMap) {
		filter = new BloomFilter2<String>(0.001, num, GeneralHashFunction.stringHashFunctions[2],
				GeneralHashFunction.stringHashFunctions[1]);
		map = new HashSet<String>(num);

		String in;
		for (int i = 0; i < num / 2; i++) {
			in = VMIDGen.next();
			filter.add(in);
			if (doMap)
				map.add(in);
		}
		for (int i = 0; i < num / 2; i++) {
			in = UIDGen.next();
			filter.add(in);
			if (doMap)
				map.add(in);
		}
	}

	@Test
	public void contains() {
		System.out.println(new Date());
		init(50000, true);
		for (String key : map) {
			if (map.contains(key) == false)
				fail();
		}
		System.out.println(new Date());
	}

	@Test
	public void doesNotContain() {
		long begin = System.currentTimeMillis();
		init(10000, false);
		int count = 0;
		for (int i = 0; i < 10000; i++) {
			if (filter.contains(VMIDGen.next()))
				count++;
		}
		for (int i = 0; i < 10000; i++) {
			if (filter.contains(UIDGen.next()))
				count++;
		}

		System.out.printf("false positive count: %d, rate : %f\n", count, count / 20000D);
		assertTrue(count < 20);
		long end = System.currentTimeMillis();
		System.out.println("not contains test elapsed " + (end - begin) + "ms");
	}

	@Test
	public void test() throws IOException {
		for (int d = 0; d < 143; d++) {
			BloomFilter<String> filter1 = new BloomFilter<String>(10);
			BloomFilter2<String> filter2 = new BloomFilter2<String>(10);

			filter1.getBitmap().set(d);
			filter2.getBitmap().set(d);

			ByteArrayOutputStream os1 = new ByteArrayOutputStream();
			filter1.save(os1);

			ByteArrayOutputStream os2 = new ByteArrayOutputStream();
			filter2.save(os2);

			byte[] b1 = os1.toByteArray();
			byte[] b2 = os2.toByteArray();

			// compare except header
			int min = Math.min(b1.length, b2.length);
			for (int i = 16; i < min; i++)
				assertEquals(b1[i], b2[i]);
		}
	}

	@Test
	public void testCompatibility() throws IOException {
		BloomFilter<String> filter1 = new BloomFilter<String>(10);
		filter1.add("test");
		ByteArrayOutputStream os1 = new ByteArrayOutputStream();
		filter1.save(os1);
		byte[] b = os1.toByteArray();

		BloomFilter2<String> filter2 = new BloomFilter2<String>();
		filter2.load(new ByteArrayInputStream(b));
		assertTrue(filter2.contains("test"));
	}
}
