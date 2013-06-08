/*
 * Copyright 2010 NCHOVY, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.araqne.bloomfilter;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.LongBuffer;
import java.util.BitSet;

public class BloomFilter<T> {
	private int numOfBits;
	private int numOfHashFunction;
	private final HashFunction<T> firstFunction;
	private final HashFunction<T> secondFunction;
	private BitSet bitmap;

	@SuppressWarnings("unchecked")
	public BloomFilter() {
		this(GeneralHashFunction.stringHashFunctions[2], GeneralHashFunction.stringHashFunctions[1]);
	}

	@SuppressWarnings("unchecked")
	public BloomFilter(long capacity) {
		this(0.001, capacity, GeneralHashFunction.stringHashFunctions[2], GeneralHashFunction.stringHashFunctions[1]);
	}

	public BloomFilter(HashFunction<T> first, HashFunction<T> second) {
		this(0.001, 1000000L, first, second);
	}

	public BloomFilter(double errorRate, long capacity, HashFunction<T> first, HashFunction<T> second) {
		OptimumFinder opt = new OptimumFinder(errorRate, capacity);
		this.firstFunction = first;
		this.secondFunction = second;
		attach(new BitSet(opt.numOfBits), opt.numOfBits, opt.numOfHashFunction);
	}

	public BloomFilter(double errorRate, long capacity, HashFunction<T> first, HashFunction<T> second, BitSet bitmap) {
		OptimumFinder opt = new OptimumFinder(errorRate, capacity);
		this.firstFunction = first;
		this.secondFunction = second;
		attach(new BitSet(opt.numOfBits), opt.numOfBits, opt.numOfHashFunction);
	}

	public BloomFilter(double errorRate, int capacity) {
		this(errorRate, capacity, GeneralHashFunction.stringHashFunctions[2], GeneralHashFunction.stringHashFunctions[1]);
	}

	public HashValue<T> getHashValue(T key) {
		return new HashValue<T>(key, firstFunction, secondFunction);
	}

	public void add(HashValue<T> v) {
		for (int i = 0; i < numOfHashFunction; i++) {
			int index = getIndex(v.getFirstHashCode(), v.getSecondHashCode(), i);
			this.bitmap.set(index);
		}
	}

	public void add(T key) {
		HashValue<T> v = new HashValue<T>(key, firstFunction, secondFunction);

		add(v);
	}

	public boolean contains(HashValue<T> v) {
		for (int i = 0; i < numOfHashFunction; i++) {
			int index = getIndex(v.getFirstHashCode(), v.getSecondHashCode(), i);
			if (this.bitmap.get(index) == false)
				return false;
		}
		return true;
	}

	public boolean contains(T key) {
		HashValue<T> v = new HashValue<T>(key, firstFunction, secondFunction);

		return contains(v);
	}

	public BitSet getBitmap() {
		return bitmap;
	}

	public void load(InputStream is) throws IOException {
		load(is, false);
	}

	public void load(InputStream is, boolean noaccel) throws IOException {
		DataInputStream dis = new DataInputStream(is);
		int length = dis.readInt();

		if (length < 0) {
			// length field means version
			int version = -length;
			if (version == 2) {
				int numOfhashFunc = dis.readInt();
				int numOfBits = dis.readInt();
				int streamLength = dis.readInt();
				// support 7 only

				LongBuffer buf = LongBuffer.allocate(streamLength / 64 + 1);
				try {
					while (true) {
						long l = dis.readLong();
						buf.put(l);
					}
				} catch (EOFException eof) {
				}

				buf.flip();
				if (!noaccel)
					try {
						this.attach(BitSet.valueOf(buf), numOfBits, numOfhashFunc);
						return;
					} catch (NoSuchMethodError e) {
					}

				// JRE 6 support
				BitSet set = new BitSet(numOfBits);
				int p = 0;
				for (int s = 0; s < buf.limit(); ++s) {
					long l = Long.reverse(buf.get());
					for (int i = 63; i >= 0; i--) {
						if (p >= streamLength)
							break;

						set.set(p, ((l >> i) & 1) == 1);
						p++;
					}
				}
				this.attach(set, numOfBits, numOfhashFunc);
				return;

			} else {
				throw new IllegalArgumentException("unsupported version: " + version);
			}
		} else {
			// version 1 load
			// try jdk 7 acceleration
			LongBuffer buf = LongBuffer.allocate(length / 64 + 1);
			try {
				while (true) {
					long l = dis.readLong();
					buf.put(l);
				}
			} catch (EOFException eof) {
			}
			buf.flip();

			if (!noaccel) {
				try {
					this.bitmap = BitSet.valueOf(buf);
					return;
				} catch (NoSuchMethodError e) {
				}
			}

			BitSet set = new BitSet(this.numOfBits);
			int p = 0;
			for (int s = 0; s < buf.limit(); ++s) {
				long l = Long.reverse(buf.get());
				for (int i = 63; i >= 0; i--) {
					if (p >= length)
						break;

					set.set(p, ((l >> i) & 1) == 1);
					p++;
				}
			}
			this.bitmap = set;
		}
	}

	private void attach(BitSet bm, int numOfBits, int numOfHash) {
		this.bitmap = bm;
		this.numOfBits = numOfBits;
		this.numOfHashFunction = numOfHash;
	}

	public long streamLength() {
		return streamLength(false);
	}

	public long streamLength(boolean noaccel) {
		if (!noaccel) {
			try {
				long[] words = bitmap.toLongArray();
				return words.length * 8 + getStreamHeaderLength();
			} catch (NoSuchMethodError e) {
			}
		}

		int count = 0;
		long wrote = getStreamHeaderLength();
		for (int i = 0; i < bitmap.length(); i++) {
			if (count++ == 63) {
				wrote += 8;
				count = 0;
			}
		}

		if (bitmap.length() % 64 != 0) {
			wrote += 8;
		}
		return wrote;
	}

	private int getStreamHeaderLength() {
		return 4 * 4;
	}

	public long save(OutputStream os) throws IOException {
		return save(os, false);
	}

	public long save(OutputStream os, boolean noaccel) throws IOException {
		DataOutputStream dos = new DataOutputStream(os);
		dos.writeInt(-2); // version
		dos.writeInt(numOfHashFunction);
		dos.writeInt(numOfBits);
		dos.writeInt(bitmap.length());

		// try jdk 7 accelration first
		if (!noaccel) {
			try {
				long[] words = bitmap.toLongArray();
				for (long word : words) {
					dos.writeLong(word);
				}
				return words.length * 8 + getStreamHeaderLength();
			} catch (NoSuchMethodError e) {
			}
		}

		int count = 0;

		long l = 0;
		long wrote = getStreamHeaderLength(); // header length (version, hash func count, length)
		for (int i = 0; i < bitmap.length(); i++) {
			l <<= 1;
			l |= bitmap.get(i) ? 1 : 0;

			if (count++ == 63) {
				dos.writeLong(Long.reverse(l));
				wrote += 8;
				l = 0;
				count = 0;
			}
		}

		if (bitmap.length() % 64 != 0) {
			l <<= 64 - count;
			dos.writeLong(Long.reverse(l));
			wrote += 8;
		}
		return wrote;
	}

	@Override
	public String toString() {
		return String.format("BloomFilter-[%d KB, %d hashFunctions (%s, %s)]", this.numOfBits / 8 / 1024, this.numOfHashFunction,
				this.firstFunction.toString(), this.secondFunction.toString());
	}

	private int getIndex(int firstHashCode, int secondHashCode, int i) {
		int index = (firstHashCode + (i * secondHashCode)) % this.numOfBits;
		return (index < 0) ? -index : index;
	}

	private static class OptimumFinder {
		private int numOfBits;
		private int numOfHashFunction;

		private OptimumFinder(double errorRate, long capacity) {
			numOfBits = Integer.MAX_VALUE;
			numOfHashFunction = 1;
			int m = 0;

			for (int k = 1; k < 20; k++) {
				m = (int) (k * capacity * -1.0 / java.lang.Math.log(1.0 - java.lang.Math.pow(errorRate, (1.0 / k))));

				if (m < numOfBits) {
					numOfBits = m;
					numOfHashFunction = k;
				}
			}

			assert numOfBits > capacity;
			assert numOfHashFunction > 1;
		}
	}

	public int getHashFuncCount() {
		return numOfHashFunction;
	}

}
