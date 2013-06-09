/*
 * Copyright 2013 Eediom Inc.
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

import java.nio.ByteBuffer;

public class Bitmap {
	private int bits;
	private ByteBuffer bb;

	public Bitmap(int bits) {
		this.bits = bits;
		int size = bits / 64 * 8;
		if (bits % 64 > 0)
			size += 8;

		this.bb = ByteBuffer.allocate(size);
	}

	public Bitmap(int bits, ByteBuffer bb) {
		this.bits = bits;
		this.bb = bb;
	}

	public ByteBuffer getBytes() {
		return bb;
	}

	public int length() {
		return bb.capacity() * 8;
	}

	public int getByteLength() {
		return bb.capacity();
	}

	public boolean get(int index) {
		int l = index >> 6;
		int p = (l << 3) + (7 - ((index >> 3) & 7));
		int m = index & 0x7;
		byte mask = (byte) (1 << m);
		return (bb.get(p) & mask) != 0;
	}

	public void set(int index) {
		if (index < 0)
			throw new IndexOutOfBoundsException("bitIndex < 0: " + index);

		if (index >= bits)
			throw new IndexOutOfBoundsException("bitIndex > max bits: " + index);

		int l = index >> 6;
		int p = (l << 3) + (7 - ((index >> 3) & 7));
		int m = index & 0x7;
		byte mask = (byte) (1 << m);
		bb.put(p, (byte) (bb.get(p) | mask));
	}
}
