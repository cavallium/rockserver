package it.cavallium.rockserver.core.config;

import java.io.Serial;
import java.math.BigInteger;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import org.jetbrains.annotations.NotNull;

@SuppressWarnings("unused")
public final class DataSize extends Number implements Comparable<DataSize> {

	@Serial
	private static final long serialVersionUID = 7213411239846723568L;

	public static DataSize ZERO = new DataSize(0L);
	public static DataSize ONE = new DataSize(1L);
	public static DataSize KIB = new DataSize(1024L);
	public static DataSize KB = new DataSize(1000L);
	public static DataSize MIB = new DataSize(1024L * 1024);
	public static DataSize MB = new DataSize(1000L * 1000);
	public static DataSize GIB = new DataSize(1024L * 1024 * 1024);
	public static DataSize GB = new DataSize(1000L * 1000 * 1000);
	public static DataSize TIB = new DataSize(1024L * 1024 * 1024 * 1024);
	public static DataSize TB = new DataSize(1000L * 1000 * 1000 * 1000);
	public static DataSize PIB = new DataSize(1024L * 1024 * 1024 * 1024 * 1024);
	public static DataSize PB = new DataSize(1000L * 1000 * 1000 * 1000 * 1000);
	public static DataSize EIB = new DataSize(1024L * 1024 * 1024 * 1024 * 1024 * 1024);
	public static DataSize EB = new DataSize(1000L * 1000 * 1000 * 1000 * 1000 * 1000);
	public static DataSize MAX_VALUE = new DataSize(Long.MAX_VALUE);

	private final long size;

	public DataSize(long size) {
		this.size = size;
	}

	public DataSize(String size) {
		size = size.replaceAll("\\s|_", "");
		switch (size) {
			case "", "0", "-0", "+0" -> {
				this.size = 0;
				return;
			}
			case "∞", "inf", "infinite", "∞b" -> {
				this.size = Long.MAX_VALUE;
				return;
			}
		}
		int numberStartOffset = size.charAt(0) == '-' ? 1 : 0;
		int numberEndOffset = numberStartOffset;
		while (numberEndOffset < size.length() && Character.isDigit(size.charAt(numberEndOffset))) {
			numberEndOffset++;
		}
		if (numberEndOffset == numberStartOffset) {
			throw new IllegalArgumentException("No number found");
		}
		for (int i = numberEndOffset; i < size.length(); i++) {
			if (!Character.isLetter(size.charAt(i))) {
				throw new IllegalArgumentException("Unsupported character");
			}
		}

		try {
			BigInteger number = new BigInteger(size.substring(0, numberEndOffset));
			BigInteger scale = getScale(size.substring(numberEndOffset));
			this.size = number.multiply(scale).longValueExact();
		} catch (ArithmeticException | NumberFormatException ex) {
			throw new IllegalArgumentException("Data size is outside the supported 64-bit range", ex);
		}
	}

	private static BigInteger getScale(String unit) {
		if (unit.isEmpty()) {
			return BigInteger.ONE;
		}
		if (unit.equals("B")) {
			return BigInteger.ONE;
		}
		if (unit.endsWith("b")) {
			throw new IllegalArgumentException("Bits are not allowed");
		}
		boolean powerOf2;
		if (unit.length() == 2 && unit.charAt(1) == 'B') {
			powerOf2 = false;
		} else if (unit.length() == 3 && unit.charAt(1) == 'i' && unit.charAt(2) == 'B') {
			powerOf2 = true;
		} else {
			throw new IllegalArgumentException("Wrong measurement unit");
		}
		return getScale(powerOf2, unit.charAt(0));
	}

	private static BigInteger getScale(boolean powerOf2, char scaleChar) {
		int exponent = switch (scaleChar) {
			case 'K', 'k' -> 1;
			case 'M', 'm' -> 2;
			case 'G', 'g' -> 3;
			case 'T', 't' -> 4;
			case 'P', 'p' -> 5;
			case 'E', 'e' -> 6;
			case 'Z', 'z' -> 7;
			case 'Y', 'y' -> 8;
			default -> throw new IllegalArgumentException("Unexpected measurement unit: " + scaleChar);
		};
		return BigInteger.valueOf(powerOf2 ? 1024L : 1000L).pow(exponent);
	}

	public static Long get(DataSize value) {
		if (value == null) {
			return null;
		} else {
			return value.size;
		}
	}

	public static long getOrElse(DataSize value, @NotNull DataSize defaultValue) {
		if (value == null) {
			return defaultValue.size;
		} else {
			return value.size;
		}
	}


	@Override
	public int intValue() {
		if (size >= Integer.MAX_VALUE) {
			return Integer.MAX_VALUE;
		}
		if (size <= Integer.MIN_VALUE) {
			return Integer.MIN_VALUE;
		}
		return (int) size;
	}

	@Override
	public long longValue() {
		return size;
	}

	@Override
	public float floatValue() {
		return size;
	}

	@Override
	public double doubleValue() {
		return size;
	}

	@Override
	public String toString() {
		return toString(true);
	}

	public String toString(boolean precise) {
		boolean siUnits = size % 1000 == 0;
		int k = siUnits ? 1000 : 1024;
		long lSize = size;
		CharacterIterator ci = new StringCharacterIterator((siUnits ? "k" : "K") + "MGTPEZY");
		while ((precise ? lSize % k == 0 : lSize > k) && lSize != 0) {
			lSize /= k;
			ci.next();
		}
		if (lSize == size) {
			return lSize + "B";
		}
		return lSize + "" + ci.previous() + (siUnits ? "B" : "iB");
	}

	@Override
	public int compareTo(@NotNull DataSize anotherLong) {
		return Long.compare(this.size, anotherLong.size);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof DataSize) {
			return size == ((DataSize)obj).size;
		}
		return false;
	}

	@Override
	public int hashCode() {
		return Long.hashCode(size);
	}
}
