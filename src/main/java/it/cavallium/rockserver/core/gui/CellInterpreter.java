package it.cavallium.rockserver.core.gui;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import org.bson.BsonBinaryReader;
import org.bson.BsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DecoderContext;


/**
 * An enum representing different ways to interpret and display byte[] data. Each interpreter provides a user-friendly
 * description and the logic for conversion.
 */
public enum CellInterpreter {
	// --- Standard Interpreters ---
	HEX_SUMMARY("Hex Summary", 0), TEXT_UTF8("Text (UTF-8)", 0), BSON("BSON Document", 0),

	// --- 32-bit Numeric Interpreters ---
	NUM_SIGNED_BE_32("Signed BE (int)", 4), NUM_UNSIGNED_BE_32("Unsigned BE (int)",
			4
	), NUM_SIGNED_LE_32("Signed LE (int)", 4), NUM_UNSIGNED_LE_32("Unsigned LE (int)", 4),

	// --- 64-bit Numeric Interpreters ---
	NUM_SIGNED_BE_64("Signed BE (long)", 8), NUM_UNSIGNED_BE_64("Unsigned BE (long)", 8), NUM_SIGNED_LE_64(
			"Signed LE (long)",
			8
	), NUM_UNSIGNED_LE_64("Unsigned LE (long)", 8),

	// --- 128-bit Numeric Interpreters ---
	NUM_UNSIGNED_BE_128("Unsigned BE (128-bit)", 16), NUM_UNSIGNED_LE_128("Unsigned LE (128-bit)", 16);


	private final String description;
	private final int requiredBytes;

	CellInterpreter(String description, int requiredBytes) {
		this.description = description;
		this.requiredBytes = requiredBytes;
	}

	/**
	 * Interprets the given byte array according to the enum constant's logic.
	 *
	 * @param data The byte array to interpret.
	 * @return A string representation of the data.
	 */
	public String interpret(byte[] data) {
		if (data == null) {
			return "null";
		}
		if (requiredBytes > 0 && data.length < requiredBytes) {
			return "<len " + data.length + " < " + requiredBytes + ">";
		}

		try {
			return switch (this) {
				case HEX_SUMMARY -> {
					if (data.length > 32) {
						yield HexFormat.of().formatHex(data, 0, 32) + "... (" + data.length + " bytes)";
					} else {
						yield HexFormat.of().formatHex(data);
					}
				}
				case TEXT_UTF8 -> new String(data, StandardCharsets.UTF_8);
				case BSON -> {
					var reader = new BsonBinaryReader(ByteBuffer.wrap(data));
					var codec = new BsonDocumentCodec();
					BsonDocument document = codec.decode(reader, DecoderContext.builder().build());
					yield document.toJson();
				}

				case NUM_SIGNED_BE_32 -> String.valueOf(ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN).getInt());
				case NUM_UNSIGNED_BE_32 -> Integer.toUnsignedString(ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN).getInt());
				case NUM_SIGNED_LE_32 -> String.valueOf(ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getInt());
				case NUM_UNSIGNED_LE_32 ->
						Integer.toUnsignedString(ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getInt());

				case NUM_SIGNED_BE_64 -> String.valueOf(ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN).getLong());
				case NUM_UNSIGNED_BE_64 -> Long.toUnsignedString(ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN).getLong());
				case NUM_SIGNED_LE_64 -> String.valueOf(ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getLong());
				case NUM_UNSIGNED_LE_64 ->
						Long.toUnsignedString(ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN).getLong());

				case NUM_UNSIGNED_BE_128 -> new BigInteger(1, data).toString();
				case NUM_UNSIGNED_LE_128 -> {
					byte[] beBytes = new byte[16];
					for (int i = 0; i < 16; i++) {
						beBytes[i] = data[15 - i];
					}
					yield new BigInteger(1, beBytes).toString();
				}
			};
		} catch (Exception e) {
			return "<error: " + e.getClass().getSimpleName() + ">";
		}
	}

	@Override
	public String toString() {
		return description;
	}
}