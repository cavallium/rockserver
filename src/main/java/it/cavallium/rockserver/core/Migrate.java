package it.cavallium.rockserver.core;

import it.cavallium.rockserver.core.common.api.ColumnSchema;
import it.cavallium.rockserver.core.common.api.RocksDB;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.bson.BSONObject;
import org.bson.BasicBSONDecoder;
import org.bson.BasicBSONEncoder;
import org.bson.BasicBSONObject;
import org.bson.Document;

public class Migrate {

	public static void main(String[] args) throws TException, IOException, InterruptedException {

		// Tunables
		var columnName = args[0];
		var password = args[1];
		//

		System.out.println("Column: " + columnName);
		var temp = Files.createTempFile("temp-out-" + columnName + "-", ".json");

		boolean peerMode = columnName.startsWith("peers_");
		var columnSchema = peerMode ? new ColumnSchema(List.of(Long.BYTES), List.of(), true) : new ColumnSchema(List.of(Byte.BYTES), List.of(), true);
		var result = Runtime
				.getRuntime()
				.exec(new String[]{"psql", "--host", "home.cavallium.it", "-d", "sessions", "-U", "postgres", "-c",
						"SELECT json_agg(t) FROM (SELECT * FROM \"" + columnName + "\") t", "-qAtX",
						"--output=" + temp}, new String[] {"PGPASSWORD=" + password});
		result.waitFor();

		var jo = Document.parse("{\"data\": " + Files.readString(temp) + "}");
		Files.delete(temp);

		List<Document> documents = (List<Document>) jo.get("data");
		jo = null;
		System.gc();
		System.out.println("Read json file");
		var it = documents.iterator();
		List<Map.Entry<Object, BasicBSONObject>> documentMap = new ArrayList<>();
		while (it.hasNext()) {
			var document = it.next();
			var obj = new BasicBSONObject();
			if (peerMode) {
				var id = ((Number) document.get("id")).longValue();
				var accessHash = ((Number) document.get("access_hash")).longValue();
				var peerType = ((String) document.get("type"));
				var username = ((String) document.get("username"));
				var phoneNumber = ((String) document.get("phone_number"));
				var lastUpdateOn = ((Number) document.get("last_update_on")).longValue();
				obj.put("access_hash", accessHash);
				obj.put("peer_type", peerType);
				obj.put("username", username);
				obj.put("phone_number", phoneNumber);
				obj.put("last_update_on", lastUpdateOn);
				if (!peerType.equals("user")) {
					documentMap.add(Map.entry(id, obj));
				}
			} else {
				byte id = 0;
				Long dcId = ((Number) document.get("dc_id")).longValue();
				Long apiId = document.get("api_id") != null ? ((Number) document.get("api_id")).longValue() : null;
				var testMode = ((Boolean) document.get("test_mode"));
				var authKey = HexFormat.of().parseHex(((String) document.get("auth_key")).substring(2));
				var date = ((Number) document.get("date")).longValue();
				var userId = ((Number) document.get("user_id")).longValue();
				var isBot = ((Boolean) document.get("is_bot"));
				var phone = ((String) document.get("phone"));

				obj.put("dc_id", dcId);
				obj.put("api_id", apiId);
				obj.put("test_mode", testMode);
				obj.put("auth_key", authKey);
				obj.put("date", date);
				obj.put("user_id", userId);
				obj.put("is_bot", isBot);
				obj.put("phone", phone);
				documentMap.add(Map.entry(id, obj));
			}
		}
		documents = null;
		System.gc();
		System.out.println("parsed documents");
		var protocol = new TBinaryProtocol.Factory();
		var clients = ThreadLocal.withInitial(() -> {
			try {
				var socket = new TSocket("10.0.0.9", 5332);
				var transport = new TFramedTransport(socket);
				transport.open();

				return new RocksDB.Client(new TBinaryProtocol(transport));
			} catch (TTransportException e) {
				throw new RuntimeException(e);
			}
		});
		var columnId = clients.get().createColumn(columnName, columnSchema);
		var encoder = ThreadLocal.withInitial(BasicBSONEncoder::new);
		var keyBBLocal = ThreadLocal.withInitial(() -> peerMode ? ByteBuffer.allocate(Long.BYTES).order(ByteOrder.BIG_ENDIAN) : ByteBuffer.allocate(1).order(ByteOrder.BIG_ENDIAN));
		AtomicLong next = new AtomicLong();
		long total = documentMap.size();
		var initTime = Instant.now();
		documentMap.stream().parallel().forEach(longDocumentEntry -> {
			ByteBuffer bb = keyBBLocal.get();
			if (peerMode) {
				bb.asLongBuffer().put((Long) longDocumentEntry.getKey());
			} else {
				bb.put((Byte) longDocumentEntry.getKey()).flip();
			}
			var valueArray = encoder.get().encode(longDocumentEntry.getValue());
			var valueBuf = ByteBuffer.wrap(valueArray);
			try {
				clients.get().putFast(0, columnId, List.of(bb), valueBuf);
			} catch (TException e) {
				throw new RuntimeException(e);
			}
			var nn = next.incrementAndGet();
			if (nn > 0 && nn % 10_000 == 0) {
				var endTime = Instant.now();
				var dur = Duration.between(initTime, endTime);
				System.out.printf("Written %d/%d elements... %.2f, speed: %.2fHz%n", nn, total, ((nn * 100d) / total),  nn / (dur.toMillis() / 1000d));
			}
		});
		if (!peerMode) {
			System.out.println("Schema: " + new BasicBSONDecoder().readObject(clients.get().get(0, columnId, List.of(ByteBuffer.wrap(new byte[] {0}))).getValue()));
		}
		var endTime = Instant.now();
		var dur = Duration.between(initTime, endTime);
		if (total > 0) System.out.printf("Took %s, speed: %.2fHz%n", dur, total / (dur.toMillis() / 1000d));
	}
}
