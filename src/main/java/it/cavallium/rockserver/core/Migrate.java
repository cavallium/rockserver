package it.cavallium.rockserver.core;

import it.cavallium.rockserver.core.common.api.ColumnSchema;
import it.cavallium.rockserver.core.common.api.RocksDB;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.bson.BasicBSONEncoder;
import org.bson.BasicBSONObject;
import org.bson.Document;

public class Migrate {

	public static void main(String[] args) throws TException, IOException {

		// Tunables
		String n = "401";
		var columnName = "peers_dtg-slave-" + n;
		var columnSchema = new ColumnSchema(List.of(Long.BYTES), List.of(), true);
		//

		var transport = new TFramedTransport(new TSocket("10.0.0.9", 5332));
		transport.open();

		var jo = Document.parse("{\"data\": " + Files.readString(Path.of("/tmp/export/" + columnName + ".json")) + "}");

		List<Document> documents = (List<Document>) jo.get("data");
		jo = null;
		System.gc();
		System.out.println("Read json file");
		var it = documents.iterator();
		List<Map.Entry<Long, BasicBSONObject>> documentMap = new ArrayList<>();
		while (it.hasNext()) {
			var document = it.next();
			var obj = new BasicBSONObject();
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
			documentMap.add(Map.entry(id, obj));
		}
		documents = null;
		System.gc();
		System.out.println("parsed documents");
		var protocol = new TBinaryProtocol(transport);
		var client = new RocksDB.Client(protocol);
		long columnId = client.createColumn(columnName, columnSchema);
		var encoder = new BasicBSONEncoder();
		long nn = 0;
		long total = documentMap.size();
		var initTime = Instant.now();
		for (Entry<Long, BasicBSONObject> longDocumentEntry : documentMap) {
			ByteBuffer bb = ByteBuffer.allocate(Long.BYTES);
			bb.asLongBuffer().put(longDocumentEntry.getKey());
			var valueArray = encoder.encode(longDocumentEntry.getValue());
			var valueBuf = ByteBuffer.wrap(valueArray);
			client.putFast(0, columnId, List.of(bb), valueBuf);
			if (nn > 0 && nn % 10_000 == 0) {
				var endTime = Instant.now();
				var dur = Duration.between(initTime, endTime);
				System.out.printf("Written %d/%d elements... %.2f, speed: %.2fHz%n", nn, total, ((nn * 100d) / total),  nn / (dur.toMillis() / 1000d));
			}
			nn++;
		}
		var endTime = Instant.now();
		var dur = Duration.between(initTime, endTime);
		if (total > 0) System.out.printf("Took %s, speed: %.2fHz%n", dur, total / (dur.toMillis() / 1000d));
	}
}
