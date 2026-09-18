package it.cavallium.rockserver.core.impl.test;
import it.cavallium.rockserver.core.common.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
class CompactionFileLimitTest {
 @Test void acceptsLargeSelectionsAndRejectsExcess() {
  var files=java.util.stream.IntStream.range(0,65_536).mapToObj(i->i+".sst").toList();
  assertEquals(65_536,new SstMaintenance.Request(1,"session",files,6,0,8_000_000_000L,128_000_000_000L,1,false).files().size());
  var excess=new java.util.ArrayList<>(files);excess.add("65536.sst");
  assertThrows(RocksDBException.class,()->new SstMaintenance.Request(1,"session",excess,6,0,8_000_000_000L,128_000_000_000L,1,false));
 }
}
