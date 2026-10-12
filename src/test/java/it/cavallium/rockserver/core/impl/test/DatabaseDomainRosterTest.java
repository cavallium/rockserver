package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class DatabaseDomainRosterTest {
    static UUID id(int n) { return new UUID(0,n); }
    @Test void canonicalRosterGrowsWithoutReusingOrRemovingIssuedIdentity() {
        var domains=new ArrayList<>(List.of(new UUID(-1,0),id(3))); var initial=DatabaseDomainRoster.initial(id(1),id(2),domains); domains.clear(); assertEquals(id(3),initial.domainIds().getFirst()); assertEquals(initial,DatabaseDomainRoster.decode(initial.encode())); assertThrows(UnsupportedOperationException.class,()->initial.domainIds().clear());
        var next=initial.add(List.of(id(4),id(5))); next.requireSuccessorOf(initial); assertEquals(1,next.generation()); assertTrue(next.domainIds().containsAll(initial.domainIds())); assertThrows(IllegalArgumentException.class,()->next.add(List.of(id(3)))); assertThrows(IllegalArgumentException.class,()->initial.add(List.of(id(4),id(4))));
        assertThrows(IllegalArgumentException.class,()->new DatabaseDomainRoster(id(1),id(2),1,List.of(id(4),id(5))).requireSuccessorOf(initial)); assertThrows(IllegalArgumentException.class,()->new DatabaseDomainRoster(id(99),id(2),1,next.domainIds()).requireSuccessorOf(initial)); assertThrows(IllegalArgumentException.class,()->new DatabaseDomainRoster(id(1),id(2),0,next.domainIds()).requireSuccessorOf(initial)); assertThrows(IllegalArgumentException.class,()->initial.requireSuccessorOf(initial));
    }
    @Test void boundedCurrentCodecRejectsMalformedCountOrderNamespaceAndGeneration() {
        var domains=new ArrayList<UUID>(); for(int i=0;i<64;i++) domains.add(id(100+i)); var maximum=DatabaseDomainRoster.initial(id(1),id(2),domains); assertEquals(DatabaseDomainRoster.MAX_BYTES,maximum.encode().length); assertEquals(maximum,DatabaseDomainRoster.decode(maximum.encode())); assertThrows(IllegalArgumentException.class,()->maximum.add(List.of(id(999))));
        for(int n=0;n<maximum.encode().length;n++) { var bytes=Arrays.copyOf(maximum.encode(),n); assertThrows(IllegalArgumentException.class,()->DatabaseDomainRoster.decode(bytes)); }
        var count=maximum.encode(); ByteBuffer.wrap(count).putInt(40,Integer.MAX_VALUE); assertThrows(IllegalArgumentException.class,()->DatabaseDomainRoster.decode(count)); var generation=maximum.encode(); ByteBuffer.wrap(generation).putLong(32,-1); assertThrows(IllegalArgumentException.class,()->DatabaseDomainRoster.decode(generation));
        var order=maximum.encode(); System.arraycopy(order,44,order,60,16); assertThrows(IllegalArgumentException.class,()->DatabaseDomainRoster.decode(order)); assertThrows(IllegalArgumentException.class,()->new DatabaseDomainRoster(id(1),id(2),Long.MAX_VALUE,List.of(id(3))).add(List.of(id(4))));
    }
}
