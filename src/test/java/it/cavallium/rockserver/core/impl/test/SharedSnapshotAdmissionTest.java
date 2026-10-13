package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.impl.rocksdb.PoolSnapshotAdmission;
import java.lang.reflect.*;
import java.util.function.LongSupplier;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class SharedSnapshotAdmissionTest {
    private static Object admission(PoolSnapshotAdmission.Limits limits,LongSupplier estimate,LongSupplier clock,PoolSnapshotAdmission.Shared shared)throws Exception {
        var source=Class.forName(PoolSnapshotAdmission.class.getName()+"$Source");var sample=Class.forName(PoolSnapshotAdmission.class.getName()+"$Sample").getDeclaredConstructor(long.class,long.class,long.class);sample.setAccessible(true);
        var callback=Proxy.newProxyInstance(source.getClassLoader(),new Class<?>[]{source},(p,m,a)->sample.newInstance(estimate.getAsLong(),1024L,clock.getAsLong()));var constructor=PoolSnapshotAdmission.class.getDeclaredConstructor(PoolSnapshotAdmission.Limits.class,source,LongSupplier.class,PoolSnapshotAdmission.Shared.class);constructor.setAccessible(true);return constructor.newInstance(limits,callback,clock,shared);
    }
    private static AutoCloseable reserve(Object admission)throws Exception{return (AutoCloseable)invoke(admission,"reserve");}
    private static void check(Object lease)throws Exception{invoke(lease,"checkUse");}
    private static Object invoke(Object target,String name)throws Exception{var method=target.getClass().getDeclaredMethod(name);method.setAccessible(true);try{return method.invoke(target);}catch(InvocationTargetException failure){if(failure.getCause() instanceof Exception cause)throw cause;throw (Error)failure.getCause();}}

    @Test void growthFromBothPoolsIsChargedUntilActualRelease()throws Exception {
        var time=new AtomicLong();var a=new AtomicLong(64);var b=new AtomicLong(64);
        var shared=new PoolSnapshotAdmission.Shared(new PoolSnapshotAdmission.Limits(4,256,Duration.ofSeconds(60),0),time::get);
        var local=new PoolSnapshotAdmission.Limits(4,512,Duration.ofSeconds(60),0);
        var first=admission(local,a::get,time::get,shared);
        var second=admission(local,b::get,time::get,shared);
        var one=reserve(first);var two=reserve(second);assertEquals(128,shared.stats().estimatedBytes());
        a.set(164);check(one);assertEquals(228,shared.stats().estimatedBytes());b.set(164);
        assertThrows(RuntimeException.class,()->check(two));assertEquals(328,shared.stats().estimatedBytes());assertThrows(RuntimeException.class,()->reserve(first));
        two.close();assertEquals(164,shared.stats().estimatedBytes());check(one);one.close();assertEquals(0,shared.stats().estimatedBytes());assertEquals(0,shared.stats().pins());
    }
    @Test void countAndAgeApplyAcrossPoolsAndLocalFailureRefundsGlobalReservation()throws Exception {
        var time=new AtomicLong();var shared=new PoolSnapshotAdmission.Shared(new PoolSnapshotAdmission.Limits(1,256,Duration.ofSeconds(1),0),time::get);
        var local=new PoolSnapshotAdmission.Limits(4,512,Duration.ofSeconds(60),0);
        var first=admission(local,()->64,time::get,shared);
        var second=admission(local,()->64,time::get,shared);
        var lease=reserve(first);assertThrows(RuntimeException.class,()->reserve(second));time.set(Duration.ofSeconds(2).toNanos());assertThrows(RuntimeException.class,()->check(lease));lease.close();
        var denied=admission(new PoolSnapshotAdmission.Limits(1,32,Duration.ofSeconds(60),0),()->64,time::get,shared);
        assertThrows(RuntimeException.class,()->reserve(denied));assertEquals(0,shared.stats().pins());try(var accepted=reserve(second)){assertEquals(1,shared.stats().pins());}
    }
}
