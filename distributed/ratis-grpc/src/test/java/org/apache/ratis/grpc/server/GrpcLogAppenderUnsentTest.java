package org.apache.ratis.grpc.server;

import java.io.InterruptedIOException;
import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.BiFunction;
import org.apache.ratis.metrics.Timekeeper;
import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.server.leader.*;
import org.apache.ratis.thirdparty.io.grpc.stub.*;
import org.apache.ratis.util.*;
import org.apache.ratis.util.function.CheckedRunnable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.*;

/** Real appendLog/sendRequest and transport ownership, with a blocked local stream call and no socket. */
class GrpcLogAppenderUnsentTest {
  static final class Stream extends CallStreamObserver<AppendEntriesRequestProto> {
    final CountDownLatch entered=new CountDownLatch(1),proceed=new CountDownLatch(1);
    final AtomicBoolean inside=new AtomicBoolean();
    volatile boolean block,ready;
    int messages;long firstIndex=-1;
    public boolean isReady(){inside.set(true);try{entered.countDown();if(block)try{assertTrue(proceed.await(5,TimeUnit.SECONDS));}catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new AssertionError(interrupted);}return ready;}finally{inside.set(false);}}
    public void onNext(AppendEntriesRequestProto request){messages++;firstIndex=request.getEntries(0).getIndex();}
    public void onError(Throwable failure){}
    public void onCompleted(){}
    public void setOnReadyHandler(Runnable handler){}
    public void disableAutoInboundFlowControl(){}
    public void request(int count){}
    public void setMessageCompression(boolean enabled){}
  }
  static final class Timeouts implements TimeoutExecutor {
    int count;
    public int getTaskCount(){return count;}
    public <T extends Throwable>void onTimeout(TimeDuration delay,CheckedRunnable<T> task,java.util.function.Consumer<T> failed){count++;}
  }

  @ParameterizedTest @ValueSource(booleans={false,true})
  void replacedStreamRetiresAfterCallReturnsAndReusesSameAppenderWithoutSkippingUnsentData(boolean resetAlreadyRemoved)throws Exception {
    try(var f=new GrpcLogAppenderWaitTest.Fixture()) {
      var stream=new Stream();stream.block=true;var observers=streams(f.appender,stream,TimeDuration.ONE_MILLISECOND);var timeouts=new Timeouts();set(GrpcLogAppender.class,f.appender,"scheduler",timeouts);
      f.appender.requests=GrpcLogAppenderUnsentTest::request;var released=new AtomicInteger();owner(f.appender,0,stream,released);
      var result=new AtomicReference<Throwable>();var thread=Thread.ofPlatform().start(()->invoke(f.appender,result));
      try {
        assertTrue(stream.entered.await(5,TimeUnit.SECONDS));assertEquals(6,f.next.get());
        var pending=pending(f.appender);var current=pending.remove(0,false);assertNotNull(current);pending.put(current);
        var timerStopped=new AtomicInteger();var context=(Timekeeper.Context)get(current,"timerContext");set(current.getClass(),current,"timerContext",(Timekeeper.Context)()->{timerStopped.incrementAndGet();return context.stop();});
        observers.stop();if(resetAlreadyRemoved){f.errors.updateErrorCount(true);set(GrpcLogAppender.class,f.appender,"appendLogRequestObserver",null);}
        assertEquals(0,released.get(),"Stop is not retirement while isReady still owns the frame");stream.proceed.countDown();thread.join(5000);assertFalse(thread.isAlive());assertNull(result.get());
        assertEquals(1,released.get());assertEquals(0,stream.messages);assertEquals(0,pending.logRequestsSize());assertEquals(1,timerStopped.get());assertEquals(5,f.next.get());assertEquals(0,f.sends.get());assertEquals(0,timeouts.count);assertNull(get(f.appender,"appendLogRequestObserver"));assertEquals(1,f.errors.getErrorCountToDelay(),"Exactly one failed transport must contribute backoff");assertEquals(0,f.errors.getErrorCountToDelay());
        var replacement=new Stream();replacement.ready=true;streams(f.appender,replacement,TimeDuration.ONE_MILLISECOND);
        invoke(f.appender,result);assertNull(result.get());assertEquals(1,replacement.messages);assertEquals(6,f.next.get());assertEquals(1,f.sends.get());assertEquals(1,timeouts.count);assertEquals(2,f.appender.getCallId(),"The same appender retries the original first index after replacement");
      }finally{stream.proceed.countDown();thread.join(5000);}
    }
  }

  @Test void realSleepInterruptionStillPropagatesAfterRetiringUnsentFrame()throws Exception {
    try(var f=new GrpcLogAppenderWaitTest.Fixture()) {
      var stream=new Stream();streams(f.appender,stream,TimeDuration.valueOf(1,TimeUnit.MINUTES));var timeouts=new Timeouts();set(GrpcLogAppender.class,f.appender,"scheduler",timeouts);
      f.appender.requests=GrpcLogAppenderUnsentTest::request;var released=new AtomicInteger();owner(f.appender,0,stream,released);
      var result=new AtomicReference<Throwable>();var interrupted=new AtomicBoolean();var thread=Thread.ofPlatform().start(()->{invoke(f.appender,result);interrupted.set(Thread.currentThread().isInterrupted());});
      assertTrue(stream.entered.await(5,TimeUnit.SECONDS));thread.interrupt();thread.join(5000);assertFalse(thread.isAlive());assertInstanceOf(InterruptedIOException.class,result.get());assertTrue(interrupted.get());assertEquals(1,released.get());assertEquals(0,stream.messages);assertEquals(0,pending(f.appender).logRequestsSize());assertEquals(5,f.next.get());assertEquals(0,f.sends.get());assertEquals(0,timeouts.count);
    }
  }

  @Test void terminalResponseBeforeBeginRejectsNormallyAndPreservesConfirmedMatch()throws Exception {
    try(var f=new GrpcLogAppenderWaitTest.Fixture()) {
      var stream=new Stream();var observers=streams(f.appender,stream,TimeDuration.ONE_MILLISECOND);var handler=get(observers,"appendHandler");((MigrationTransportOwners)get(handler,"migration")).terminal();
      var timeouts=new Timeouts();set(GrpcLogAppender.class,f.appender,"scheduler",timeouts);f.appender.requests=GrpcLogAppenderUnsentTest::request;var released=new AtomicInteger();owner(f.appender,0,stream,released);f.match.set(5);
      var result=new AtomicReference<Throwable>();invoke(f.appender,result);assertNull(result.get());assertEquals(1,released.get());assertEquals(1,stream.entered.getCount(),"A terminal transport must never enter its frame");assertEquals(0,pending(f.appender).logRequestsSize());assertEquals(6,f.next.get(),"A separately confirmed entry must not be rewound");assertEquals(0,f.sends.get());assertEquals(0,timeouts.count);
    }
  }

  @ParameterizedTest @ValueSource(booleans={false,true})
  void responseTerminalDuringConstructionRemovesDeadObserverAndReconnectsSameAppender(boolean leased)throws Exception {
    try(var f=new GrpcLogAppenderWaitTest.Fixture()) {
      var first=new Stream();var fresh=new Stream();fresh.ready=true;var openings=new AtomicInteger();
      f.appender.streamFactory=()->{int attempt=openings.incrementAndGet();try{return newStreams(f.appender,attempt==1?first:fresh,TimeDuration.ONE_MILLISECOND,attempt==1);}catch(Exception failure){throw new CompletionException(failure);}};
      f.appender.requests=GrpcLogAppenderUnsentTest::request;var timeouts=new Timeouts();set(GrpcLogAppender.class,f.appender,"scheduler",timeouts);var released=new AtomicInteger();if(leased)owner(f.appender,0,first,released);
      var result=new AtomicReference<Throwable>();invoke(f.appender,result);
      assertNull(result.get());assertEquals(1,openings.get());assertNull(get(f.appender,"appendLogRequestObserver"));assertEquals(1,first.entered.getCount());assertEquals(leased?1:0,released.get());assertEquals(5,f.next.get());assertEquals(0,pending(f.appender).logRequestsSize());assertEquals(0,f.sends.get());assertEquals(0,timeouts.count);assertEquals(1,f.errors.getErrorCountToDelay());
      invoke(f.appender,result);
      assertNull(result.get());assertEquals(2,openings.get(),"The same appender must construct a fresh stream after early rejection");assertEquals(1,fresh.messages);assertEquals(5,fresh.firstIndex);assertEquals(6,f.next.get());assertEquals(1,f.sends.get());assertEquals(1,timeouts.count);assertEquals(2,f.appender.getCallId());
    }
  }

  private static AppendEntriesRequestProto request(long call){return AppendEntriesRequestProto.newBuilder().setServerRequest(RaftRpcRequestProto.newBuilder().setCallId(call)).setPreviousLog(org.apache.ratis.server.protocol.TermIndex.valueOf(1,4).toProto()).addEntries(LogEntryProto.newBuilder().setTerm(1).setIndex(5).setMetadataEntry(MetadataProto.newBuilder().setCommitIndex(4))).build();}
  private static GrpcLogAppender.StreamObservers streams(GrpcLogAppender appender,Stream stream,TimeDuration wait)throws Exception {
    var observers=newStreams(appender,stream,wait,false);set(GrpcLogAppender.class,appender,"appendLogRequestObserver",observers);return observers;
  }
  private static GrpcLogAppender.StreamObservers newStreams(GrpcLogAppender appender,Stream stream,TimeDuration wait,boolean terminalBeforePublication)throws Exception {
    var handlerType=Class.forName(GrpcLogAppender.class.getName()+"$AppendLogResponseHandler");var constructor=handlerType.getDeclaredConstructor(GrpcLogAppender.class);constructor.setAccessible(true);var handler=constructor.newInstance(appender);
    BiFunction<StreamObserver<AppendEntriesReplyProto>,Boolean,CallStreamObserver<AppendEntriesRequestProto>> open=(reply,heartbeat)->{if(terminalBeforePublication)reply.onError(new java.io.IOException("Terminal response during stream construction"));return stream;};
    var build=GrpcLogAppender.StreamObservers.class.getDeclaredConstructors()[0];build.setAccessible(true);return (GrpcLogAppender.StreamObservers)build.newInstance(open,handler,null,false,wait);
  }
  @SuppressWarnings("unchecked")private static void owner(GrpcLogAppender appender,long id,Stream stream,AtomicInteger released)throws Exception {
    var field=LogAppenderBase.class.getDeclaredField("migrationOwners");field.setAccessible(true);((Map<Long,MigrationLogStateMachine.Lease>)field.get(appender)).put(id,new MigrationLogStateMachine.Lease(){public void materialized(long bytes){}public void close(){assertFalse(stream.inside.get());assertEquals(1,released.incrementAndGet());}});
  }
  private static GrpcLogAppender.RequestMap pending(GrpcLogAppender appender)throws Exception{return (GrpcLogAppender.RequestMap)get(appender,"pendingRequests");}
  private static void invoke(GrpcLogAppender appender,AtomicReference<Throwable> result){try{var method=GrpcLogAppender.class.getDeclaredMethod("appendLog",boolean.class);method.setAccessible(true);method.invoke(appender,false);}catch(InvocationTargetException failure){result.set(failure.getCause());}catch(Throwable failure){result.set(failure);}}
  private static Object get(Object owner,String name)throws Exception{var field=(owner instanceof GrpcLogAppender?GrpcLogAppender.class:owner.getClass()).getDeclaredField(name);field.setAccessible(true);return field.get(owner);}
  private static void set(Class<?> type,Object owner,String name,Object value)throws Exception{var field=type.getDeclaredField(name);field.setAccessible(true);field.set(owner,value);}
}
