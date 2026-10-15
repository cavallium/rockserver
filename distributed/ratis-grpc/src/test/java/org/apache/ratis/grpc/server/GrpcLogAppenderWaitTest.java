package org.apache.ratis.grpc.server;

import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.*;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.*;
import org.apache.ratis.server.leader.*;
import org.apache.ratis.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/** Actual appender scheduling with controlled clocks; transport is constructed but never started or bound. */
class GrpcLogAppenderWaitTest {
  private static final class Appender extends GrpcLogAppender {
    boolean data;
    long heartbeatWait=100;
    Appender(RaftServer.Division division,FollowerInfo follower){super(division,null,follower);}
    @Override public boolean hasAppendEntries(){return data;}
    @Override public long getHeartbeatWaitTimeMs(){return heartbeatWait;}
    @Override protected TimeDuration getRemainingWaitTime(){return TimeDuration.ZERO;}
    void clearHeartbeat(){resetHeartbeatTrigger();}
  }

  @Test void absentIdleStreamWaitsForHeartbeatButDataAndExplicitHeartbeatWakeImmediately()throws Exception {
    var properties=new RaftProperties();GrpcConfigKeys.Server.setPort(properties,0);
    GrpcConfigKeys.Server.setBossGroupSize(properties,1);GrpcConfigKeys.Server.setWorkerGroupSize(properties,1);
    GrpcConfigKeys.Server.setAsyncRequestThreadPoolSize(properties,1);
    var leader=RaftPeerId.valueOf("idle-leader");var followerId=RaftPeerId.valueOf("idle-follower");var rpc=new AtomicReference<GrpcServicesImpl>();
    var server=proxy(RaftServer.class,(method,args)->switch(method){
      case "getProperties"->properties;case "getId"->leader;case "getServerRpc"->rpc.get();case "getRpcType"->SupportedRpcType.GRPC;
      case "getGroups","getGroupIds"->List.of();case "getPeer"->RaftPeer.newBuilder().setId(leader).setAddress("unused:1").build();
      case "toString"->"unstarted-idle-server";default->throw new AssertionError(method);
    });
    var member=RaftGroupMemberId.valueOf(leader,RaftGroupId.randomId());
    var divisionProperties=new DivisionProperties(){
      public TimeDuration minRpcTimeout(){return TimeDuration.valueOf(400,TimeUnit.MILLISECONDS);}
      public TimeDuration maxRpcTimeout(){return TimeDuration.valueOf(800,TimeUnit.MILLISECONDS);}
      public TimeDuration rpcSleepTime(){return TimeDuration.ONE_MILLISECOND;}
      public TimeDuration rpcSlownessTimeout(){return TimeDuration.valueOf(1,TimeUnit.MINUTES);}
    };
    var division=proxy(RaftServer.Division.class,(method,args)->switch(method){
      case "getRaftServer"->server;case "getMemberId"->member;case "getId"->leader;case "properties"->divisionProperties;case "getThreadGroup"->Thread.currentThread().getThreadGroup();
      case "toString"->member.toString();default->throw new AssertionError(method);
    });
    var follower=proxy(FollowerInfo.class,(method,args)->switch(method){
      case "getName"->"idle-follower";case "getId"->followerId;case "getLastRpcResponseTime"->Timestamp.currentTime();
      case "toString"->"idle-follower";default->throw new AssertionError(method);
    });
    // No server.start or appender.start: the test owns no listening endpoint or RPC.
    try(var transport=GrpcServicesImpl.newBuilder().setServer(server).setCustomizer(GrpcServices.Customizer.getDefaultInstance()).build()) {
      rpc.set(transport);var appender=new Appender(division,follower);
      try {
        assertTrue(appender.shouldSendAppendEntries(),"An absent stream still requests an initial reconnect attempt");
        for(int retry=0;retry<1000;retry++)assertEquals(100,appender.getWaitTimeMs(),"No idle zero-delay retry after an unconstructible request");
        assertEquals(0,appender.getCallId(),"Scheduling does not construct or send another RPC");
        appender.data=true;assertEquals(0,appender.getWaitTimeMs(),"New replicated data remains prompt");
        appender.data=false;appender.triggerHeartbeat();assertEquals(0,appender.getWaitTimeMs(),"Explicit read-index heartbeat remains prompt");
        appender.clearHeartbeat();assertEquals(100,appender.getWaitTimeMs());
        appender.heartbeatWait=0;assertEquals(0,appender.getWaitTimeMs(),"A due heartbeat must reconnect immediately");
      }finally{appender.stopAsync().get(5,TimeUnit.SECONDS);}
    }
  }

  @FunctionalInterface private interface Invocation{Object call(String method,Object[] arguments)throws Throwable;}
  private static <T>T proxy(Class<T> type,Invocation call){return type.cast(Proxy.newProxyInstance(type.getClassLoader(),new Class<?>[]{type},(proxy,method,args)->call.call(method.getName(),args)));}
}
