package it.cavallium.rockserver.distributed.ratis;

import java.io.IOException;
import java.util.*;

/** Node aggregate admission; services also retain their own physical pool ceilings. */
public final class RatisSchemaReplicaAdmission {
    private final RatisSchemaReplicaService.Limits limits;private final RatisNodeResourceBudget graph;
    private final Map<String,Long> uploads=new HashMap<>();private long disk,heap;private int tasks;
    public RatisSchemaReplicaAdmission(RatisSchemaReplicaService.Limits limits,RatisNodeResourceBudget graph){this.limits=Objects.requireNonNull(limits);this.graph=Objects.requireNonNull(graph);}
    synchronized Heap heap(UUID pool,long bytes,int taskCount)throws IOException{if(bytes>limits.maximumHeapBytes()-heap || taskCount>limits.maximumTasks()-tasks)throw new IOException("Node schema heap/task admission exhausted");var resource=graph.reserve(pool,RatisNodeResourceBudget.Kind.PROTECTED,bytes,1);heap+=bytes;tasks+=taskCount;return new Heap(pool,bytes,taskCount,resource);}
    synchronized void disk(UUID pool,String name,long bytes)throws IOException{String key=pool+"/"+name;long previous=uploads.getOrDefault(key,0L);if(previous==bytes && uploads.containsKey(key))return;if(!uploads.containsKey(key) && uploads.size()>=limits.maximumUploads() || bytes-previous>limits.maximumDiskBytes()-disk)throw new IOException("Node schema upload/disk admission exhausted");uploads.put(key,bytes);disk+=bytes-previous;}
    synchronized void released(UUID pool,String name){var bytes=uploads.remove(pool+"/"+name);if(bytes!=null)disk-=bytes;}
    final class Heap implements AutoCloseable {final UUID pool;final List<RatisNodeResourceBudget.Lease> resources=new ArrayList<>();long bytes;int taskCount;boolean closed;Heap(UUID pool,long bytes,int count,RatisNodeResourceBudget.Lease resource){this.pool=pool;this.bytes=bytes;taskCount=count;resources.add(resource);}
        void resize(long wanted,int wantedTasks)throws IOException{synchronized(RatisSchemaReplicaAdmission.this){if(closed)throw new IllegalStateException("Schema heap owner closed");long delta=wanted-bytes;if(delta>limits.maximumHeapBytes()-heap || wantedTasks-taskCount>limits.maximumTasks()-tasks)throw new IOException("Node schema heap/task admission exhausted");if(delta>0)resources.add(graph.reserve(pool,RatisNodeResourceBudget.Kind.PROTECTED,delta,1));else if(delta<0){if(resources.size()!=1)throw new IllegalStateException("Cannot shrink expanded schema owner");resources.getFirst().shrinkTo(wanted,1);}heap+=delta;tasks+=wantedTasks-taskCount;bytes=wanted;taskCount=wantedTasks;}}
        @Override public void close(){synchronized(RatisSchemaReplicaAdmission.this){if(closed)return;closed=true;heap-=bytes;tasks-=taskCount;}resources.forEach(RatisNodeResourceBudget.Lease::close);}
    }
}
