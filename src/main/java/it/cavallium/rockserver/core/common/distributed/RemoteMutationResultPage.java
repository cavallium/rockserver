package it.cavallium.rockserver.core.common.distributed;

import java.util.*;

/** Copied encoded CommandResult rows bound to one observed prefix count and terminal digest. */
public record RemoteMutationResultPage(RemoteMutationStatus status,int offset,int nextOffset,List<byte[]> rows) {
    public static final int MAX_ITEMS=256,MAX_ROW_BYTES=CommandResult.MAX_BYTES;
    public RemoteMutationResultPage {
        Objects.requireNonNull(status);Objects.requireNonNull(rows);
        if(offset<0 || rows.size()>MAX_ITEMS || nextOffset!=Math.addExact(offset,rows.size()) || nextOffset>status.retainedResults())throw new IllegalArgumentException("Invalid result page cursor/count");
        var copied=new ArrayList<byte[]>(rows.size());long bytes=0;
        for(var row:rows){Objects.requireNonNull(row);bytes+=row.length;if(bytes>MAX_ROW_BYTES)throw new IllegalArgumentException("Result page exceeds encoded row bound");copied.add(row.clone());}
        rows=List.copyOf(copied);
    }
    @Override public List<byte[]> rows(){var copied=new ArrayList<byte[]>(rows.size());for(var row:rows)copied.add(row.clone());return List.copyOf(copied);}
    public boolean hasMore(){return nextOffset<status.retainedResults();}
}
