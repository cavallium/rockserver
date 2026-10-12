package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.util.*;
import java.io.*;
import java.nio.ByteBuffer;

/** Exact HOME reservation/promotion preparation; echoed command bytes are not caller-supplied authority. */
public final class RatisOperationRerouteAdmissionRpc {
    public static final int PREPARE=96,CANCEL=97,MAX_BYTES=ScopedTransactionRpc.HEADER_BYTES+2*DistributedCommandCodec.MAX_ENCODED_BYTES+2*RatisCatalogue.MAX_DIRECTORY_BYTES+RatisTopologyOperationOutcomeRpc.MAX_REPLY_BYTES+(int)RatisTransactionPlan.MAX_BYTES+64*40+64;
    private RatisOperationRerouteAdmissionRpc() {}
    public static boolean kind(int kind){return kind==PREPARE || kind==CANCEL;}
    public record Evidence(DistributedCommand original,RangeDirectory sourceDirectory,RangeDirectory targetDirectory,RatisTopologyOperationOutcomeRpc.Reply absence,List<RatisDurableTransaction.Intent> intents) {
        public Evidence { Objects.requireNonNull(original);Objects.requireNonNull(sourceDirectory);Objects.requireNonNull(targetDirectory);Objects.requireNonNull(absence);intents=intents.stream().sorted(Comparator.comparing(intent->intent.route().groupId())).toList();if(intents.size()>TransactionStatus.MAX_PARTICIPANTS)throw new IllegalArgumentException("Too many accepted reroute intents"); }
        public byte[] encode() {
            try { var bytes=new ByteArrayOutputStream();var out=new DataOutputStream(bytes);
                put(out,DistributedCommandCodec.encode(original));put(out,RangeDirectoryCodec.encode(sourceDirectory));put(out,RangeDirectoryCodec.encode(targetDirectory));put(out,RatisTopologyOperationOutcomeRpc.reply(absence));out.writeInt(intents.size());long aggregate=0;
                for(var intent:intents){uuid(out,intent.route().groupId());if(intent.captureId()==null)throw new IllegalArgumentException("Reroute promotion requires original capture");uuid(out,intent.captureId());byte[] command=DistributedCommandCodec.encode(intent.acceptedBusiness());aggregate+=command.length;if(aggregate>RatisTransactionPlan.MAX_BYTES)throw new IllegalArgumentException("Accepted reroute exceeds budget");put(out,command);}
                out.flush();if(bytes.size()>MAX_BYTES-16)throw new IllegalArgumentException("Reroute evidence exceeds budget");return bytes.toByteArray();
            }catch(IOException impossible){throw new AssertionError(impossible);}
        }
        /** Decode the forced reservation evidence; accepted intents require the bound HOME origin. */
        public static Evidence decode(byte[] bytes){return decode(bytes,null);}
        private static Evidence decode(byte[] bytes,OperationRerouteOrigin origin) {
            if(bytes.length>MAX_BYTES-16)throw new IllegalArgumentException("Reroute evidence exceeds budget");
            try {var in=new DataInputStream(new ByteArrayInputStream(bytes));var original=DistributedCommandCodec.decode(blob(in,DistributedCommandCodec.MAX_ENCODED_BYTES));var source=RangeDirectoryCodec.decode(blob(in,RatisCatalogue.MAX_DIRECTORY_BYTES));var target=RangeDirectoryCodec.decode(blob(in,RatisCatalogue.MAX_DIRECTORY_BYTES));
                var request=new RatisTopologyOperationOutcomeRpc.Request(source.clusterId(),source.databaseId(),source.domainId(),lineage(source,original),original.operationId(),original.receiptDigest());var absence=RatisTopologyOperationOutcomeRpc.decodeReply(blob(in,RatisTopologyOperationOutcomeRpc.MAX_REPLY_BYTES),request);
                int count=in.readInt();if(count<0 || count>TransactionStatus.MAX_PARTICIPANTS || count>0 && origin==null)throw new IllegalArgumentException("Invalid accepted reroute count");var captures=new HashMap<UUID,UUID>();var accepted=new HashMap<UUID,DistributedCommand>();long aggregate=0;
                for(int i=0;i<count;i++){UUID group=uuid(in),capture=uuid(in);byte[] command=blob(in,DistributedCommandCodec.MAX_ENCODED_BYTES);aggregate+=command.length;if(aggregate>RatisTransactionPlan.MAX_BYTES || captures.putIfAbsent(group,capture)!=null)throw new IllegalArgumentException("Duplicate/oversized accepted reroute");accepted.put(group,DistributedCommandCodec.decode(command));}
                if(in.available()!=0)throw new IllegalArgumentException("Trailing reroute evidence");
                if(count==0)return new Evidence(original,source,target,absence,List.of());
                var plan=RatisTransactionPlan.fromOriginal(target,original);var intents=new ArrayList<RatisDurableTransaction.Intent>();int n=0;var first=origin.firstControl();
                // Controls are supplied by the HOME origin, rather than decoded from duplicated caller manifests.
                for(var partition:plan.partitions()){var command=accepted.get(partition.route().groupId());if(command!=null)intents.add(new RatisDurableTransaction.Intent(partition.route(),command,new OperationId(first.sessionId(),first.epoch(),first.sequence()+n),new OperationId(first.sessionId(),first.epoch(),first.sequence()+64+n),partition.resultPositions(),captures.get(partition.route().groupId())));n++;}
                if(intents.size()!=count)throw new IllegalArgumentException("Accepted reroute contains foreign group");return new Evidence(original,source,target,absence,intents);
            }catch(IOException invalid){throw new IllegalArgumentException("Truncated reroute evidence",invalid);}
        }
    }
    public record Request(DistributedCommand command,Evidence evidence) { public Request { Objects.requireNonNull(command);if(!RatisTransactionStatuses.requiresRerouteAdmission(command))throw new IllegalArgumentException("Expected HOME reroute command"); } }
    public static byte[] prepareRequest(DistributedCommand command){return frame(PREPARE,command,null);}
    public static byte[] prepareRequest(DistributedCommand command,Evidence evidence){return frame(PREPARE,command,Objects.requireNonNull(evidence));}
    public static byte[] cancelRequest(DistributedCommand command){return frame(CANCEL,command,null);}
    public static byte[] preparedReply(int kind,DistributedCommand command){return frame(kind,command,null);}
    private static byte[] frame(int kind,DistributedCommand command,Evidence evidence){
        if(!kind(kind) || !RatisTransactionStatuses.requiresRerouteAdmission(command))throw new IllegalArgumentException("Expected HOME reroute reservation/promotion");
        if(evidence!=null)requireControls(command,evidence);
        try{var bytes=new ByteArrayOutputStream();var out=new DataOutputStream(bytes);put(out,DistributedCommandCodec.encode(command));put(out,evidence==null?new byte[0]:evidence.encode());out.flush();if(bytes.size()>MAX_BYTES-ScopedTransactionRpc.HEADER_BYTES)throw new IllegalArgumentException("Reroute request exceeds budget");return ScopedTransactionRpc.frame(kind,bytes.toByteArray());}catch(IOException impossible){throw new AssertionError(impossible);}
    }
    public static Request decodeRequest(byte[] bytes){int kind=ScopedTransactionRpc.kind(bytes,MAX_BYTES);if(!kind(kind))throw new IllegalArgumentException("Wrong reroute admission kind");var input=ByteBuffer.wrap(bytes,ScopedTransactionRpc.HEADER_BYTES,bytes.length-ScopedTransactionRpc.HEADER_BYTES);var command=DistributedCommandCodec.decode(blob(input,DistributedCommandCodec.MAX_ENCODED_BYTES));byte[] evidence=blob(input,MAX_BYTES-16);if(input.hasRemaining() || kind==CANCEL && evidence.length!=0)throw new IllegalArgumentException("Trailing/cancel reroute evidence");return new Request(command,evidence.length==0?null:Evidence.decode(evidence,TransactionStatus.decode(command.mutations().getFirst().value()).origin()));}
    public static DistributedCommand decode(byte[] bytes){return decodeRequest(bytes).command();}
    public static void requireEcho(byte[] bytes,int kind,DistributedCommand expected){var packet=ScopedTransactionRpc.parse(bytes,MAX_BYTES);var request=decodeRequest(bytes);if(packet.kind()!=kind || request.evidence()!=null || !Arrays.equals(DistributedCommandCodec.encode(request.command()),DistributedCommandCodec.encode(expected)))throw new IllegalArgumentException("Reroute admission changed original command/kind");}
    public static boolean acknowledgementOnly(DistributedCommand original){return original.kind()==DistributedCommand.Kind.DATA && original.mutations().isEmpty() && original.predicates().isEmpty() && original.acknowledgedThrough()>=0;}
    public static byte[] normalizedDigest(DistributedCommand original,RangeDirectory target){
        if(original.kind()==DistributedCommand.Kind.DATA && !acknowledgementOnly(original))return RatisAtomicClient.rawPlanDigest(RatisTransactionPlan.fromOriginal(target,original));
        if(original.kind()!=DistributedCommand.Kind.COMMIT_ACCEPTED && !acknowledgementOnly(original))throw new IllegalArgumentException("Invalid original reroute kind");
        byte[] encoded=DistributedCommandCodec.encode(original);byte[] domain="rockserver-reroute-original-input".getBytes(java.nio.charset.StandardCharsets.US_ASCII);return OperatorUpload.sha256(ByteBuffer.allocate(domain.length+32+4+encoded.length).put(domain).put(target.payloadDigest()).putInt(encoded.length).put(encoded).array());
    }
    public static int targetParticipantCount(DistributedCommand original,RangeDirectory target){if(acknowledgementOnly(original))return 0;if(original.kind()==DistributedCommand.Kind.COMMIT_ACCEPTED)return original.acceptance().scope().participants(target,TransactionStatus.MAX_PARTICIPANTS).size();return RatisTransactionPlan.fromOriginal(target,original).partitions().size();}
    private static void requireControls(DistributedCommand command,Evidence evidence){if(evidence.intents().isEmpty())return;var origin=TransactionStatus.decode(command.mutations().getFirst().value()).origin();var plan=RatisTransactionPlan.fromOriginal(evidence.targetDirectory(),evidence.original());int n=0;for(var part:plan.partitions()){for(var intent:evidence.intents())if(intent.route().groupId().equals(part.route().groupId()) && (!intent.route().equals(part.route()) || !intent.resultPositions().equals(part.resultPositions()) || !intent.prepareControl().equals(control(origin,n)) || !intent.finalizeControl().equals(control(origin,64+n))))throw new IllegalArgumentException("Reroute intent changed derived route/results/controls");n++;}}
    static OperationId control(OperationRerouteOrigin origin,int offset){var first=origin.firstControl();return new OperationId(first.sessionId(),first.epoch(),first.sequence()+offset);}
    private static DistributedCdcCheckpoint.Lineage lineage(RangeDirectory source,DistributedCommand command){var route=source.ranges().stream().filter(r->r.groupId().equals(command.groupId()) && r.rangeEpoch()==command.rangeEpoch()).findFirst().orElseThrow(()->new IllegalArgumentException("Original route missing"));return new DistributedCdcCheckpoint.Lineage(route.rangeId(),route.groupId(),route.rangeEpoch());}
    private static void put(DataOutputStream out,byte[] bytes)throws IOException{out.writeInt(bytes.length);out.write(bytes);}
    private static byte[] blob(ByteBuffer in,int max){if(in.remaining()<4)throw new IllegalArgumentException("Truncated reroute blob");int size=in.getInt();if(size<0 || size>max || size>in.remaining())throw new IllegalArgumentException("Invalid reroute blob bound");byte[] bytes=new byte[size];in.get(bytes);return bytes;}
    private static byte[] blob(DataInputStream in,int max)throws IOException{int size=in.readInt();if(size<0 || size>max || size>in.available())throw new IllegalArgumentException("Invalid reroute blob bound");return in.readNBytes(size);}
    private static void uuid(DataOutputStream out,UUID id)throws IOException{out.writeLong(id.getMostSignificantBits());out.writeLong(id.getLeastSignificantBits());}
    private static UUID uuid(DataInputStream in)throws IOException{return new UUID(in.readLong(),in.readLong());}
}
