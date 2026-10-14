package it.cavallium.rockserver.core.common.distributed;

/** Backend-wide POINT/BULK/RANGE ownership across current and historical generations. Journal bytes are logical file lengths. */
public record RemoteMutationOwnerLimits(int pointSlots,int maximumPointOwners,int maximumBulkOwners,int maximumRangeOwners,
        int maximumRequests,long maximumBytes,long maximumDiskBytes,int maximumJournalPaths,long recoveryBytes,long renewalBytes,int recoveryWorkers) {
    public RemoteMutationOwnerLimits {
        int owners=maximumPointOwners+maximumBulkOwners+maximumRangeOwners;
        if(pointSlots<1 || pointSlots>16 || maximumPointOwners<pointSlots || maximumPointOwners>64 || maximumBulkOwners<0 || maximumBulkOwners>64 || maximumRangeOwners<0 || maximumRangeOwners>64
                || maximumRequests<pointSlots || maximumRequests>owners || maximumBytes<16L*1024*1024 || maximumBytes>256L*1024*1024 || maximumDiskBytes<16L*1024*1024 || maximumDiskBytes>(1L<<40) || maximumJournalPaths<1 || maximumJournalPaths>4096
                || recoveryBytes<16L*1024*1024 || renewalBytes<1024*1024 || recoveryBytes+renewalBytes>=maximumBytes || recoveryWorkers<1 || recoveryWorkers>2)throw new IllegalArgumentException("Invalid shared remote owner envelope");
    }
    public static RemoteMutationOwnerLimits defaults(){return new RemoteMutationOwnerLimits(2,32,8,8,16,128L<<20,512L<<20,512,16L<<20,8L<<20,1);}
}
