package it.cavallium.rockserver.core.common.distributed;

/** One backend-wide point envelope, including historical owners and reserved recovery/renewal workspace. */
public record RemoteMutationOwnerLimits(int pointSlots,int maximumPointOwners,int maximumRequests,long maximumBytes,long recoveryBytes,long renewalBytes,int recoveryWorkers) {
    public RemoteMutationOwnerLimits {
        if(pointSlots<1 || pointSlots>16 || maximumPointOwners<pointSlots || maximumPointOwners>64 || maximumRequests<pointSlots || maximumRequests>maximumPointOwners || maximumBytes<16L*1024*1024 || maximumBytes>256L*1024*1024 || recoveryBytes<16L*1024*1024 || renewalBytes<1024*1024 || recoveryBytes+renewalBytes>=maximumBytes || recoveryWorkers<1 || recoveryWorkers>2)throw new IllegalArgumentException("Invalid shared point owner envelope");
    }
    public static RemoteMutationOwnerLimits defaults(){return new RemoteMutationOwnerLimits(2,32,16,128L*1024*1024,16L*1024*1024,8L*1024*1024,1);}
}
