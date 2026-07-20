package it.cavallium.rockserver.core.replication.ratis;

import java.lang.reflect.InvocationTargetException;
import java.util.List;

public record RatisNonVotingCatchUpAssessment(String ratisVersion,
                                              boolean listenerRolePresent,
                                              boolean observerRolePresent,
                                              boolean listenerStartupRoleSupported,
                                              boolean listenerConfigurationApiPresent,
                                              boolean catchUpAndPromotionProven,
                                              String decision) {

    public RatisNonVotingCatchUpAssessment {
        RatisAdapterBoundary.requireNonBlank(ratisVersion, "ratisVersion");
        RatisAdapterBoundary.requireNonBlank(decision, "decision");
    }

    public static RatisNonVotingCatchUpAssessment assessClasspath(String ratisVersion) {
        var listenerRolePresent = hasRole("LISTENER");
        var observerRolePresent = hasRole("OBSERVER");
        var listenerStartupRoleSupported = listenerRolePresent && canBuildListenerPeer();
        var listenerConfigurationApiPresent = listenerRolePresent && canBuildListenerConfiguration();
        var catchUpAndPromotionProven = false;
        var decision = listenerRolePresent
                ? "Ratis listener APIs are present, but catch-up and promotion behavior is not proven"
                : "Ratis listener APIs are not present on the current classpath";
        return new RatisNonVotingCatchUpAssessment(
                ratisVersion,
                listenerRolePresent,
                observerRolePresent,
                listenerStartupRoleSupported,
                listenerConfigurationApiPresent,
                catchUpAndPromotionProven,
                decision
        );
    }

    public boolean permitsNonVotingOnlineReplacement() {
        return listenerRolePresent
                && listenerStartupRoleSupported
                && listenerConfigurationApiPresent
                && catchUpAndPromotionProven;
    }

    private static boolean hasRole(String roleName) {
        try {
            role(roleName);
            return true;
        } catch (ReflectiveOperationException | IllegalArgumentException ignored) {
            return false;
        }
    }

    private static boolean canBuildListenerPeer() {
        try {
            var listenerRole = role("LISTENER");
            var raftPeerClass = Class.forName("org.apache.ratis.protocol.RaftPeer");
            var builder = raftPeerClass.getMethod("newBuilder").invoke(null);
            builder.getClass().getMethod("setId", String.class).invoke(builder, "listener-probe");
            builder.getClass().getMethod("setAddress", String.class).invoke(builder, "127.0.0.1:1");
            builder.getClass().getMethod("setStartupRole", listenerRole.getDeclaringClass()).invoke(builder,
                    listenerRole);
            var listener = builder.getClass().getMethod("build").invoke(builder);
            return listenerRole.equals(raftPeerClass.getMethod("getStartupRole").invoke(listener));
        } catch (ReflectiveOperationException | IllegalArgumentException ignored) {
            return false;
        }
    }

    private static boolean canBuildListenerConfiguration() {
        try {
            var roleClass = role("LISTENER").getDeclaringClass();
            var argumentsClass = Class.forName("org.apache.ratis.protocol.SetConfigurationRequest$Arguments");
            var builder = argumentsClass.getMethod("newBuilder").invoke(null);
            var builderClass = builder.getClass();
            builderClass.getMethod("setServersInNewConf", List.class);
            builderClass.getMethod("setListenersInNewConf", List.class);
            argumentsClass.getMethod("getPeersInNewConf", roleClass);
            return true;
        } catch (ReflectiveOperationException | IllegalArgumentException ignored) {
            return false;
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Enum<?> role(String roleName) throws ReflectiveOperationException {
        var roleClass = Class.forName("org.apache.ratis.proto.RaftProtos$RaftPeerRole").asSubclass(Enum.class);
        try {
            return Enum.valueOf((Class<? extends Enum>) roleClass, roleName);
        } catch (ExceptionInInitializerError exception) {
            throw new InvocationTargetException(exception);
        }
    }
}
