package it.cavallium.rockserver.core.common;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

public final class FutureUtils {

    private FutureUtils() {
    }

    public static <T, R> CompletableFuture<R> mapCancellation(CompletableFuture<T> source,
                                                              Function<? super T, ? extends R> mapper) {
        requireArgument(source, "source");
        requireArgument(mapper, "mapper");
        return propagateCancellation(source.thenApply(mapper), source);
    }

    public static <T> CompletableFuture<T> propagateCancellation(CompletableFuture<T> response,
                                                                 Future<?> cancellationTarget) {
        requireArgument(response, "response");
        requireArgument(cancellationTarget, "cancellationTarget");
        return new CompletableFuture<>() {
            {
                response.whenComplete((value, failure) -> {
                    if (failure != null) {
                        completeFailure(this, failure);
                    } else {
                        complete(value);
                    }
                });
            }

            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                var canceled = super.cancel(mayInterruptIfRunning);
                if (canceled) {
                    cancellationTarget.cancel(mayInterruptIfRunning);
                }
                return canceled;
            }
        };
    }

    public static <T, R> CompletableFuture<R> composeCancellation(
            CompletableFuture<T> source,
            Function<? super T, ? extends CompletableFuture<? extends R>> mapper) {
        requireArgument(source, "source");
        requireArgument(mapper, "mapper");
        var active = new AtomicReference<Future<?>>(source);
        var interruptOnCancel = new AtomicBoolean();
        var response = new CompletableFuture<R>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                if (mayInterruptIfRunning) {
                    interruptOnCancel.set(true);
                }
                var canceled = super.cancel(mayInterruptIfRunning);
                if (canceled) {
                    active.get().cancel(interruptOnCancel.get());
                }
                return canceled;
            }
        };
        source.whenComplete((value, failure) -> {
            if (response.isDone()) {
                return;
            }
            if (failure != null) {
                completeFailure(response, failure);
                return;
            }
            CompletableFuture<? extends R> next;
            try {
                next = requireMapperResult(mapper.apply(value));
            } catch (Throwable mappingFailure) {
                completeFailure(response, mappingFailure);
                return;
            }
            active.set(next);
            if (response.isCancelled()) {
                next.cancel(interruptOnCancel.get());
                return;
            }
            next.whenComplete((nextValue, nextFailure) -> {
                if (nextFailure != null) {
                    completeFailure(response, nextFailure);
                } else {
                    response.complete(nextValue);
                }
            });
        });
        return response;
    }

    public static <T> CompletableFuture<T> submitCancellationAware(ExecutorService executor,
                                                                   Supplier<? extends T> supplier) {
        requireArgument(executor, "executor");
        requireArgument(supplier, "supplier");
        var response = new CompletableFuture<T>();
        var invocation = executor.submit(() -> {
            try {
                response.complete(supplier.get());
            } catch (Throwable failure) {
                response.completeExceptionally(failure);
            }
        });
        return propagateCancellation(response, invocation);
    }

    public static <T> T awaitInterruptibly(Future<T> future, String operation) {
        requireArgument(future, "future");
        requireArgument(operation, "operation");
        try {
            return future.get();
        } catch (InterruptedException e) {
            cancelWithSuppressed(future, e);
            Thread.currentThread().interrupt();
            throw new CompletionException(operation + " interrupted", e);
        } catch (ExecutionException e) {
            var failure = unwrapCompletion(e.getCause());
            if (failure instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (failure instanceof Error error) {
                throw error;
            }
            throw new CompletionException(explicitCompletionMessage(e.getCause(), operation + " failed"), failure);
        }
    }

    public static RuntimeException unwrapCompletionException(CompletionException failure, String fallbackMessage) {
        var unwrappedFailure = unwrapCompletionFailure(failure, fallbackMessage);
        if (unwrappedFailure instanceof RuntimeException runtimeException) {
            return runtimeException;
        }
        throw (Error) unwrappedFailure;
    }

    public static Throwable unwrapCompletionFailure(CompletionException failure, String fallbackMessage) {
        requireArgument(failure, "failure");
        requireArgument(fallbackMessage, "fallbackMessage");
        var cause = unwrapCompletion(failure);
        if (cause instanceof RuntimeException runtimeException) {
            return runtimeException;
        }
        if (cause instanceof Error error) {
            return error;
        }
        return new CompletionException(explicitCompletionMessage(failure, fallbackMessage), cause);
    }

    public static void cancelWithSuppressed(Future<?> future, Throwable failure) {
        requireArgument(future, "future");
        requireArgument(failure, "failure");
        try {
            future.cancel(true);
        } catch (RuntimeException | Error cancellationFailure) {
            if (failure != cancellationFailure) {
                failure.addSuppressed(cancellationFailure);
            }
        }
    }

    private static void requireArgument(Object value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
    }

    private static <T> T requireMapperResult(T value) {
        if (value == null) {
            throw new IllegalStateException("mapper result must not be null");
        }
        return value;
    }

    private static <T> void completeFailure(CompletableFuture<T> response, Throwable failure) {
        var cause = unwrapCompletion(failure);
        if (cause instanceof CancellationException) {
            response.cancel(false);
        } else {
            response.completeExceptionally(cause);
        }
    }

    private static Throwable unwrapCompletion(Throwable failure) {
        if (failure instanceof CompletionException completionException && completionException.getCause() != null) {
            return unwrapCompletion(completionException.getCause());
        }
        return failure;
    }

    private static String explicitCompletionMessage(Throwable failure, String fallbackMessage) {
        if (failure instanceof CompletionException completionException) {
            if (hasExplicitMessage(completionException)) {
                return completionException.getMessage();
            }
            if (completionException.getCause() != null) {
                return explicitCompletionMessage(completionException.getCause(), fallbackMessage);
            }
        }
        return fallbackMessage;
    }

    private static boolean hasExplicitMessage(CompletionException completionException) {
        var message = completionException.getMessage();
        var cause = completionException.getCause();
        return message != null && (cause == null || !message.equals(cause.toString()));
    }
}
