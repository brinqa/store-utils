package org.neo4j.tool.copy;

import io.reactivex.rxjava3.core.Flowable;
import it.unimi.dsi.fastutil.Pair;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

@Slf4j
public class FlowableRetryUtil {

    @Value
    @Builder(toBuilder = true)
    public static class Config implements TrackingInfo {

        // required information for the meta-data
        @NonNull
        String username;
        @NonNull
        String transactionId;

        /**
         * Timeout for the query
         */
        @NonNull
        Duration timeout;

        /**
         * Additional meta-data
         */
        @Singular
        Map<String, ?> items;

        @Default
        @NonNull
        ExponentialBackoff exponentialBackoff = ExponentialBackoff.DEFAULT;

        @Value
        @Builder(toBuilder = true)
        public static class ExponentialBackoff {
            private static final ExponentialBackoff DEFAULT = ExponentialBackoff.builder().build();

            /**
             * Number of retries
             */
            @Default
            int count = 5;

            /**
             * Multiplier
             */
            @Default
            double multiplier = 1.1;

            /**
             * Initial interval (ms)
             */
            @Default
            long initialInterval = 500L;

            /**
             * Max added interval (ms)
             */
            @Default
            long maximumInterval = 2_000L;
        }
    }

    public static Function<Flowable<? extends Throwable>, Flowable<?>> exponentialBackoff(
            Config config) {
        final int maxRetryCount = config.getExponentialBackoff().getCount();
        final ExponentialBackOff backOff = new ExponentialBackOff(config.getExponentialBackoff());
        return errors ->
                errors
                        .zipWith(Flowable.range(1, maxRetryCount), Pair::of)
                        .flatMap(
                                p -> {
                                    final int retry = p.right();
                                    final Throwable th = p.left();
                                    log.warn("Retrying exception count {},", retry, th);
                                    return (Retryables.isRetryableError(th))
                                            ? Flowable.timer(backOff.nextInterval(), TimeUnit.MILLISECONDS)
                                            : Flowable.error(th);
                                });
    }

    static class ExponentialBackOff {

        private final double multiplier;
        private final long maxIntervalMillis;

        // field to calculate
        private final AtomicLong currentIntervalMillis = new AtomicLong();

        public ExponentialBackOff(final Config.ExponentialBackoff policy) {
            this.multiplier = policy.getMultiplier();
            this.maxIntervalMillis = policy.getMaximumInterval();
            this.currentIntervalMillis.set(policy.getInitialInterval());
        }

        public long nextInterval() {
            return currentIntervalMillis.getAndUpdate(
                    operand -> {
                        // Check for overflow, if overflow is detected set the current
                        // interval variable to the max interval.
                        final long time = (long) (operand * multiplier);
                        return Math.min(maxIntervalMillis, time);
                    });
        }
    }
}
