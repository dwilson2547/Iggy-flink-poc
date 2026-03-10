package com.example.iggy.flink.pipeline;

import org.apache.flink.api.common.functions.MapFunction;

import java.io.Serializable;

/**
 * Abstract stage for applying domain logic between the deserialization and serialization stages.
 *
 * <p>This stage transforms an input type {@code I} (deserialized from Iggy) into an output
 * type {@code O} (which will be serialized back to Iggy). Users should extend this class to
 * implement their core business logic.
 *
 * <p>This stage implements Flink's {@link MapFunction} to integrate cleanly into the
 * Flink pipeline, allowing full access to Flink's execution context.
 *
 * <p>Example usage:
 * <pre>{@code
 * public class OrderEnrichmentStage extends DomainStage<OrderEvent, EnrichedOrder> {
 *     @Override
 *     public EnrichedOrder process(OrderEvent input) throws Exception {
 *         return new EnrichedOrder(input, lookupCustomer(input.getCustomerId()));
 *     }
 * }
 * }</pre>
 *
 * <p>Future extension point: database connectivity can be added by injecting a
 * repository into implementations of this stage.
 *
 * @param <I> the input type (produced by {@link DeserializationStage})
 * @param <O> the output type (consumed by {@link SerializationStage})
 */
public abstract class DomainStage<I, O> implements MapFunction<I, O>, Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Applies domain logic to transform the input into the output type.
     *
     * @param input the deserialized input object
     * @return the domain-processed output object
     * @throws Exception if processing fails
     */
    public abstract O process(I input) throws Exception;

    /**
     * Implements Flink's {@link MapFunction#map(Object)} by delegating to {@link #process(Object)}.
     */
    @Override
    public final O map(I value) throws Exception {
        return process(value);
    }

    /**
     * Returns a human-readable name for this domain stage, used in logging.
     *
     * @return stage name
     */
    public String getStageName() {
        return getClass().getSimpleName();
    }
}
