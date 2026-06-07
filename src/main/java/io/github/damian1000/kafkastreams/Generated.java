package io.github.damian1000.kafkastreams;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marker annotation read by JaCoCo (>= 0.8.2) to skip the annotated method
 * during coverage analysis. Use it for entry points that only wire up the
 * KafkaStreams runtime + shutdown hook — they cannot be exercised without a
 * real broker and would otherwise drag overall coverage down.
 */
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.CLASS)
public @interface Generated {
}
