package com.criteo.kafka;

/**
 * Throw this exception if you have a gauge which can not be reported to graphite due an invalid value.
 * An invalid value can be e.g. null or a non-numeric value
 *
 */
public class InvalidGaugeValueException extends Exception {
    public InvalidGaugeValueException(String message) {
        super(message);
    }
}
