package com.orange.kafka.utils;

import java.time.Instant;

/**
 * DateUtils compares two dates and returns bigger one
 * @return instant
 * @author Meirkhan Rakhmetzhanov
 */
public class DateUtils {
    public static Instant MaxInstant (Instant i1, Instant i2){
        return i1.compareTo(i2) > 0 ? i1 : i2;
    }
}

