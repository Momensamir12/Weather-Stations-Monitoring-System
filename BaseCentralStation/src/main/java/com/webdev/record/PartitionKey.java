package com.webdev.record;

import java.time.LocalDate;

public record PartitionKey(long stationId, LocalDate date) {}