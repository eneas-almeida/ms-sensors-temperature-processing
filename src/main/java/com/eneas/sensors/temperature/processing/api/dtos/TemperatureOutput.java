package com.eneas.sensors.temperature.processing.api.dtos;

import io.hypersistence.tsid.TSID;
import lombok.Builder;
import lombok.Data;

import java.time.OffsetDateTime;
import java.util.UUID;

@Data
@Builder
public class TemperatureOutput {
    private UUID id;
    private TSID sensorId;
    private OffsetDateTime createdAt;
    private Double value;
}