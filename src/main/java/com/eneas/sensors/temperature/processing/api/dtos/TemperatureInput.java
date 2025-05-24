package com.eneas.sensors.temperature.processing.api.dtos;

import io.hypersistence.tsid.TSID;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class TemperatureInput {

    public TSID sensorId;
    public String value;
}
