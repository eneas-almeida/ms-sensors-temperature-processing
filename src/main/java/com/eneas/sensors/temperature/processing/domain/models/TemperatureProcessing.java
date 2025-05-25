package com.eneas.sensors.temperature.processing.domain.models;

import io.hypersistence.tsid.TSID;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;

@Builder
@Getter
@Setter
@Document(collection = "temperatures")
public class TemperatureProcessing {

    @Id
    private String id;

    private TSID sensorId;

    private Double value;

    private Date createdAt;
}
