package com.eneas.sensors.temperature.processing.infra.repositories;

import com.eneas.sensors.temperature.processing.domain.models.TemperatureProcessing;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface TemperatureProcessingRepository extends MongoRepository<TemperatureProcessing, String> {
}
