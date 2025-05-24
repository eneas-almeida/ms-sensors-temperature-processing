package com.eneas.sensors.temperature.processing.api.controllers;

import com.eneas.sensors.temperature.processing.domain.services.TemperatureProcessingService;
import io.hypersistence.tsid.TSID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;


@RestController
@RequestMapping("/api/sensors/{sensorId}/temperatures/data")
@Slf4j
@RequiredArgsConstructor
public class TemperatureProcessingController {

    private final TemperatureProcessingService temperatureProcessingService;

    @PostMapping()
    public void handle(@PathVariable TSID sensorId, @RequestBody String value) {
        temperatureProcessingService.execute(sensorId, value);
    }
}