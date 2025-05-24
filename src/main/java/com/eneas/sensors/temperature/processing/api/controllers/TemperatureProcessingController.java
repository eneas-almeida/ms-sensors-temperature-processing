package com.eneas.sensors.temperature.processing.api.controllers;

import com.eneas.sensors.temperature.processing.api.dtos.TemperatureInput;
import com.eneas.sensors.temperature.processing.domain.services.TemperatureProcessingService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/sensors")
@RequiredArgsConstructor
public class TemperatureProcessingController {

    private final TemperatureProcessingService temperatureProcessingService;

    @PostMapping
    public void handle(@RequestBody TemperatureInput temperatureInput) {
        temperatureProcessingService.execute(temperatureInput);
    }
}