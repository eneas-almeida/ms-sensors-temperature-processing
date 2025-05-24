package com.eneas.sensors.temperature.processing.domain.services;

import com.eneas.sensors.temperature.processing.api.dtos.TemperatureInput;
import com.eneas.sensors.temperature.processing.api.dtos.TemperatureOutput;
import com.eneas.sensors.temperature.processing.commons.IdGenerator;
import com.eneas.sensors.temperature.processing.infra.rabbitmq.RabbitMQConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.time.OffsetDateTime;

@Service
@Slf4j
@RequiredArgsConstructor
public class TemperatureProcessingService {

    private final RabbitTemplate rabbitTemplate;

    public void execute(TemperatureInput input) {
        if (input.getValue() == null || input.getValue().isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST);
        }

        Double temperature;

        try {
            temperature = Double.parseDouble(input.getValue());
        } catch (NumberFormatException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST);
        }

        TemperatureOutput output = TemperatureOutput.builder()
                .id(IdGenerator.generateTimeBasedUUID())
                .sensorId(input.getSensorId())
                .value(temperature)
                .createdAt(OffsetDateTime.now())
                .build();

        log.info(output.toString());

        String exchange = RabbitMQConfig.FANOUT_EXCHANGE_NAME;
        String routingKey = "";
        Object payload = output;

        MessagePostProcessor messagePostProcessor = message -> {
            message.getMessageProperties().setHeader("sensorId", output.getSensorId().toString());
            return message;
        };

        rabbitTemplate.convertAndSend(exchange, routingKey, payload, messagePostProcessor);
    }
}
