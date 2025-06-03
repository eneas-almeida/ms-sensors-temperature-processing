package com.eneas.sensors.temperature.processing.domain.services;

import com.eneas.sensors.temperature.processing.api.dtos.TemperatureInput;
import com.eneas.sensors.temperature.processing.api.dtos.TemperatureOutput;
import com.eneas.sensors.temperature.processing.commons.IdGenerator;
import com.eneas.sensors.temperature.processing.domain.models.TemperatureProcessing;
import com.eneas.sensors.temperature.processing.infra.rabbitmq.RabbitMQConfig;
import com.eneas.sensors.temperature.processing.infra.repositories.TemperatureProcessingRepository;
import io.hypersistence.tsid.TSID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Service
@Slf4j
@RequiredArgsConstructor
public class TemperatureProcessingService {

    private final RabbitTemplate rabbitTemplate;
    private final TemperatureProcessingRepository repository;

    private static final int BUFFER_LIMIT = 10000;
    private static final int TIMEOUT = 5;

    private final ExecutorService workerPool = Executors.newFixedThreadPool(100);
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(50);

    private final List<TemperatureProcessing> bufferOk = new ArrayList<>();
    private final List<TemperatureProcessing> bufferErro = new ArrayList<>();
    private final Lock lockOk = new ReentrantLock();
    private final Lock lockErro = new ReentrantLock();

    private ScheduledFuture<?> flushTaskOk;
    private ScheduledFuture<?> flushTaskErro;

    public void execute(TemperatureInput input) {
        Double temperature = obtemTemperatura(input.getValue());

        CompletableFuture.runAsync(() -> {
            try {
                enviaParaFila(input.getSensorId(), temperature);

                TemperatureProcessing entity = TemperatureProcessing.builder()
                        .id(IdGenerator.generateTimeBasedUUID().toString())
                        .sensorId(input.getSensorId())
                        .value(temperature)
                        .enqueued(true)
                        .createdAt(new Date())
                        .build();

                adicionarAoBuffer(entity, bufferOk, lockOk, true);

            } catch (Exception e) {
                log.error("Erro ao enviar para fila: {}", e.getMessage());

                TemperatureProcessing entity = TemperatureProcessing.builder()
                        .id(IdGenerator.generateTimeBasedUUID().toString())
                        .sensorId(input.getSensorId())
                        .value(temperature)
                        .enqueued(false)
                        .createdAt(new Date())
                        .build();

                adicionarAoBuffer(entity, bufferErro, lockErro, false);
            }
        }, workerPool);
    }

    private void adicionarAoBuffer(
            TemperatureProcessing entity,
            List<TemperatureProcessing> buffer,
            Lock lock,
            boolean sucessoEnvio
    ) {
        lock.lock();
        try {
            buffer.add(entity);

            if (buffer.size() >= BUFFER_LIMIT) {
                List<TemperatureProcessing> lote = new ArrayList<>(buffer);
                buffer.clear();
                enviarLoteAsync(lote, sucessoEnvio);
                cancelarFlush(sucessoEnvio);
            } else {
                reprogramarFlush(sucessoEnvio);
            }
        } finally {
            lock.unlock();
        }
    }

    private void reprogramarFlush(boolean sucessoEnvio) {
        ScheduledFuture<?> flushTask = sucessoEnvio ? flushTaskOk : flushTaskErro;

        if (flushTask != null && !flushTask.isDone()) {
            flushTask.cancel(false);
        }

        ScheduledFuture<?> newTask = scheduler.schedule(() -> {
            Lock lock = sucessoEnvio ? lockOk : lockErro;
            List<TemperatureProcessing> buffer = sucessoEnvio ? bufferOk : bufferErro;

            lock.lock();
            try {
                if (!buffer.isEmpty()) {
                    List<TemperatureProcessing> lote = new ArrayList<>(buffer);
                    buffer.clear();
                    enviarLoteAsync(lote, sucessoEnvio);
                }
            } finally {
                lock.unlock();
            }
        }, TIMEOUT, TimeUnit.SECONDS);

        if (sucessoEnvio) {
            flushTaskOk = newTask;
        } else {
            flushTaskErro = newTask;
        }
    }

    private void cancelarFlush(boolean sucessoEnvio) {
        if (sucessoEnvio && flushTaskOk != null) {
            flushTaskOk.cancel(false);
        } else if (!sucessoEnvio && flushTaskErro != null) {
            flushTaskErro.cancel(false);
        }
    }

    private void enviarLoteAsync(List<TemperatureProcessing> lote, boolean sucessoEnvio) {
        CompletableFuture.runAsync(() -> enviarParaMongo(lote, sucessoEnvio), workerPool)
                .thenRun(() -> log.info("Flush de {} concluído com {} registros",
                        sucessoEnvio ? "sucesso" : "erro", lote.size()))
                .exceptionally(ex -> {
                    log.error("Erro ao salvar no Mongo: {}", ex.getMessage());
                    return null;
                });
    }

    private void enviarParaMongo(List<TemperatureProcessing> lote, boolean sucessoEnvio) {
        try {
            repository.saveAll(lote);
        } catch (RuntimeException e) {
            log.error("Erro ao salvar lote [{}] no MongoDB: {}", sucessoEnvio ? "sucesso" : "erro", e.getMessage());
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Erro ao salvar no MongoDB");
        }
    }

    private void enviaParaFila(TSID sensorId, Double temperature) {
        TemperatureOutput output = TemperatureOutput.builder()
                .id(IdGenerator.generateTimeBasedUUID())
                .sensorId(sensorId)
                .value(temperature)
                .createdAt(OffsetDateTime.now())
                .build();

        log.info("Enviando para fila: {}", output);

        String exchange = RabbitMQConfig.FANOUT_EXCHANGE_NAME;
        String routingKey = "";

        MessagePostProcessor messagePostProcessor = message -> {
            message.getMessageProperties().setHeader("sensorId", sensorId.toString());
            return message;
        };

        rabbitTemplate.convertAndSend(exchange, routingKey, output, messagePostProcessor);
    }

    private Double obtemTemperatura(String value) {
        if (value == null || value.isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST);
        }

        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST);
        }
    }
}
