package com.eneas.sensors.temperature.processing.domain.services;

import com.eneas.sensors.temperature.processing.api.dtos.TemperatureInput;
import com.eneas.sensors.temperature.processing.api.dtos.TemperatureOutput;
import com.eneas.sensors.temperature.processing.commons.IdGenerator;
import com.eneas.sensors.temperature.processing.infra.rabbitmq.RabbitMQConfig;
import io.hypersistence.tsid.TSID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Service
@Slf4j
@RequiredArgsConstructor
public class TemperatureProcessingService {

    // Rabbit template
    private final RabbitTemplate rabbitTemplate;

    // Configurações
    private static final int BUFFER_LIMIT = 10;
    private static final int TIMEOUT = 5; // segundos
    private static final int THREADS_CORE = 4;
    private static final int THREADS_MAX = 10;
    private static final int MAX_TAREFAS_PENDENTES = 100;

    // Buffer e sincronização
    private final List<TemperatureInput> buffer = new ArrayList<>();
    private final Lock lock = new ReentrantLock();

    // Pool para tarefas de envio para o Mongo
    private final ExecutorService workerPool = new ThreadPoolExecutor(
            THREADS_CORE, THREADS_MAX, 60L, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(MAX_TAREFAS_PENDENTES),
            new ThreadPoolExecutor.CallerRunsPolicy() // Se tudo estiver cheio, executa na thread chamadora
    );

    // Agendador para flush por timeout
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture<?> flushTask;

    public void execute(TemperatureInput input) {
        Double temperature = obtemTemperatura(input.getValue());

        // 1. Envia para a fila
        enviaParaFila(input.getSensorId(), temperature);

        lock.lock();

        try {
            // 2. Adiciona no buffer
            buffer.add(input);

            // 3. Se atingiu o limite, flush imediato
            if (buffer.size() >= BUFFER_LIMIT) {
                List<TemperatureInput> lote = new ArrayList<>(buffer);
                buffer.clear();
                enviarLoteAsync(lote);

                // Cancela flush por timeout se houver
                if (flushTask != null && !flushTask.isDone()) {
                    flushTask.cancel(false);
                }
            } else {
                // 4. Se não atingiu limite, agenda flush por timeout
                if (flushTask != null && !flushTask.isDone()) {
                    flushTask.cancel(false);
                }

                flushTask = scheduler.schedule(() -> {
                    lock.lock();

                    try {
                        if (!buffer.isEmpty()) {
                            List<TemperatureInput> lote = new ArrayList<>(buffer);
                            buffer.clear();
                            enviarLoteAsync(lote);
                        }
                    } finally {
                        lock.unlock();
                    }
                }, TIMEOUT, TimeUnit.SECONDS);
            }
        } finally {
            lock.unlock();
        }
    }

    private void enviarLoteAsync(List<TemperatureInput> lote) {
        CompletableFuture.runAsync(() -> enviarParaMongo(lote), workerPool)
                .thenRun(() -> System.out.println("Flush concluido com " + lote.size() + " registros"))
                .exceptionally(ex -> {
                    System.err.println("Erro ao enviar para Mongo: " + ex.getMessage());
                    return null;
                });
    }

    private void enviarParaMongo(List<TemperatureInput> input) {
        log.info("Enviando {} medições para o mongo", input.size());

        for (TemperatureInput e : input) {
            System.out.println(" - MongoDB <- " + e);
        }

        try {
            Thread.sleep(3000); // simula tempo de escrita
            System.out.println("✓ MongoDB >>>>>>>>>>>>>>>>> <- " + input.size() + " medicoes");
        } catch (InterruptedException ignored) {
        }
    }

    private Double obtemTemperatura(String value) {
        if (value == null || value.isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST);
        }

        Double temperature;

        try {
            temperature = Double.parseDouble(value);
        } catch (NumberFormatException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST);
        }

        return temperature;
    }

    public void enviaParaFila(TSID sensorId, Double temperature) {
        TemperatureOutput output = TemperatureOutput.builder()
                .id(IdGenerator.generateTimeBasedUUID())
                .sensorId(sensorId)
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
