package com.admin.service.impl;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.IntStream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.admin.dto.DetallePorCampaniaDTO;
import com.admin.service.GeneraReporte;
import com.admin.service.dao.ConsultaCampaniaDetalle;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class GeneraReporteImpl implements GeneraReporte {

    @Autowired
    private ConsultaCampaniaDetalle consultaCampaniaDetalle;

    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor(); // Hilos Virtuales
    private final BlockingQueue<String> queue = new LinkedBlockingQueue<>(10000); // Límite para evitar OOM
    private final int FLUSH_INTERVAL = 100; // Número de líneas antes de hacer flush

    @Override
    public void generaCSVPorFechaCampania(Integer contador, Integer idCampania, String fecha,BufferedWriter writer) {
        if (contador > 0) {
            String nombreArchivo = "D:\\develop_java\\reportes_campania\\reporte_" + idCampania + ".csv";
            log.info("Registros a procesar: " + contador);

            int limiteP = 4000;
            int paginas = (contador + limiteP - 1) / limiteP;
            try {
                // Iniciar proceso de escritura en un hilo separado
                CompletableFuture<Void> writerTask = CompletableFuture.runAsync(() -> processQueue(writer), executor);

                // Cargar datos en paralelo
                List<CompletableFuture<Void>> futures = IntStream.range(0, paginas)
                        .mapToObj(page -> fetchAndQueuePage(page, limiteP, idCampania))
                        .toList();

                log.info("Páginas Listas: " + futures.size());

                // Esperar a que todas las páginas se carguen
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
                
                // Señal de finalización al escritor
                queue.put("EOF");
                
                // Esperar a que la escritura termine
                writerTask.join();
			} catch (Exception e) {
				executor.shutdown();
			}
        } else {
            log.info("Nada que procesar");
        }
    }

    private CompletableFuture<Void> fetchAndQueuePage(int page, int size, Integer idCampania) {
        return CompletableFuture.runAsync(() -> {
            log.info("Procesando página: " + page);
            List<DetallePorCampaniaDTO> detalles = consultaCampaniaDetalle.recuperaDetallePorIdCampania(idCampania, size, page);
            log.info("Registros en la página: " + detalles.size());
            
            detalles.forEach(detalle -> {
                try {
                    queue.put(generaLineaPorDetallePorCampaniaDTO(detalle));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.error("Error insertando en la cola", e);
                }
            });
        }, executor);
    }

    private void processQueue(BufferedWriter writer) {
        try {
            int lineCount = 0;
            while (true) {
                String line = queue.take(); // Bloquea hasta recibir datos
                if ("EOF".equals(line)) break; // Terminar si recibe la señal EOF
                
                writer.write(line + "\n");
                lineCount++;
                
                if (lineCount % FLUSH_INTERVAL == 0) {
                    writer.flush(); // Hacer flush cada 100 líneas
                }
            }
            writer.flush(); // Asegurar escritura final
        } catch (IOException | InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Error en escritura de archivo", e);
        }
    }

    private String generaLineaPorDetallePorCampaniaDTO(DetallePorCampaniaDTO detalle) {
        return detalle.getIdSmsDetalle() + "," + detalle.getDetalleCampania();
    }
}
