package com.admin.service.impl;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.admin.dto.CampaniaDTO;
import com.admin.dto.ContadorDTO;
import com.admin.service.GeneraReporte;
import com.admin.service.dao.ConsultaCampaniaDetalle;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class ReporteServiceListenerImpl {
	
	@Autowired
	private GeneraReporte generaReporte;
	
	@Autowired
	private ConsultaCampaniaDetalle consultaCampaniaDetalle;
	
	@RabbitListener(queues = "solitudesReportes")
    public void receiveReporte(List<CampaniaDTO> listCampaniaDTO) {
        try {
        	for (CampaniaDTO campaniaDTO : listCampaniaDTO) {
        		ContadorDTO contadorRegistro = consultaCampaniaDetalle.validarDetallePorIdCampania(campaniaDTO.getIdCampania());
        		if (contadorRegistro.getContadorRegistros()>0) {
        			 String nombreArchivo = "D:\\develop_java\\reportes_campania\\reporte_" + campaniaDTO.getIdCampania() + ".csv";
        			 try (BufferedWriter writer = new BufferedWriter(new FileWriter(nombreArchivo))) {
             			generaReporte.generaCSVPorFechaCampania(contadorRegistro.getContadorRegistros(), campaniaDTO.getIdCampania(), null,writer);
        			 } catch (IOException e) {
        	                e.printStackTrace(); // Manejo de excepciones
        	         }
				}
                //Thread.sleep(1000);
			}//for
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
