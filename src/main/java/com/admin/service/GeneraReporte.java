package com.admin.service;

import java.io.BufferedWriter;

public interface GeneraReporte {

	void generaCSVPorFechaCampania(Integer contador,Integer idCampania,String fecha,BufferedWriter writer);
	
}
