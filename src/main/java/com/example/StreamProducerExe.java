package com.example;

import com.google.gson.Gson;

public class StreamProducerExe {

	public static void main(String[] args) {
		System.out.println("producer");
		StreamProducer producer = new StreamProducer();
		
         
		Customer cus = new Customer();
		cus.setName("Thiago");
		cus.setAge(29);
		cus.setCpf("112233440088");

		Gson gson = new Gson();
		String json = gson.toJson(cus);
		System.out.println(json);
	
	
		producer.produce(json);
	
	
	}	

	

}