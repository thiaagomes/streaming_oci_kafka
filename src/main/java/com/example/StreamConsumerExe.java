package com.example;

public class StreamConsumerExe {
	
	public static void main(String[] args) {
		System.out.println("consumer");
		StreamConsumer consume = new StreamConsumer();
		consume.consume();
	}

}