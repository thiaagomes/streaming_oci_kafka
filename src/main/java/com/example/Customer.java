package com.example;

public class Customer {
	
	private String name;
	private int age;
	private String cpf;
	
	
	public Customer() {

	}


	public Customer(String name, int age, String cpf) {
		super();
		this.name = name;
		this.age = age;
		this.cpf = cpf;
	}


	public String getName() {
		return name;
	}


	public void setName(String name) {
		this.name = name;
	}


	public int getAge() {
		return age;
	}


	public void setAge(int age) {
		this.age = age;
	}


	public String getCpf() {
		return cpf;
	}


	public void setCpf(String cpf) {
		this.cpf = cpf;
	}
	
	
	
	

}