package com.devs4j.kafka.callbacks;

import com.github.javafaker.Faker;

public class Test {
	public static void main(String[] args) {
		Faker faker = new Faker();
		faker.number().numberBetween(0, 100000);
	}
}
