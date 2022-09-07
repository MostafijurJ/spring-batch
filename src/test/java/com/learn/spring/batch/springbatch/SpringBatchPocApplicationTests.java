package com.learn.spring.batch.springbatch;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class SpringBatchPocApplicationTests {

	@Test
	void contextLoads() {
	}

	private static final String TEST_STRING = "This is a test string";

	@Test
	void testString() {
		if(TEST_STRING.contains("test")) {
			System.out.println("Test string contains test");
		}
	}

}
