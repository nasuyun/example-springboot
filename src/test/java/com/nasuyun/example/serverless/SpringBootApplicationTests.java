package com.nasuyun.example.serverless;

import org.elasticsearch.common.time.DateUtils;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Calendar;
import java.util.Date;

@SpringBootTest
class SpringBootApplicationTests {

	@Test
	void contextLoads() {
		Calendar c = Calendar.getInstance();
		Date from = c.getTime();
	}

}
