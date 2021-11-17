package com.github.zinuapp.backend;

import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.env.Environment;
import io.micronaut.scheduling.annotation.Scheduled;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

@Singleton
@Requires(env = "car")
public class FakeCarJob {

	private static final Logger LOG = LoggerFactory.getLogger(FakeCarJob.class);
	private static final Random random = new Random();

	@Inject Environment environment;
	@Inject	StatefulRedisPubSubConnection<String, String> connection;

	@Scheduled(fixedDelay = "10s")
	void executeEveryTen() {
		var commands = connection.sync();

		for (var carId : Constants.CARS_ID) {
			var randomCoordinates = Constants.COORDINATES[random.nextInt(Constants.COORDINATES.length)];
			LOG.info("The car {} will be placed in longitude {} and latitude {}", carId, randomCoordinates[1],
				randomCoordinates[0]);
			commands.publish(carId + "-geoloc-topic", randomCoordinates[0] + "$" + randomCoordinates[1]);
			commands.geoadd("cars-geoloc", randomCoordinates[1], randomCoordinates[0], carId);
		}
	}
}
