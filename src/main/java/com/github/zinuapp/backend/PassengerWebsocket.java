package com.github.zinuapp.backend;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.lettuce.core.GeoArgs;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.pubsub.api.reactive.ChannelMessage;
import io.micronaut.context.annotation.Requires;
import io.micronaut.websocket.WebSocketSession;
import io.micronaut.websocket.annotation.OnMessage;
import io.micronaut.websocket.annotation.OnOpen;
import io.micronaut.websocket.annotation.ServerWebSocket;
import jakarta.inject.Inject;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.util.stream.Collectors;

@ServerWebSocket("/ws/chat/{passengerId}")
@Requires(env = "passenger")
public class PassengerWebsocket {

	private static final Logger LOG = LoggerFactory.getLogger(PassengerWebsocket.class);

	@Inject
	StatefulRedisPubSubConnection<String, String> connection;

	@OnOpen
	public Publisher<String> onOpen(String passengerId, WebSocketSession session) {
		LOG.info("Passenger [passengerId={}] connected", passengerId);
		return session.send("welcome");
	}

	@OnMessage
	public Publisher<String> onMessage(String passengerId, String message, WebSocketSession session)
		throws JsonProcessingException {
		LOG.info("Received from passenger [passengerId={}]: {}", passengerId, message);

		if(message.startsWith("FIND_CARS")) {
			var lat = message.split("\s")[1];
			var longi = message.split("\s")[2];

			var commands = connection.sync();
			var cars = commands.georadius("cars-geoloc", Double.parseDouble(longi),
				Double.parseDouble(lat), 5, GeoArgs.Unit.km);

			LOG.info("Cars found [passengerId={}]: {}", passengerId, cars.size());
			return session.send(new JsonMapper().writeValueAsString(cars));
		}

		if (message.startsWith("GET_LOCATION")) {
			var car = message.split("\s")[1];

			if (session.contains("GET_LOCATION_ALREADY_SUBSCRIBED_" + car)) {
				return Flux.from(session.send("already subscribed to car " + car + " geolocation"))
					.doOnNext(msg -> logMessageSentToPassenger(passengerId, msg));
			}

			var reactive = connection.reactive();
			reactive.subscribe(car + "-geoloc-topic")
				.doOnSuccess(o -> session.put("GET_LOCATION_ALREADY_SUBSCRIBED_" + car, true))
				.subscribe();

			return reactive.observeChannels().map(ChannelMessage::getMessage)
				.doOnNext(msg -> logMessageSentToPassenger(passengerId, msg))
				.flatMap(session::send);
		}

		return Flux.from(session.send(message)).doOnNext(msg -> logMessageSentToPassenger(passengerId, msg));
	}

	private static void logMessageSentToPassenger(String passengerId, String message) {
		LOG.info("Message sent to passenger [passengerId={}]: {}", passengerId, message);
	}
}
