package com.github.zinuapp.backend;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.lettuce.core.GeoArgs;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.pubsub.api.reactive.ChannelMessage;
import io.micronaut.context.annotation.Requires;
import io.micronaut.core.type.Argument;
import io.micronaut.websocket.WebSocketSession;
import io.micronaut.websocket.annotation.OnClose;
import io.micronaut.websocket.annotation.OnMessage;
import io.micronaut.websocket.annotation.OnOpen;
import io.micronaut.websocket.annotation.ServerWebSocket;
import jakarta.inject.Inject;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.util.ArrayList;

@ServerWebSocket("/ws/passenger")
@Requires(env = "passenger")
public class PassengerWebsocket {

	private static final Logger LOG = LoggerFactory.getLogger(PassengerWebsocket.class);

	@Inject
	RedisClient redisClient;

	@OnOpen
	public Publisher<String> onOpen(WebSocketSession session) {
		LOG.info("Passenger [sessionId={}] connected", session.getId());
		return session.send("welcome$" + session.getId());
	}

	@OnMessage
	public Publisher<String> onMessage(String message, WebSocketSession session)
		throws JsonProcessingException {
		LOG.info("Received from passenger [sessionId={}]: {}", session.getId(), message);

		if (message.startsWith("FIND_CARS")) {
			var connection = redisClient.connect();
			var lat = message.split("\\$")[1];
			var longi = message.split("\\$")[2];

			var commands = connection.sync();
			var cars = commands.georadius("cars-geoloc", Double.parseDouble(longi),
				Double.parseDouble(lat), 5, GeoArgs.Unit.km);

			LOG.info("Cars found [sessionId={}]: {}", session.getId(), cars.size());
			connection.close();

			return session.send(new JsonMapper().writeValueAsString(cars));
		}

		if (message.startsWith("GET_LOCATION")) {
			var car = message.split("\\$")[1];

			if (session.contains("GET_LOCATION_ALREADY_SUBSCRIBED_" + car)) {
				return Flux.from(session.send("already subscribed to car " + car + " geolocation"))
					.doOnNext(msg -> logMessageSentToPassenger(session.getId(), msg));
			}

			var connection = redisClient.connectPubSub();
			var reactive = connection.reactive();
			reactive.subscribe(car + "-geoloc-topic")
				.doOnSuccess(o -> session.put("GET_LOCATION_ALREADY_SUBSCRIBED_" + car, true))
				.doFinally(o -> {
					if(!session.contains("redis-connections")) {
						session.put("redis-connections", new ArrayList<>());
					}

					session.get("redis-connections", Argument.listOf(StatefulRedisPubSubConnection.class))
						.ifPresent(connections -> connections.add(connection));
				})
				.subscribe();

			return reactive.observeChannels()
				.map(ChannelMessage::getMessage)
				.map(coordinates -> car + "$" + coordinates)
				.doOnNext(msg -> logMessageSentToPassenger(session.getId(), msg))
				.flatMap(session::send);
		}

		return Flux.from(session.send(message)).doOnNext(msg -> logMessageSentToPassenger(session.getId(), msg));
	}

	@OnClose
	public Publisher<String> onClose(WebSocketSession session) {
		LOG.info("Passenger [sessionId={}] disconnected", session.getId());

		session.get("redis-connections", Argument.listOf(StatefulRedisPubSubConnection.class))
			.ifPresent(connections -> connections.forEach(StatefulConnection::close));

		return session.send("bye");
	}

	private static void logMessageSentToPassenger(String sessionId, String message) {
		LOG.info("Message sent to passenger [sessionId={}]: {}", sessionId, message);
	}
}
