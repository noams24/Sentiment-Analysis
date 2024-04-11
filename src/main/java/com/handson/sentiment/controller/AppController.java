package com.handson.sentiment.controller;

import com.handson.sentiment.kafka.AppKafkaSender;
// import com.handson.sentiment.nlp.SentimentAnalyzer;
// import com.handson.sentiment.twitter.AppNewsStream;
import com.handson.sentiment.nlp.SentimentAnalyzer;
import com.handson.sentiment.twitter.AppNewsStream;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import static com.handson.sentiment.kafka.KafkaTopicConfig.APP_TOPIC;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;

@RestController
public class AppController {

    @Autowired
    SentimentAnalyzer sentimentAnalyzer;

    @Autowired
    AppNewsStream newsStream;

    @Autowired
    AppKafkaSender kafkaSender;

    @Autowired
    KafkaReceiver<String, String> kafkaReceiver;

    @RequestMapping(path = "/sendKafka", method = RequestMethod.GET)
    public @ResponseBody Mono<String> sendText(String text) {
        kafkaSender.send(text, APP_TOPIC);
        return Mono.just("OK");
    }

    @RequestMapping(path = "/getKafka", method = RequestMethod.GET)
    public @ResponseBody Flux<String> getKafka() {
        return kafkaReceiver.receive().map(x -> x.value() + "<br>");

    }

    @RequestMapping(path = "/startNews", method = RequestMethod.GET)
    public @ResponseBody Flux<String> start(String text) throws InterruptedException {
        return newsStream.filter(text)
                .window(Duration.ofSeconds(3))
                .flatMap(window -> toArrayList(window))
                .map(messages -> {
                    if (messages.size() == 0)
                        return "size: 0 <br>";
                    return "size: " + messages.size() + "<br>";
                });
    }

    @RequestMapping(path = "/stopNews", method = RequestMethod.GET)
    public @ResponseBody Mono<String> stop() {
        newsStream.shutdown();
        return Mono.just("shutdown");
    }

    @RequestMapping(path = "/grouped", method = RequestMethod.GET)
    public @ResponseBody Flux<String> grouped(@RequestParam(defaultValue = "biden") String text,
            @RequestParam(defaultValue = "3") Integer timeWindowSec) throws IOException, InterruptedException {
        var flux = kafkaReceiver.receive().map(message -> message.value());
        newsStream.filter(text).map((x) -> kafkaSender.send(x, APP_TOPIC)).subscribe();

        return flux.map(x -> new TimeAndMessage(DateTime.now(), x))
                .window(Duration.ofSeconds(timeWindowSec))
                .flatMap(window -> toArrayList(window))
                .map(y -> {
                    if (y.size() == 0)
                        return "size: 0 <br>";
                    return "time:" + y.get(0).curTime + " size: " + y.size() + "<br>";
                });
    }

    @RequestMapping(path = "/sentiment", method = RequestMethod.GET)
    public @ResponseBody Flux<String> sentiment(@RequestParam(defaultValue = "obama") String text,
            @RequestParam(defaultValue = "3") Integer timeWindowSec) throws IOException, InterruptedException {
        var flux = kafkaReceiver.receive().map(message -> message.value());
        newsStream.filter(text).map((x) -> kafkaSender.send(x, APP_TOPIC)).subscribe();

        return flux.map(x -> new TimeAndMessage(DateTime.now(), x))
                .window(Duration.ofSeconds(timeWindowSec))
                .flatMap(window -> toArrayList(window))
                .map(items -> {
                    if (items.size() > 10)
                        return "size:" + items.size() + "<br>";
                    System.out.println("size:" + items.size());
                    double avg = items.stream().map(x -> sentimentAnalyzer.analyze(x.message))
                            .mapToDouble(y -> y).average().orElse(0.0);
                    if (items.size() == 0)
                        return "EMPTY<br>";
                    return items.size() + " messages, sentiment = " + avg + "<br>";

                });
    }

    static class TimeAndMessage {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd, HH:mm:ss, z");
        DateTime curTime;
        String message;

        public TimeAndMessage(DateTime curTime, String message) {
            this.curTime = curTime;
            this.message = message;
        }

        @Override
        public String toString() {
            return "TimeAndMessage{" +
                    "formatter=" + formatter +
                    ", curTime=" + curTime +
                    ", message='" + message + '\'' +
                    '}';
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static <T> Mono<ArrayList<T>> toArrayList(Flux<T> source) {
        return source.reduce(new ArrayList(), (a, b) -> {
            a.add(b);
            return a;
        });
    }

}
