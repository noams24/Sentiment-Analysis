package com.handson.sentiment.twitter;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.Disposable;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;

@Component
public class AppNewsStream {


    private WebClient webClient;

    private Queue<String> articlesQueue;

    EmitterProcessor<String> emitterProcessor;

    private ScheduledExecutorService scheduler ;

    @PostConstruct
    public void init() {
        this.webClient = WebClient.builder()
                .baseUrl("https://newsapi.org/v2")
                .defaultHeader("X-Api-Key", "5b38541e2ee7427bb8840ea681ea5ccd")
                .build();
    }


    public Flux<String> filter(String keyword) throws InterruptedException {
        shutdown();
        this.articlesQueue = new ConcurrentLinkedQueue<>();
        return fetch(keyword);
    }

    public Flux<String> fetch(String keyword) throws InterruptedException {
        this.webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/everything")
                        .queryParam("q", keyword)
                        .queryParam("pageSize", 100)
                        .queryParam("page", 1)
                        .build()).retrieve()
                .bodyToMono(NewsApiResponse.class)
                .flatMapIterable(NewsApiResponse::getArticlesLines)
                .subscribe(articlesQueue::offer);

        emitterProcessor = EmitterProcessor.create();
        emitterProcessor.map(x -> x);
        scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(runnable, 0, 300, TimeUnit.MILLISECONDS);
        return emitterProcessor;
    }


    List<String> ignoreLines = Arrays.asList("Deliver and maintain Google services" ,
            "Track outages and protect against spam, fraud, and abuse" ,
            "Measure audience engagement and site statistics");
    Runnable runnable = () -> {
            String articleLine = articlesQueue.poll();

            if (articleLine != null) {
                for  (String line: ignoreLines) {
                    if (articleLine.contains(line)) {
                        return;
                    }
                }
                System.out.println(articleLine);
                emitterProcessor.onNext(articleLine);
            }
    };


    public void shutdown() {
        if (emitterProcessor!= null && !emitterProcessor.isTerminated())   {
            emitterProcessor.onComplete();
            scheduler.shutdown();
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class NewsApiResponse {

        @JsonProperty("status")
        private String status;

        @JsonProperty("totalResults")
        private int totalResults;

        @JsonProperty("articles")
        private List<NewsArticle> articles;

        public String getStatus() {
            return status;
        }

        public List<String> getArticlesLines() {
            List<String> lines = new ArrayList<>();
            for (NewsArticle article : getArticles()) {
                lines.add(article.getTitle());
                lines.addAll(Arrays.asList(article.getContent().split("\n")));
            }
            return lines;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public int getTotalResults() {
            return totalResults;
        }

        public void setTotalResults(int totalResults) {
            this.totalResults = totalResults;
        }

        public List<NewsArticle> getArticles() {
            return articles;
        }

        public void setArticles(List<NewsArticle> articles) {
            this.articles = articles;
        }

        public NewsApiResponse(String status, int totalResults, List<NewsArticle> articles) {
            this.status = status;
            this.totalResults = totalResults;
            this.articles = articles;
        }

        public NewsApiResponse() {
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class NewsArticle {

        @JsonProperty("title")
        private String title;

        @JsonProperty("description")
        private String description;

        @JsonProperty("content")
        private String content;

        public String getContent() {
            return content;
        }

        public void setContent(String content) {
            this.content = content;
        }

        @JsonProperty("url")
        private String url;

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public NewsArticle(String title, String description, String url) {
            this.title = title;
            this.description = description;
            this.url = url;
        }

        public NewsArticle() {
        }
    }
}

