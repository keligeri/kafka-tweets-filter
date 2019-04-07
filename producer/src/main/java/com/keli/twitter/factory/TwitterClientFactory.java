package com.keli.twitter.factory;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.util.List;
import java.util.concurrent.BlockingQueue;

public class TwitterClientFactory {

    public static Client createTwitterClient(
            BlockingQueue<String> queue, List<String> termsToFollow) {

        Hosts hosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();

        endpoint.trackTerms(termsToFollow);

        return new ClientBuilder()
                .name("twitter-one")
                .hosts(hosts)
                .authentication(configureAuthentication())
                .endpoint(endpoint)
                .processor(new StringDelimitedProcessor(queue))
                .build();
    }

    private static Authentication configureAuthentication() {
        return new OAuth1(
                System.getenv("CONSUMER_KEY"),
                System.getenv("CONSUMER_SECRET"),
                System.getenv("TOKEN"),
                System.getenv("TOKEN_SECRET"));
    }

}
