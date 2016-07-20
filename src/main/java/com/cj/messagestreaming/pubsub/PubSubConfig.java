package com.cj.messagestreaming.pubsub;

public class PubSubConfig {
    String project;
    String topic;
    String subscription;

    PubSubConfig(String project, String topic, String subscription) {
        this.project = project;
        this.topic = topic;
        this.subscription = subscription;
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getSubscription() {
        return subscription;
    }

    public void setSubscription(String subscription) {
        this.subscription = subscription;
    }
}
