package com.processor.ddos.config;

import java.util.Properties;

public class KafkaConsumerConfig {

    private static final String SSL = "SSL";

    private String groupId;
    private String topic;
    private String maxPollRecords;
    private String consumerBootstrapServers;
    private boolean enableAutoCommit;
    private Integer sessionTimeoutMs;
    private Integer concurrency;

    private String keyDeserializer;
    private String valueDeserializer;

    private String retryBackoffMs;
    private String autoOffsetReset;

    private Integer maxPollIntervalMs;

    private String securityProtocol;
    private String sslTruststoreLocation;
    private String sslTruststorePassword;
    private String sslKeystoreLocation;
    private String sslKeystorePassword;

    private Integer fetchMinBytes;
    private Integer heartBeatIntervalMs;
    private Integer maxPartitionFetchBytes;
    private Integer fetchMaxWaitMs;

    private Long connectionsMaxIdleMs;
    private Integer requestTimeoutMs;

    public Properties createkafkaProp(){

        Properties properties = new Properties();

        addProperty("group.id", groupId, properties);
        addProperty("topic", topic, properties);
        addProperty("bootstrap.servers", consumerBootstrapServers, properties);
        addProperty("enable.auto.commit", enableAutoCommit, properties);
        addProperty("key.deserializer", keyDeserializer, properties);
        addProperty("value.deserializer", valueDeserializer, properties);
        addProperty("auto.offset.reset", autoOffsetReset, properties);
        addProperty("max.poll.records", maxPollRecords, properties);
        addProperty("session.timeout.ms", sessionTimeoutMs, properties);
        addProperty("retry.backoff.ms", retryBackoffMs, properties);
        addProperty("max.poll.interval.ms", maxPollIntervalMs, properties);

        addProperty("connections.max.idle.ms", connectionsMaxIdleMs, properties);
        addProperty("fetch.min.bytes", fetchMinBytes, properties);
        addProperty("heartbeat.interval.ms", heartBeatIntervalMs, properties);
        addProperty("max.partition.fetch.bytes", maxPartitionFetchBytes, properties);
        addProperty("fetch.max.wait.ms", fetchMaxWaitMs, properties);
        addProperty("request.timeout.ms", requestTimeoutMs, properties);

        if(SSL.equalsIgnoreCase(securityProtocol)){
            addProperty("security.protocol", securityProtocol, properties);
            addProperty("ssl.truststore.location", sslTruststoreLocation, properties);
            addProperty("ssl.truststore.password", sslTruststorePassword, properties);
            addProperty("ssl.keystore.location", sslKeystoreLocation, properties);
            addProperty("ssl.keystore.password", sslKeystorePassword, properties);
        }

        return properties;
    }

    private void addProperty(String name, Object value, Properties properties) {
        if(value != null)
            properties.put(name, value);
    }

    public Integer getMaxPollIntervalMs() {
        return maxPollIntervalMs;
    }

    public void setMaxPollIntervalMs(Integer maxPollIntervalMs) {
        this.maxPollIntervalMs = maxPollIntervalMs;
    }

    public String getSecurityProtocol() {
        return securityProtocol;
    }

    public void setSecurityProtocol(String securityProtocol) {
        this.securityProtocol = securityProtocol;
    }

    public String getSslTruststoreLocation() {
        return sslTruststoreLocation;
    }

    public void setSslTruststoreLocation(String sslTruststoreLocation) {
        this.sslTruststoreLocation = sslTruststoreLocation;
    }

    public String getSslTruststorePassword() {
        return sslTruststorePassword;
    }

    public void setSslTruststorePassword(String sslTruststorePassword) {
        this.sslTruststorePassword = sslTruststorePassword;
    }

    public String getSslKeystoreLocation() {
        return sslKeystoreLocation;
    }

    public void setSslKeystoreLocation(String sslKeystoreLocation) {
        this.sslKeystoreLocation = sslKeystoreLocation;
    }

    public String getSslKeystorePassword() {
        return sslKeystorePassword;
    }

    public void setSslKeystorePassword(String sslKeystorePassword) {
        this.sslKeystorePassword = sslKeystorePassword;
    }

    public String getConsumerBootstrapServers() {
        return consumerBootstrapServers;
    }

    public void setConsumerBootstrapServers(String consumerBootstrapServers) {
        this.consumerBootstrapServers = consumerBootstrapServers;
    }

    public boolean isEnableAutoCommit() {
        return enableAutoCommit;
    }

    public void setEnableAutoCommit(boolean enableAutoCommit) {
        this.enableAutoCommit = enableAutoCommit;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public Integer getSessionTimeoutMs() {
        return sessionTimeoutMs;
    }

    public void setSessionTimeoutMs(Integer sessionTimeoutMs) {
        this.sessionTimeoutMs = sessionTimeoutMs;
    }

    public String getKeyDeserializer() {
        return keyDeserializer;
    }

    public void setKeyDeserializer(String keyDeserializer) {
        this.keyDeserializer = keyDeserializer;
    }

    public String getValueDeserializer() {
        return valueDeserializer;
    }

    public void setValueDeserializer(String valueDeserializer) {
        this.valueDeserializer = valueDeserializer;
    }

    public String getMaxPollRecords() {
        return maxPollRecords;
    }

    public void setMaxPollRecords(String maxPollRecords) {
        this.maxPollRecords = maxPollRecords;
    }

    public String getAutoOffsetReset() {
        return autoOffsetReset;
    }

    public void setAutoOffsetReset(String autoOffsetReset) {
        this.autoOffsetReset = autoOffsetReset;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getRetryBackoffMs() {
        return retryBackoffMs;
    }

    public void setRetryBackoffMs(String retryBackoffMs) {
        this.retryBackoffMs = retryBackoffMs;
    }

    public Integer getConcurrency() {
        return concurrency;
    }

    public Integer getFetchMinBytes() {
        return fetchMinBytes;
    }

    public void setFetchMinBytes(Integer fetchMinBytes) {
        this.fetchMinBytes = fetchMinBytes;
    }

    public Integer getHeartBeatIntervalMs() {
        return heartBeatIntervalMs;
    }

    public void setHeartBeatIntervalMs(Integer heartBeatIntervalMs) {
        this.heartBeatIntervalMs = heartBeatIntervalMs;
    }

    public Integer getMaxPartitionFetchBytes() {
        return maxPartitionFetchBytes;
    }

    public void setMaxPartitionFetchBytes(Integer maxPartitionFetchBytes) {
        this.maxPartitionFetchBytes = maxPartitionFetchBytes;
    }

    public Integer getFetchMaxWaitMs() {
        return fetchMaxWaitMs;
    }

    public void setFetchMaxWaitMs(Integer fetchMaxWaitMs) {
        this.fetchMaxWaitMs = fetchMaxWaitMs;
    }

    public Long getConnectionsMaxIdleMs() {
        return connectionsMaxIdleMs;
    }

    public void setConnectionsMaxIdleMs(Long connectionsMaxIdleMs) {
        this.connectionsMaxIdleMs = connectionsMaxIdleMs;
    }

    public void setConcurrency(Integer concurrency) {
        this.concurrency = concurrency;
    }

    public Integer getRequestTimeoutMs() {
        return requestTimeoutMs;
    }

    public void setRequestTimeoutMs(Integer requestTimeoutMs) {
        this.requestTimeoutMs = requestTimeoutMs;
    }
}
