package com.processor.ddos.model;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;

import java.time.LocalDateTime;
import java.util.Objects;

public class ApacheLogEntry {

        @SerializedName("ip_address")
        private String ipAddress;

        @SerializedName("request")
        private String requestDetail;

        @SerializedName("bytes_sent")
        private String bytesSent;

        @SerializedName("browser")
        private String browser;

        @SerializedName("response_status_code")
        private String responseStatusCode;

        @SerializedName("timestamp")
        private LocalDateTime timestamp;

        public ApacheLogEntry(String ipAddress, String requestDetail, String bytesSent, String browser, String responseStatusCode, LocalDateTime timestamp) {
            this.ipAddress = ipAddress;
            this.requestDetail = requestDetail;
            this.bytesSent = bytesSent;
            this.browser = browser;
            this.responseStatusCode = responseStatusCode;
            this.timestamp = timestamp;
        }

        public String getIpAddress() {
            return ipAddress;
        }

        public String getRequestDetail() {
            return requestDetail;
        }

        public String getBytesSent() {
            return bytesSent;
        }

        public String getBrowser() {
            return browser;
        }

        public String getResponseStatusCode() {
            return responseStatusCode;
        }

        public LocalDateTime getTimestamp() {
            return timestamp;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ApacheLogEntry that = (ApacheLogEntry) o;
            return Objects.equals(ipAddress, that.ipAddress) &&
                    Objects.equals(requestDetail, that.requestDetail) &&
                    Objects.equals(bytesSent, that.bytesSent) &&
                    Objects.equals(browser, that.browser) &&
                    Objects.equals(responseStatusCode, that.responseStatusCode) &&
                    Objects.equals(timestamp, that.timestamp);
        }

        @Override
        public int hashCode() {
            return Objects.hash(ipAddress, requestDetail, bytesSent, browser, responseStatusCode, timestamp);
        }

        @Override
        public String toString() {
            return "ApacheLogTemplate{" +
                    "ipAddress='" + ipAddress + '\'' +
                    ", request='" + requestDetail + '\'' +
                    ", bytesSent='" + bytesSent + '\'' +
                    ", browser='" + browser + '\'' +
                    ", responseStatusCode='" + responseStatusCode + '\'' +
                    ", timestamp='" + timestamp + '\'' +
                    '}';
        }

        public static ApacheLogEntry fromJson(String json) {
            Gson gson = new GsonBuilder().registerTypeAdapter(LocalDateTime.class, new LocalDateTimeJsonConverter()).create();
            return gson.fromJson(json, ApacheLogEntry.class);
        }
}
