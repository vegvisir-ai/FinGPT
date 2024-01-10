package com.vegvisir.common.entity;

public record Ticker(String tickerId, String retrievedInfo, String predictionResult, double rate) {
}
