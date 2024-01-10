package com.vegvisir.common.entity;

public record PredictionRequest(String tickerId,
                                int period,
                                String customerEmail) {
}
