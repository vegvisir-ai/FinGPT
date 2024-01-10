package com.vegvisir.common.entity;

import java.util.List;
import java.util.UUID;

public record Customer(UUID id, String username, String email, String stripeToken, List<Long> history) {
}
