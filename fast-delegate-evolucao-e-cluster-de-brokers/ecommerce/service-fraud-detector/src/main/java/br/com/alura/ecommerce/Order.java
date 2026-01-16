package br.com.alura.ecommerce;

import java.math.BigDecimal;

public record Order(String userId, String orderId, BigDecimal amount) {
    @Override
    public String toString() {
        return "Order{" +
                "userId='" + userId + '\'' +
                ", orderId='" + orderId + '\'' +
                ", amount=" + amount +
                '}';
    }
}
