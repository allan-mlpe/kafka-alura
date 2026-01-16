package br.com.alura.ecommerce;

import java.math.BigDecimal;

public record Order(String orderId, BigDecimal amount, String email) {

    @Override
    public String toString() {
        return "Order{" +
                "orderId='" + orderId + '\'' +
                ", amount=" + amount +
                ", email='" + email + '\'' +
                '}';
    }
}
