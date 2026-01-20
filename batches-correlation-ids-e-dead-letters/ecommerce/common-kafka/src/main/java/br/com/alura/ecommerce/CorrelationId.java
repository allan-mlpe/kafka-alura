package br.com.alura.ecommerce;

import java.util.UUID;

public class CorrelationId {

    private final String id;

    public CorrelationId(String prefix) {
        this.id = String.format("%s(%s)", prefix, UUID.randomUUID().toString());
    }

    @Override
    public String toString() {
        return "CorrelationId{" +
                "id='" + id + '\'' +
                '}';
    }

    public CorrelationId continueWith(String title) {
        return new CorrelationId(String.format("%s-%s", id, title));
    }
}
