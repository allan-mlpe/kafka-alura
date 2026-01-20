package br.com.alura.ecommerce;

public record User(String uuid) {
    @Override
    public String toString() {
        return "User{" +
                "uuid='" + uuid + '\'' +
                '}';
    }
}
