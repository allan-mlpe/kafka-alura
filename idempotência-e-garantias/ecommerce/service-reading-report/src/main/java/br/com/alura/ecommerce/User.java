package br.com.alura.ecommerce;

public record User(String uuid) {
    public String getReportPath() {
        return "target/" + uuid + "-report.txt";
    }
}
