package br.com.alura.ecommerce;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class GenerateAllReportsServlet extends HttpServlet {
    private final KafkaDispatcher<String> taskDispatcher = new KafkaDispatcher<>();

    @Override
    public void destroy() {
        super.destroy();
        this.taskDispatcher.close();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        try {

            // aqui estamos fazendo um fast delegate, mandando uma mensagem "cedo",
            // retornando rápido para o usuário e fazendo o resto do processamento
            // pesado em "background". Isso é útil pois evita que qualquer problema
            // no servidor cause perda de dados, por exemplo.
            taskDispatcher.send(
                    "ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS",
                    UUID.randomUUID().toString(), // usando um randomUUID como key para ter paralelismo com consumidores em partições diferentes
                    new CorrelationId(GenerateAllReportsServlet.class.getSimpleName()),
                    "ECOMMERCE_USER_GENERATE_READING_REPORT"
            );

            System.out.println("Sent generate report to all users");
            resp.setStatus(HttpServletResponse.SC_OK);
            resp.getWriter().println("Report requests generated!");

        } catch (ExecutionException e) {
            throw new ServletException(e);
        } catch (InterruptedException e) {
            throw new ServletException(e);
        }
    }
}
