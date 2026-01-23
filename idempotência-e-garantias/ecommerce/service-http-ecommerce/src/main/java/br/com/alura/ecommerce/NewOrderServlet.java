package br.com.alura.ecommerce;


import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

public class NewOrderServlet extends HttpServlet {

    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();

    @Override
    public void destroy() {
        super.destroy();
        this.orderDispatcher.close();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        try {
            // we're not caring about any security issues, we're only
            // showing how to use http as a starting point to send messages
            var orderId = req.getParameter("uuid");
            var amount = new BigDecimal(req.getParameter("amount"));
            var email = req.getParameter("email");

            var order = new Order(orderId, amount, email);

            // estamos criando uma conexão com o banco a cada request para fins didáticos
            // no mundo real, poderíamos usar, por exemplo, um pool de conexões.
            try(var database = new OrdersDatabase()) {
                if (database.saveNew(order)) {
                    this.orderDispatcher.send(
                            "ECOMMERCE_NEW_ORDER",
                            email,
                            new CorrelationId(NewOrderServlet.class.getSimpleName()),
                            order);

                    System.out.println("New order sent successfully");

                    resp.setStatus(HttpServletResponse.SC_OK);
                    resp.getWriter().println("New order sent!");

                } else {
                    System.out.println("Old order received.");

                    resp.setStatus(HttpServletResponse.SC_OK);
                    resp.getWriter().println("Old order received!");
                }
            }

        } catch (ExecutionException e) {
            throw new ServletException(e);
        } catch (InterruptedException | SQLException e) {
            throw new ServletException(e);
        }
    }
}
