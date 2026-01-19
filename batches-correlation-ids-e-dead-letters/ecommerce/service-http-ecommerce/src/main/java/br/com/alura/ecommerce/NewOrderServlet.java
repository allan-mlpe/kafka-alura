package br.com.alura.ecommerce;


import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderServlet extends HttpServlet {

    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();
    private final KafkaDispatcher<Email> emailDispatcher = new KafkaDispatcher<>();

    @Override
    public void destroy() {
        super.destroy();
        this.orderDispatcher.close();
        this.emailDispatcher.close();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        try {
            // we're not caring about any security issues, we're only
            // showing how to use http as a starting point to send messages
            var orderId = UUID.randomUUID().toString();
            var amount = new BigDecimal(req.getParameter("amount"));
            var email = req.getParameter("email");

            var order = new Order(orderId, amount, email);
            this.orderDispatcher.send("ECOMMERCE_NEW_ORDER", email, order);

            var successEmail = new Email("Thanks for your order!", "We're processing your products.");
            this.emailDispatcher.send("ECOMMERCE_SEND_EMAIL", email, successEmail);

            System.out.println("New order sent successfully");

            resp.setStatus(HttpServletResponse.SC_OK);
            resp.getWriter().println("New order sent!");

        } catch (ExecutionException e) {
            throw new ServletException(e);
        } catch (InterruptedException e) {
            throw new ServletException(e);
        }
    }
}
