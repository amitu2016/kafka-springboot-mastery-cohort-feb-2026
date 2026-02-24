-- Create orders table
CREATE TABLE IF NOT EXISTS orders (
    order_id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    order_status VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL
);

-- Insert sample data
INSERT INTO orders (customer_id, product_id, quantity, price, order_status) VALUES
(1001, 2001, 2, 59.98, 'PENDING'),
(1002, 2002, 1, 89.99, 'CONFIRMED'),
(1003, 2003, 3, 137.97, 'SHIPPED'),
(1004, 2001, 1, 29.99, 'DELIVERED'),
(1005, 2004, 2, 599.98, 'PENDING');

