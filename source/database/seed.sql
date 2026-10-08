INSERT INTO customers (customer_id, full_name, email, created_at, updated_at) VALUES (0000001, 'Le Phuoc Yen', 'lephuocyen20@gmail.com', '2026-09-26', '2026-09-26');
INSERT INTO customers (customer_id, full_name, email, created_at, updated_at) VALUES (0000002, 'Nguyen Van A', 'nguyenvana@gmail.com', '2026-09-26', '2026-09-26');
INSERT INTO customers (customer_id, full_name, email, created_at, updated_at) VALUES (0000003, 'Tran Van B', 'tranvanb@gmail.com', '2026-09-26', '2026-09-26');
INSERT INTO customers (customer_id, full_name, email, created_at, updated_at) VALUES (0000004, 'Tran Quan Vu', 'vunecacem@gmail.com', '2026-09-26', '2026-09-26');
INSERT INTO customers (customer_id, full_name, email, created_at, updated_at) VALUES (0000005, 'Doan Van C', 'cdoan@gmail.com', '2026-09-26', '2026-09-26');

INSERT INTO products (product_id, product_name, stock_quantity, price, created_at,updated_at) VALUES (0000001, 'RAM', 40, 4000000, '2026-09-26', '2026-09-26');
INSERT INTO products (product_id, product_name, stock_quantity, price, created_at,updated_at) VALUES (0000002, 'CPU', 10, 10000000, '2026-09-26', '2026-09-26');
INSERT INTO products (product_id, product_name, stock_quantity, price, created_at,updated_at) VALUES (0000003, 'GPU', 7, 15000000, '2026-09-26', '2026-09-26');
INSERT INTO products (product_id, product_name, stock_quantity, price, created_at,updated_at) VALUES (0000004, 'Main', 9, 2000000, '2026-09-26', '2026-09-26');
INSERT INTO products (product_id, product_name, stock_quantity, price, created_at,updated_at) VALUES (0000005, 'Fan', 30, 500000, '2026-09-26', '2026-09-26');
INSERT INTO products (product_id, product_name, stock_quantity, price, created_at,updated_at) VALUES (0000006, 'Mouse', 40, 400000, '2026-09-26', '2026-09-26');
INSERT INTO products (product_id, product_name, stock_quantity, price, created_at,updated_at) VALUES (0000007, 'Keyboard', 12, 350000, '2026-09-26', '2026-09-26');
INSERT INTO products (product_id, product_name, stock_quantity, price, created_at,updated_at) VALUES (0000008, 'SSD', 10, 600000, '2026-09-26', '2026-09-26');
INSERT INTO products (product_id, product_name, stock_quantity, price, created_at,updated_at) VALUES (0000009, 'Screen', 9, 1500000, '2026-09-26', '2026-09-26');
INSERT INTO products (product_id, product_name, stock_quantity, price, created_at,updated_at) VALUES (0000010, 'Bin', 20, 1000000, '2026-09-26', '2026-09-26');

INSERT INTO orders (order_id, customer_id, status, order_date, updated_at) VALUES (0000001, 0000004, 'PROCESSING', '2026-09-27', '2026-09-28');
INSERT INTO orders (order_id, customer_id, status, order_date, updated_at) VALUES (0000002, 0000001, 'CONFIRMED', '2026-09-27', '2026-09-27');
INSERT INTO orders (order_id, customer_id, status, order_date, updated_at) VALUES (0000003, 0000001, 'SHIPPED', '2026-09-26', '2026-09-28');
INSERT INTO orders (order_id, customer_id, status, order_date, updated_at) VALUES (0000004, 0000002, 'SHIPPED', '2026-09-27', '2026-09-28');
INSERT INTO orders (order_id, customer_id, status, order_date, updated_at) VALUES (0000005, 0000005, 'DELIVERED', '2026-09-27', '2026-09-29');

INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price, discount_amount) VALUES (0000001, 0000001, 0000002, 1, 10000000, 1500000);
INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price, discount_amount) VALUES (0000002, 0000001, 0000001, 2, 4000000, 500000);
INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price, discount_amount) VALUES (0000003, 0000001, 0000004, 1, 2000000, 0);
INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price, discount_amount) VALUES (0000004, 0000001, 0000010, 1, 1000000, 0);
INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price, discount_amount) VALUES (0000005, 0000002, 0000001, 2, 4000000, 1000000);
INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price, discount_amount) VALUES (0000006, 0000002, 0000003, 1, 15000000, 1000000);
INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price, discount_amount) VALUES (0000007, 0000003, 0000006, 1, 400000, 0);
INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price, discount_amount) VALUES (0000008, 0000003, 0000007, 1, 350000, 0);
INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price, discount_amount) VALUES (0000009, 0000003, 0000009, 1, 1500000, 100000);
INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price, discount_amount) VALUES (0000010, 0000003, 0000008, 1, 600000, 0);
INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price, discount_amount) VALUES (0000011, 0000004, 0000001, 2, 4000000, 100000);
INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price, discount_amount) VALUES (0000012, 0000004, 0000008, 1, 600000, 50000);
INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price, discount_amount) VALUES (0000013, 0000004, 0000005, 3, 500000, 0);
INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price, discount_amount) VALUES (0000014, 0000005, 0000001, 2, 4000000, 150000);
INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price, discount_amount) VALUES (0000015, 0000005, 0000004, 1, 2000000, 170000);
INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price, discount_amount) VALUES (0000016, 0000005, 0000002, 1, 10000000, 1000000);

INSERT INTO payments (payment_id, order_id, amount, payment_method, status, transaction_id, created_at) VALUES (0000001, 0000001, 19000000, 'CASH', 'PENDING', '00000dfg01', '2026-09-28 09:30:00');
INSERT INTO payments (payment_id, order_id, amount, payment_method, status, transaction_id, created_at) VALUES (0000002, 0000001, 19000000, 'CASH', 'SUCCESS', '000dsf0001', '2026-09-29 09:30:00');
INSERT INTO payments (payment_id, order_id, amount, payment_method, status, transaction_id, created_at) VALUES (0000003, 0000002, 21000000, 'MOMO', 'PENDING', '00sdw00001', '2026-09-28 09:30:00');
INSERT INTO payments (payment_id, order_id, amount, payment_method, status, transaction_id, created_at) VALUES (0000004, 0000003, 2750000, 'CASH', 'PENDING', '000faf0001', '2026-09-28 09:30:00');
INSERT INTO payments (payment_id, order_id, amount, payment_method, status, transaction_id, created_at) VALUES (0000005, 0000003, 2750000, 'CASH', 'SUCCESS', '0df00000g1', '2026-09-30 09:30:00');
INSERT INTO payments (payment_id, order_id, amount, payment_method, status, transaction_id, created_at) VALUES (0000006, 0000004, 9950000, 'CASH', 'FAILED', '00dg00h001', '2026-09-28 09:30:00');
INSERT INTO payments (payment_id, order_id, amount, payment_method, status, transaction_id, created_at) VALUES (0000007, 0000002, 21000000, 'MOMO', 'PENDING', '0000dsg001', '2026-09-30 09:30:00');
INSERT INTO payments (payment_id, order_id, amount, payment_method, status, transaction_id, created_at) VALUES (0000008, 0000005, 19000000, 'CASH', 'PENDING', '00dgh00001', '2026-09-28 09:30:00');
INSERT INTO payments (payment_id, order_id, amount, payment_method, status, transaction_id, created_at) VALUES (0000009, 0000005, 18680000, 'CASH', 'FAILED', '00hf00001', '2026-09-30 09:30:00');

INSERT INTO shipments (shipment_id, order_id, carrier, tracking_number, shipping_address, shipping_fee, status, shipped_at, delivered_at, estimated_delivery_at, created_at, updated_at) VALUES (0000001, 0000001, 'GHN', 'GHN202609280001', '123 Nguyen Trai, Thanh Xuan, Ha Noi', 30000, 'PICKED_UP', '2026-09-28 09:30:00', NULL, '2026-09-30 18:00:00', '2026-09-28 09:00:00', '2026-09-28 09:30:00');
INSERT INTO shipments (shipment_id, order_id, carrier, tracking_number, shipping_address, shipping_fee, status, shipped_at, delivered_at, estimated_delivery_at, created_at, updated_at) VALUES (0000002, 0000003, 'GHTK', 'GHTK202609280003', '45 Le Loi, Hai Chau, Da Nang', 25000, 'IN_TRANSIT', '2026-09-28 10:15:00', NULL, '2026-09-30 18:00:00', '2026-09-28 10:00:00', '2026-09-28 14:00:00');
INSERT INTO shipments (shipment_id, order_id, carrier, tracking_number, shipping_address, shipping_fee, status, shipped_at, delivered_at, estimated_delivery_at, created_at, updated_at) VALUES (0000003, 0000004, 'Viettel Post', 'VTP202609280004', '78 Vo Van Tan, District 3, Ho Chi Minh City', 35000, 'OUT_FOR_DELIVERY', '2026-09-28 08:45:00', NULL, '2026-09-29 18:00:00', '2026-09-28 08:30:00', '2026-09-29 08:00:00');
INSERT INTO shipments (shipment_id, order_id, carrier, tracking_number, shipping_address, shipping_fee, status, shipped_at, delivered_at, estimated_delivery_at, created_at, updated_at) VALUES (0000004, 0000005, 'J&T Express', 'JNT202609280005', '12 Tran Phu, Ngo Quyen, Hai Phong', 30000, 'DELIVERED', '2026-09-28 11:00:00', '2026-09-29 15:20:00', '2026-09-29 18:00:00', '2026-09-28 10:45:00', '2026-09-29 15:20:00');

SELECT setval(
    pg_get_serial_sequence('customers', 'customer_id'),
    (SELECT MAX(customer_id) FROM customers)
);

SELECT setval(
    pg_get_serial_sequence('products', 'product_id'),
    (SELECT MAX(product_id) FROM products)
);

SELECT setval(
    pg_get_serial_sequence('orders', 'order_id'),
    (SELECT MAX(order_id) FROM orders)
);

SELECT setval(
    pg_get_serial_sequence('order_items', 'order_item_id'),
    (SELECT MAX(order_item_id) FROM order_items)
);

SELECT setval(
    pg_get_serial_sequence('payments', 'payment_id'),
    (SELECT MAX(payment_id) FROM payments)
);

SELECT setval(
    pg_get_serial_sequence('shipments', 'shipment_id'),
    (SELECT MAX(shipment_id) FROM shipments)
);