
-- Complicated schema with 20+ tables, FKs, unique constraints, and diverse datatypes
CREATE TABLE users (
    user_id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE roles (
    role_id SERIAL PRIMARY KEY,
    role_name VARCHAR(30) UNIQUE NOT NULL
);

CREATE TABLE user_roles (
    user_id INT REFERENCES users(user_id),
    role_id INT REFERENCES roles(role_id),
    assigned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, role_id)
);

CREATE TABLE departments (
    dept_id SERIAL PRIMARY KEY,
    dept_name VARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE employees (
    emp_id SERIAL PRIMARY KEY,
    user_id INT UNIQUE REFERENCES users(user_id),
    dept_id INT REFERENCES departments(dept_id),
    hire_date DATE NOT NULL,
    salary NUMERIC(10,2) NOT NULL,
    manager_id INT
);

CREATE TABLE projects (
    project_id SERIAL PRIMARY KEY,
    project_name VARCHAR(100) NOT NULL,
    dept_id INT REFERENCES departments(dept_id),
    start_date DATE,
    end_date DATE
);

CREATE TABLE project_assignments (
    emp_id INT REFERENCES employees(emp_id),
    project_id INT REFERENCES projects(project_id),
    assigned_on DATE,
    PRIMARY KEY (emp_id, project_id)
);

CREATE TABLE addresses (
    address_id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(user_id),
    street VARCHAR(200),
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    country VARCHAR(50)
);

CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    sku VARCHAR(50) UNIQUE NOT NULL,
    price NUMERIC(10,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(user_id),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) NOT NULL
);

CREATE TABLE order_items (
    order_id INT REFERENCES orders(order_id),
    product_id INT REFERENCES products(product_id),
    quantity INT NOT NULL,
    price NUMERIC(10,2) NOT NULL,
    PRIMARY KEY (order_id, product_id)
);

CREATE TABLE reviews (
    review_id SERIAL PRIMARY KEY,
    product_id INT REFERENCES products(product_id),
    user_id INT REFERENCES users(user_id),
    rating INT,
    comment TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE categories (
    category_id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE product_categories (
    product_id INT REFERENCES products(product_id),
    category_id INT REFERENCES categories(category_id),
    PRIMARY KEY (product_id, category_id)
);

CREATE TABLE suppliers (
    supplier_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    contact_email VARCHAR(100) UNIQUE
);

CREATE TABLE product_suppliers (
    product_id INT REFERENCES products(product_id),
    supplier_id INT REFERENCES suppliers(supplier_id),
    PRIMARY KEY (product_id, supplier_id)
);

CREATE TABLE inventory (
    product_id INT PRIMARY KEY REFERENCES products(product_id),
    stock INT NOT NULL,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE payments (
    payment_id SERIAL PRIMARY KEY,
    order_id INT REFERENCES orders(order_id),
    amount NUMERIC(10,2) NOT NULL,
    paid_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    payment_method VARCHAR(30) NOT NULL
);

CREATE TABLE shipment (
    shipment_id SERIAL PRIMARY KEY,
    order_id INT REFERENCES orders(order_id),
    shipped_at TIMESTAMP,
    delivered_at TIMESTAMP,
    address_id INT REFERENCES addresses(address_id)
);

CREATE TABLE notifications (
    notification_id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(user_id),
    message TEXT NOT NULL,
    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    read BOOLEAN DEFAULT FALSE
);

CREATE TABLE audit_logs (
    log_id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(user_id),
    action VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE settings (
    user_id INT PRIMARY KEY REFERENCES users(user_id),
    theme VARCHAR(20) DEFAULT 'light',
    notifications_enabled BOOLEAN DEFAULT TRUE
);

CREATE TABLE tags (
    tag_id SERIAL PRIMARY KEY,
    name VARCHAR(50) UNIQUE NOT NULL
);

CREATE TABLE product_tags (
    product_id INT REFERENCES products(product_id),
    tag_id INT REFERENCES tags(tag_id),
    PRIMARY KEY (product_id, tag_id)
);

CREATE TABLE warehouses (
    warehouse_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    location VARCHAR(200)
);

CREATE TABLE warehouse_inventory (
    warehouse_id INT REFERENCES warehouses(warehouse_id),
    product_id INT REFERENCES products(product_id),
    stock INT NOT NULL,
    PRIMARY KEY (warehouse_id, product_id)
);

CREATE TABLE returns (
    return_id SERIAL PRIMARY KEY,
    order_id INT REFERENCES orders(order_id),
    product_id INT REFERENCES products(product_id),
    reason TEXT,
    returned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE coupons (
    coupon_id SERIAL PRIMARY KEY,
    code VARCHAR(30) UNIQUE NOT NULL,
    discount NUMERIC(5,2) NOT NULL,
    expires_at TIMESTAMP
);

CREATE TABLE order_coupons (
    order_id INT REFERENCES orders(order_id),
    coupon_id INT REFERENCES coupons(coupon_id),
    PRIMARY KEY (order_id, coupon_id)
);
