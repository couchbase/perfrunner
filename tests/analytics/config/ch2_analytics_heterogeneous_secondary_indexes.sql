CREATE INDEX customer_c_balance ON customer(c_balance);
CREATE INDEX orders_entry_d ON orders(o_entry_d);
CREATE INDEX orderline_i_id ON orders(UNNEST o_orderline SELECT ol_i_id:BIGINT) EXCLUDE UNKNOWN KEY;
CREATE INDEX orderline_delivery_d ON orders(UNNEST o_orderline SELECT ol_delivery_d:STRING) EXCLUDE UNKNOWN KEY;