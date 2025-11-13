SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;


CREATE INDEX idx_onesql_customer ON onesql_customer (c_w_id,c_d_id,c_last,c_first);
CREATE INDEX idx_onesql_orders ON onesql_orders (o_w_id,o_d_id,o_c_id,o_id);
-- CREATE INDEX fkey_onesql_stock_2 ON onesql_stock (s_i_id);
-- CREATE INDEX fkey_onesql_order_line_2 ON onesql_order_line (ol_supply_w_id,ol_i_id);

-- ALTER TABLE onesql_district  ADD CONSTRAINT fkey_onesql_district_1
--     FOREIGN KEY(d_w_id) REFERENCES onesql_warehouse(w_id);
-- ALTER TABLE onesql_customer ADD CONSTRAINT fkey_onesql_customer_1
--     FOREIGN KEY(c_w_id,c_d_id) REFERENCES onesql_district(d_w_id,d_id);
-- ALTER TABLE onesql_history  ADD CONSTRAINT fkey_onesql_history_1
--     FOREIGN KEY(h_c_w_id,h_c_d_id,h_c_id) REFERENCES onesql_customer(c_w_id,c_d_id,c_id);
-- ALTER TABLE onesql_history  ADD CONSTRAINT fkey_onesql_history_2
--     FOREIGN KEY(h_w_id,h_d_id) REFERENCES onesql_district(d_w_id,d_id);
-- ALTER TABLE onesql_new_orders ADD CONSTRAINT fkey_onesql_new_orders_1
--     FOREIGN KEY(no_w_id,no_d_id,no_o_id) REFERENCES onesql_orders(o_w_id,o_d_id,o_id);
-- ALTER TABLE onesql_orders ADD CONSTRAINT fkey_onesql_orders_1
--     FOREIGN KEY(o_w_id,o_d_id,o_c_id) REFERENCES onesql_customer(c_w_id,c_d_id,c_id);
-- ALTER TABLE onesql_order_line ADD CONSTRAINT fkey_onesql_order_line_1
--     FOREIGN KEY(ol_w_id,ol_d_id,ol_o_id) REFERENCES onesql_orders(o_w_id,o_d_id,o_id);
-- ALTER TABLE onesql_order_line ADD CONSTRAINT fkey_onesql_order_line_2
--     FOREIGN KEY(ol_supply_w_id,ol_i_id) REFERENCES onesql_stock(s_w_id,s_i_id);
-- ALTER TABLE onesql_stock ADD CONSTRAINT fkey_onesql_stock_1
-      FOREIGN KEY(s_w_id) REFERENCES onesql_warehouse(w_id);
-- ALTER TABLE onesql_stock ADD CONSTRAINT fkey_onesql_stock_2
--     FOREIGN KEY(s_i_id) REFERENCES onesql_item(i_id);


SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
