from datetime import datetime
from typing import Dict, Any
from lib.pg import PgConnect

class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def update_user_category_counter(self, user_id: str, category_id: str, category_name: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO cdm.user_category_counters (id, user_id, category_id, category_name, order_cnt)
                    VALUES (DEFAULT, %s, %s, %s, 1)
                    ON CONFLICT (user_id, category_id) 
                    DO UPDATE SET 
                        order_cnt = cdm.user_category_counters.order_cnt + 1;
                """, (user_id, category_id, category_name))

    def update_user_product_counter(self, user_id: str, product_id: str, product_name: str) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO cdm.user_product_counters (id, user_id, product_id, product_name, order_cnt)
                    VALUES (DEFAULT, %s, %s, %s, 1)
                    ON CONFLICT (user_id, product_id) 
                    DO UPDATE SET 
                        order_cnt = cdm.user_product_counters.order_cnt + 1;
                """, (user_id, product_id, product_name))