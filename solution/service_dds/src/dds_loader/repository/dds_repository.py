import uuid
from datetime import datetime
from typing import Dict, Any
from lib.pg import PgConnect

class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def insert_hub(self, table_name: str, pk_field: str, id_value: str, src: str) -> uuid.UUID:
        pk = uuid.uuid4()
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    INSERT INTO dds.{table_name} ({pk_field}, {table_name.replace('h_', '')}_id, load_dt, load_src)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT ({table_name.replace('h_', '')}_id) DO NOTHING;
                """, (str(pk), id_value, datetime.utcnow(), src))
        return pk

    def insert_link(self, table_name: str, pk_field: str, fk_fields: Dict[str, uuid.UUID], src: str) -> None:
        pk = uuid.uuid4()
        columns = ', '.join([pk_field] + list(fk_fields.keys()) + ['load_dt', 'load_src'])
        values = ', '.join(['%s'] * (len(fk_fields) + 3))
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    INSERT INTO dds.{table_name} ({columns})
                    VALUES ({values})
                    ON CONFLICT DO NOTHING;
                """, [str(pk)] + list(fk_fields.values()) + [datetime.utcnow(), src])

    def insert_satellite(self, table_name: str, hk_field: str, hk_value: uuid.UUID, fields: Dict[str, Any], src: str) -> None:
        hashdiff = uuid.uuid4()
        columns = ', '.join(['hk_' + table_name.replace('s_', '') + '_hashdiff'] + list(fields.keys()) + ['load_dt', 'load_src'])
        values = ', '.join(['%s'] * (len(fields) + 3))
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    INSERT INTO dds.{table_name} ({hk_field}, {columns})
                    VALUES (%s, {values})
                    ON CONFLICT ({hk_field}) DO NOTHING;
                """, [str(hk_value)] + [str(hashdiff)] + list(fields.values()) + [datetime.utcnow(), src])