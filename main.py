import pandas as pd
import pyarrow as pa
from pyiceberg.exceptions import NamespaceAlreadyExistsError, TableAlreadyExistsError
from hive_catalog_iceberg import HiveCatalogIceberg


class DFToIceberg():
    def __init__(self, catalog_name, host, port, s3_endpoint, s3_access_key_id, s3_secret):
        self.hive_catalog = HiveCatalogIceberg(
            catalog_name=catalog_name,
            host=host,
            port=port,
            s3_endpoint=s3_endpoint,
            s3_access_key_id=s3_access_key_id,
            s3_secret_access_key=s3_secret,
        )
        self.catalog = self.hive_catalog.catalog

    def save_to_iceberg(self, df: pd.DataFrame, iceberg_table_name: str):
        # save to format iceberg
        schema = pa.Schema.from_pandas(df)

        try:
            table = self.catalog.create_table(iceberg_table_name, schema=schema)
        except TableAlreadyExistsError as e:
            print("Table exists")

        except NamespaceAlreadyExistsError as e:
            print("Namespace exists")

        except Exception as e:
            print(f"Error: {e}")

        table = self.catalog.load_table(iceberg_table_name)
        df_arrow = pa.Table.from_pandas(df)
        table.append(df_arrow)
        print(f"Saved to iceberg table: {iceberg_table_name}")