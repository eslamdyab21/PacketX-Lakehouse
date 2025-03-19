from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, IntegerType, StringType, FloatType, TimestampType
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform
import logging
import os



def create_raw_schema(catalog, name_space, table_name):
    logging.info(f"""create_raw_schema""")

    catalog.create_namespace_if_not_exists(name_space)

    schema = Schema(
        NestedField(field_id=1, name='user_id', field_type = IntegerType(), required=False),
        NestedField(field_id=2, name='time_stamp', field_type = TimestampType(), required=True),
        NestedField(field_id=3, name='source_ip', field_type = StringType(), required=True),
        NestedField(field_id=4, name='destination_ip', field_type = StringType(), required=True),
        NestedField(field_id=5, name='bandwidth_kb', field_type = FloatType(), required=True)
    )

    partition_spec = PartitionSpec(
        PartitionField(
            source_id=2,
            field_id=1000,
            transform=DayTransform(),
            name="partition_date"
        )
    )

    iceberg_table = catalog.create_table_if_not_exists(
        identifier=f'{name_space}.{table_name}',
        schema=schema,
        partition_spec=partition_spec
    )

    logging.info(f"""create_raw_schema -> iceberg_table.schema()""")
    print(iceberg_table.schema())
    logging.info(f"""create_raw_schema -> Done""")



def load_local_sqlite_catalog():
    logging.info(f"""load_local_sqlite_catalog""")

    os.environ['PYICEBERG_HOME'] = os.getcwd()
    catalog = load_catalog(name='lakehouse')


    logging.info(f"""load_local_sqlite_catalog -> Done""")
    return catalog



if __name__ == "__main__":
    logging.basicConfig(level = "INFO")
    logging.info(f"""Main""")

    catalog = load_local_sqlite_catalog()

    create_raw_schema(catalog = catalog, name_space = 'PacketX_Raw', table_name = 'Packets')

    logging.info(f"""Main -> Done""")