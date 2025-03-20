from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, IntegerType, StringType, FloatType, TimestampType
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform
import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.compute as pc
import logging
import os



def read_csv_file(csv_file_path):
    logging.info(f"""read_csv_file""")
    

    arrow_schema = pa.schema(
        [
            pa.field("user", pa.string(), nullable=False),
            pa.field("time_stamp", pa.timestamp('us'), nullable=False),
            pa.field("source_ip", pa.string(), nullable=False),
            pa.field("destination_ip", pa.string(), nullable=False),
            pa.field("bandwidth_kb", pa.float32(), nullable=False),
            pa.field("total_bandwidth_kb", pa.float32(), nullable=True),
        ]
    )


    read_options = pcsv.ReadOptions(
        column_names=arrow_schema.names,
        skip_rows=1
    )
    convert_options = pcsv.ConvertOptions(
        column_types=arrow_schema,
        strings_can_be_null=True,
        null_values=["", "NULL"],
    )
    df = pcsv.read_csv(csv_file_path, convert_options = convert_options, read_options = read_options)

    df = df.drop(["total_bandwidth_kb"])
    

    logging.info(f"""read_csv_file -> Done""")

    return df



def add_id_column(df):
    logging.info(f"""add_id_column""")

    num_rows = df.num_rows
    row_numbers = pa.array(range(1, num_rows + 1), type=pa.int32())

    # Convert time_stamp to string using strftime
    time_strings = pc.strftime(df["time_stamp"], format="%Y-%m-%d %H:%M:%S")

    # Convert row numbers to strings
    row_number_strings = pc.cast(row_numbers, pa.string())

    # Concatenate user, time_stamp, and row number with a separator
    user_array = df["user"]
    combined = pc.binary_join_element_wise(
        user_array, time_strings, row_number_strings,
        "-"
    )

    # Updated schema with the new column
    new_schema = pa.schema(
        [
            pa.field("user", pa.string(), nullable=False),
            pa.field("time_stamp", pa.timestamp('us'), nullable=False),
            pa.field("source_ip", pa.string(), nullable=False),
            pa.field("destination_ip", pa.string(), nullable=False),
            pa.field("bandwidth_kb", pa.float32(), nullable=False),
            pa.field("id", pa.string(), nullable=False),  # New id column
        ]
    )

    logging.info(f"""add_id_column -> Done""")
    return df.append_column("id", combined).cast(new_schema)



def upsert_new_df(df, iceberg_table):
    logging.info(f"""upsert_new_df""")

    df = add_id_column(df)
    iceberg_table.upsert(df, join_cols=["id"])
    # iceberg_table.append(df)

    
    logging.info(f"""upsert_new_df -> Done""")



def create_raw_schema(catalog, name_space, table_name):
    logging.info(f"""create_raw_schema""")

    catalog.create_namespace_if_not_exists(name_space)

    if catalog.table_exists(f"{name_space}.{table_name}"):
        logging.info(f"""create_raw_schema -> Table <{table_name}> Already Created Before""")
        logging.info(f"""create_raw_schema -> Done""")
        return

    
    schema = Schema(
        NestedField(field_id=1, name='user', field_type = StringType(), required=False),
        NestedField(field_id=2, name='time_stamp', field_type = TimestampType(), required=True),
        NestedField(field_id=3, name='source_ip', field_type = StringType(), required=True),
        NestedField(field_id=4, name='destination_ip', field_type = StringType(), required=True),
        NestedField(field_id=5, name='bandwidth_kb', field_type = FloatType(), required=True),
        NestedField(field_id=6, name='id', field_type = StringType(), required=True),

        identifier_field_ids=[6]  # Primary-key, Pyiceberg will use it in upsert
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

    # logging.info(f"""create_raw_schema -> iceberg_table.schema()""")
    # print(iceberg_table.schema())
    logging.info(f"""create_raw_schema -> Done""")



def load_local_sqlite_catalog():
    logging.info(f"""load_local_sqlite_catalog""")


    catalog = load_catalog('lakehouse', **{
        'uri': 'sqlite:///iceberg_catalog/catalog.db',
        'warehouse': 'file://iceberg_catalog'
    })
    os.environ['PYICEBERG_HOME'] = os.getcwd()


    logging.info(f"""load_local_sqlite_catalog -> Done""")
    return catalog



if __name__ == "__main__":
    logging.basicConfig(level = "INFO")
    logging.info(f"""Main""")

    catalog = load_local_sqlite_catalog()

    create_raw_schema(catalog = catalog, name_space = 'PacketX_Raw', table_name = 'Packets')
    iceberg_table = catalog.load_table("PacketX_Raw.Packets")

    df = read_csv_file('/home/dyab/projects/PacketX/traffic_log/2025-03-20.gz')
    upsert_new_df(df, iceberg_table)
    
    logging.info(f"""Main -> Done""")