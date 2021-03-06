from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = "#89DA59"

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id="",
        tables=[],
        *args,
        **kwargs,
    ):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)

        # Check that none of the tables are empty
        for table in self.tables:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")

            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(
                    f"Data quality check failed. {table} returned no results"
                )
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            self.log.info(
                f"Data quality on table {table} check passed with {records[0][0]} records"
            )

            # Check if the city names are all upper caps
            column_name = {
                "demographics": "city",
                "immigration": "arrived_city",
                "airports": "city",
            }

            self.log.info(f"yugesh Checking quality for {table}")

            record = redshift_hook.get_records(
                f"SELECT {column_name[table]} FROM {table}"
            )
            rec = 0
            for i in range(len(record)):
                if (
                    record[i][0] is None
                    or str(record[i][0]) == "None"
                    or len(record[i][0]) == 0
                ):
                    continue

                if str(record[i][0]).isupper() is False:
                    raise ValueError(
                        f"Data quality check failed in no upper case record {record[i][0]}"
                    )
                rec += 1
