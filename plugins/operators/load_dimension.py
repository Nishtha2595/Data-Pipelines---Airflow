from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table = "",
                 sql = "",  
                 append_only = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.append_only = append_only

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate_table:
            self.log.info(f"Truncating dimension table: {self.table}")
            redshift.run(LoadDimensionOperator.truncate_sql.format(self.table))

        self.log.info(f"Loading dimension table {self.table}")
        formatted_sql = LoadDimensionOperator.insert_sql.format(
            self.table,
            self.load_sql_stmt
        )
        
        self.log.info(f"Executing Query: {formatted_sql}")
        redshift.run(formatted_sql)