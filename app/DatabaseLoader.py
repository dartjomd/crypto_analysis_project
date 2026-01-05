import os
from dotenv import load_dotenv

from sqlalchemy import Engine, Table, create_engine, text
from pandas import DataFrame
from sqlalchemy.engine.base import Connection


load_dotenv()


class DatabaseLoader:
    """
    Base class for managing MySQL connection and data Loading
    """

    def __init__(self):
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")
        host = os.getenv("DB_HOST")
        port = os.getenv("INTERNAL_DB_PORT")
        name = os.getenv("DB_NAME")

        # define connection string for db
        self.connection_string: str = (
            f"mysql+pymysql://{user}:{password}@{host}:{port}/{name}"
        )
        # define engine for db
        self.engine: Engine | None = None
        self._init_engine()

    def _init_engine(self):
        """
        Initialize SQLAlchemy engine
        """
        try:
            self.engine = create_engine(self.connection_string)

            self._test_db_initialization()
            print("Success. DB engine has been created.")

        except Exception as e:
            print(f"Unable to create SQLAlchemy engine: {e}")
            self.engine = None
            raise ConnectionError(f"Unable to connect to BD: {e}")

    def _test_db_initialization(self):
        """
        Test DB connection
        """
        with self.engine.connect() as connection:
            result = connection.execute(text("SELECT 1 + 1")).fetchone()
            if result and result[0] == 2:
                print(
                    f"Success. MySQL is working correctly. {self.engine.url.host}:{self.engine.url.port}"
                )

    def execute_query(self, query: str):
        """
        Execute custom MYSQL query and return result

        :param query: query to execute
        :type query: str
        """

        try:
            with self.engine.connect() as connection:
                result = connection.execute(text(query)).fetchall()
                if result:
                    return result
        except Exception as e:
            print(f"Error. Unable to execute SQL query: {e}")
            return []

    def load_dataframe(
        self,
        df: DataFrame,
        table_name: str,
    ):
        """
        Load Pandas DataFrame into MySQL table

        :param df: DataFrame to load.
        :param table_name: table name where to load.
        :param if_exists: action when table already exists.
        """

        if not self.engine:
            print("Engine is not initialized")
            return

        data_to_insert = df.to_dict(orient="records")
        keys = list(df.columns)

        cols = ", ".join(f"`{k}`" for k in keys)
        placeholders = ", ".join([f":{k}" for k in keys])

        sql = f"""
            INSERT IGNORE INTO `{table_name}` ({cols}) 
            VALUES ({placeholders})
        """

        try:
            with self.engine.connect() as conn:
                conn.execute(text(sql), data_to_insert)
                conn.commit()

            print(f"Success. Loaded {len(df)} records into {table_name}")

        except Exception as e:
            print(f"Error while loading DataFrame into table. {e}")
