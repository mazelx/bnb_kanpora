from bnb_kanpora.config import Config, MODELS

class DBUtils:
    def __init__(self, config:Config) -> None:
        self.database = config.database

    @property
    def db(self):
        self.database

    def drop_tables(self):
        with self.database:
            self.database.drop_tables(MODELS)
        return True

    def check_connection(self):
        try:
            self.database.connect()
            return True
        except:
            return False

