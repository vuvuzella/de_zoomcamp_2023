from argparse import ArgumentParser, Namespace
from os import getenv, path


class CLI:
    def __init__(self) -> None:
        self.parser = ArgumentParser()
        self.parser.add_argument(
            "--username",
            dest="username",
            type=str,
            help="Enter database username",
            required=False,
        )
        self.parser.add_argument(
            "--password",
            dest="password",
            type=str,
            help="Enter database password",
            required=False,
        )
        self.parser.add_argument(
            "--host",
            dest="host",
            type=str,
            help="Hostname of the database to use",
            required=False,
        )
        self.parser.add_argument(
            "--port",
            dest="port",
            type=str,
            help="Port of the database to use",
            required=False,
        )
        self.parser.add_argument(
            "--db_name",
            dest="db_name",
            type=str,
            help="Name of the database to use",
            required=False,
        )

        self.args = self.parser.parse_args()


class DBConfig:
    """
    Gets config parameters from environment variables to be passed to the pipeline
    username
    password
    host
    db_name
    port
    """

    def __init__(self, args: Namespace = Namespace()) -> None:
        self.args = args

    @property
    def username(self) -> str | None:
        return vars(self.args).get("username") or getenv("DB_USERNAME")

    @property
    def password(self) -> str | None:
        return vars(self.args).get("password") or getenv("DB_PASSWORD")

    @property
    def host(self) -> str | None:
        return vars(self.args).get("host") or getenv("DB_HOST")

    @property
    def port(self) -> str | None:
        return vars(self.args).get("port") or getenv("DB_PORT")

    @property
    def db_name(self) -> str | None:
        return vars(self.args).get("db_name") or getenv("DB")
