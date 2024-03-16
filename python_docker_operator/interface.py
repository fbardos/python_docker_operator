from __future__ import annotations

import datetime as dt
import logging
import os
from abc import abstractmethod
from enum import Enum
from typing import Any, Dict

from airflow.models import Connection
from airflow.utils.context import Context
from pymongo import MongoClient
from pymongo.database import Database
from redis import Redis
from sqlalchemy.engine import URL, Engine
from sqlmodel import create_engine


# TODO: Extend later to be used by all other parameters who needs to be passed by CustomDockerOperator
class EnvironmentInterface:
    ENV_PREFIX: str
    ENV_DELIMITER = '__'
    _env_middle: str
    
    def env_name(self, variable_name: str) -> str:
        return self.ENV_DELIMITER.join([self.ENV_PREFIX, self._env_middle.upper(), variable_name.upper()])
    
    def read_env(self, variable_name: str) -> str:
        env_name = self.env_name(variable_name)
        logging.debug(f'Looking for environment variable {env_name}')
        env = os.environ.get(env_name, None)
        if env:
            logging.debug(f'Found environment variable {env_name}')
            return env
        else:
            raise ValueError(f'No environment variable {env_name} found')

    def generate_env(self, variable_name: str, value: Any) -> Dict[str, str]:
        env_name = self.env_name(variable_name)
        return {env_name: str(value)}


class ContextParam(Enum):
    DATA_INTERVAL_START = 'data_interval_start'
    DATA_INTERVAL_END = 'data_interval_end'


class ContextInterface(EnvironmentInterface):
    ENV_PREFIX = 'AIRFLOW_CONTEXT'

    def __init__(self):
        self._env_middle = 'CONTEXT'

    def with_context(self, context: Context) -> ContextInterface:
        """Can only work when executed inside DAG context."""
        self._context = context
        return self

    @property
    def env_data_interval_start(self) -> dt.datetime:
        return dt.datetime.fromisoformat(self.read_env(ContextParam.DATA_INTERVAL_START.name))

    @property
    def env_data_interval_end(self) -> dt.datetime:
        return dt.datetime.fromisoformat(self.read_env(ContextParam.DATA_INTERVAL_END.name))

    @property
    def dict_data_interval_start(self) -> Dict[str, str]:
        return self.generate_env(ContextParam.DATA_INTERVAL_START.name, self._context['data_interval_start'])

    @property
    def dict_data_interval_end(self) -> Dict[str, str]:
        return self.generate_env(ContextParam.DATA_INTERVAL_END.name, self._context['data_interval_end'])
    
    def dict_all(self, context: Context) -> Dict[str, str]:
        return {
            **self.with_context(context).dict_data_interval_start,
            **self.with_context(context).dict_data_interval_end,
        }


class ConnectionParam(Enum):
    HOST = 'host'
    PORT = 'port'
    LOGIN = 'login'
    PASSWORD = 'password'
    SCHEMA = 'schema'
    EXTRA = 'extra'


class ConnectionInterface(EnvironmentInterface):
    """Stores connection variables and can generate env variables for docker container"""
    ENV_PREFIX = 'AIRFLOW_CONN'

    def __init__(self, connection_id: str):
        self.connection_id = connection_id
        self._env_middle = connection_id

    @property
    def connection(self) -> Connection:
        """Can only work when executed inside DAG context."""
        return Connection.get_connection_from_secrets(self.connection_id)

    @property
    def env_host(self) -> str:
        return self.read_env(ConnectionParam.HOST.name)

    @property
    def env_port(self) -> int:
        return int(self.read_env(ConnectionParam.PORT.name))

    @property
    def env_login(self) -> str:
        return self.read_env(ConnectionParam.LOGIN.name)

    @property
    def env_password(self) -> str:
        return self.read_env(ConnectionParam.PASSWORD.name)

    @property
    def env_schema(self) -> str:
        return self.read_env(ConnectionParam.SCHEMA.name)

    @property
    def env_extra(self) -> str:
        return self.read_env(ConnectionParam.EXTRA.name)

    @property
    def dict_host(self) -> Dict[str, str]:
        return self.generate_env(ConnectionParam.HOST.name, self.connection.host)

    @property
    def dict_port(self) -> Dict[str, str]:
        return self.generate_env(ConnectionParam.PORT.name, self.connection.port)

    @property
    def dict_login(self) -> Dict[str, str]:
        return self.generate_env(ConnectionParam.LOGIN.name, self.connection.login)

    @property
    def dict_password(self) -> Dict[str, str]:
        return self.generate_env(ConnectionParam.PASSWORD.name, self.connection.password)

    @property
    def dict_schema(self) -> Dict[str, str]:
        return self.generate_env(ConnectionParam.SCHEMA.name, self.connection.schema)

    @property
    def dict_extra(self) -> Dict[str, str]:
        return self.generate_env(ConnectionParam.EXTRA.name, self.connection.extra)

    @property
    def dict_all(self) -> Dict[str, str]:
        return {
            **self.dict_host,
            **self.dict_port,
            **self.dict_login,
            **self.dict_password,
            **self.dict_schema,
            **self.dict_extra,
        }

    @property
    def sqlalchemy_engine(self) -> Engine:
        url = URL.create(
            'postgresql+psycopg2',
            username=self.env_login,
            password=self.env_password,
            host=self.env_host,
            port=self.env_port,
            database=self.env_schema,
        )
        return create_engine(url)

    @property
    def mongodb_collection(self) -> Database:
        connection = MongoClient(
            host=self.env_host,
            port=self.env_port,
            username=self.env_login,
            password=self.env_password,
        )
        return connection.get_database(self.env_schema)

    @property
    def redis_connection(self) -> Redis:
        return Redis(
            host=self.env_host,
            port=self.env_port,
            password=self.env_password,
        )

