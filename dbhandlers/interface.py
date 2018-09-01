"""
Abstract base class defining the interface
and type checking for various databases.
"""

from abc import ABC, abstractmethod


class DbHandler(ABC):

    @abstractmethod
    def __init__(self, conn_str):
        self.conn_str = conn_str

    @property
    @abstractmethod
    def connection(self):
        """
        The database connection object.
        """

    @abstractmethod
    def read(self):
        """
        A read action against the database,
        this might be a SELECT query for a SQL
        database.
        """

    @abstractmethod
    def write(self):
        """
        A write action against the database,
        this might be an INSERT query for a SQL
        database.
        """



