import asyncio
import sys

from .utils import LemkPgUtils
from .exceptions import LemkPgError
from .constants import JOINS_LIST


class LemkPgApi:
    """
    LemkPgApi class give API interface for quick access to PostgreSQL DB via async way.
    You can use CRUD and other DB operations with some methods above.
    This DB API will be work only with Python 3.5 and greater versions.
    """

    def __init__(self, db_name: str, db_user: str, db_password: str, db_host: str, *args, **kwargs):
        """
        You can create db_connect of LemkPgApi when you define all required attrs.
        Example of db_connect creation:
        >>> db_conn = LemkPgApi(db_name="demo_db", db_password="pass", db_user="postgres", db_host="127.0.0.1")

        :param db_name: string with name of the database
        :param db_user: string with user name
        :param db_password: string with password for selected user
        :param db_host: string with database host
        :param args: additional attr
        :param kwargs: additional attr
        """
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.db_host = db_host
        self.dsn = f"dbname={self.db_name} user={self.db_user} password={self.db_password} host={self.db_host}"

    def _run_async(self, func):
        # check python version
        if sys.version_info[1] < 7:
            # if python3 version less then python3.7 - run this case
            return asyncio.get_event_loop().run_until_complete(func)
        else:
            # if python3 version equal or greater then python3.7 - run this case
            return asyncio.run(func)

    def create_table(self, table_name: str, fields: dict, primary_key=False):
        """
        >>> db_conn.create_table("demo", {"id": "integer", "date": "text", "trans": "text", "symbol": "text"})

        :param table_name: string with table name
        :param fields: dict with new fields and their types (key - field name, value - type)
        :param  primary_key: bool value - default False - if True add autoincrement primary key
        :return: True if query success
        """

        async def func():
            new_fields = [f"{field[0]} {field[1]}" for field in fields.items()]
            if not primary_key:
                query = f"""CREATE TABLE IF NOT EXISTS {table_name} ({", ".join(new_fields)})"""
            else:
                query = f"""CREATE TABLE IF NOT EXISTS {table_name} (id SERIAL PRIMARY KEY, {", ".join(new_fields)})"""
            await LemkPgUtils.execute_query(self.dsn, query)
            return True

        return self._run_async(func())

    def insert(self, table_name: str, values: tuple, columns=None):
        """
        >>> db_conn.insert("demo", (1, '2006-01-05', 'Some Text', 'A'))

        :param table_name: string with table name
        :param values: tuple with values
        :param columns: None or tuple with columns
        :return: True if query success
        """

        async def func():
            query = f"""INSERT INTO {table_name} {'(' + ', '.join(columns) + ')' if columns else ''} VALUES {values}"""
            await LemkPgUtils.execute_query(self.dsn, query)
            return True

        return self._run_async(func())

    def get_all(self, table_name: str, order_by=None, sort_type=None):
        """
        >>> db_conn.get_all("demo")

        :param table_name: string with table name
        :param order_by: string with column for ordering
        :param sort_type: string with type of ordering (ASC / DESC)
        :return: result if query success
        """

        async def func():
            sort = f"{' ORDER BY ' + order_by + ' ' + sort_type if order_by and sort_type else ''}"
            query = f"""SELECT * FROM {table_name}{sort}"""
            result = await LemkPgUtils.get_query_result(self.dsn, query)
            return result

        return self._run_async(func())

    def get(self, table_name: str, fields: list, conditions_list=None, distinct=False, order_by=None, sort_type=None):
        """
        >>> db_conn.get("demo", ["date", "symbol"], conditions_list=[("date", "=", "2006-01-05", None)], distinct=True)

        :param table_name: string with table name
        :param fields: list with fields for selection
        :param conditions_list: list with tuples with conditions in it. In each tuple should be defined four values:
                 1) column for assert in WHERE clause (e.g. "date")
                 2) operand for assert column (e.g. "=", or "!=")
                 3) value for assert (e.g. "2006-01-05")
                 4) additional value if you need more then one conditions in where clause.
                    if one tuple in list - this value should be None. If more then one tuple in conditions_list -
                    this value should be string (e.g. "AND", or "OR")
        :param distinct: bool value. Default False. If True - get unique records
        :param order_by: string with column for ordering
        :param sort_type: string with type of ordering (ASC / DESC)
        :return: result if query success
        """

        async def func():
            dist = f"{'DISTINCT ' if distinct else ''}"
            sort = f"{' ORDER BY ' + order_by + ' ' + sort_type if order_by and sort_type else ''}"
            if conditions_list:
                conditions = LemkPgUtils.get_conditions(conditions_list)
                query = f"""SELECT {dist}{", ".join(fields)} FROM {table_name} WHERE {" ".join(
                    conditions)}{sort}"""
            else:
                query = f"""SELECT {dist}{", ".join(fields)} FROM {table_name}{sort}"""
            result = await LemkPgUtils.get_query_result(self.dsn, query)
            return result

        return self._run_async(func())

    def update(self, table_name: str, fields: dict, conditions_list=None):
        """
        >>> db_conn.update("demo", {"date": "2005-01-05", "symbol": "Adc"}, [("date", "=", "2006-01-05", None),
                                                                             ("symbol", "=", "A", "OR")])

        :param table_name: string with table name
        :param fields: dict with column name and their new value (key - column name, value - new column value)
        :param conditions_list: list with tuples with conditions in it. In each tuple should be defined four values:
                 1) column for assert in WHERE clause (e.g. "date")
                 2) operand for assert column (e.g. "=", or "!=")
                 3) value for assert (e.g. "2006-01-05")
                 4) additional value if you need more then one conditions in where clause.
                    if one tuple in list - this value should be None. If more then one tuple in conditions_list -
                    this value should be string (e.g. "AND", or "OR")
        :return: result if query success
        """

        async def func():
            columns_for_update = [f"{field[0]} = '{field[1]}'" for field in fields.items()]
            if conditions_list:
                conditions = LemkPgUtils.get_conditions(conditions_list)
                query = f"""UPDATE {table_name} SET {", ".join(columns_for_update)} WHERE {" ".join(
                    conditions)}"""
            else:
                query = f"""UPDATE {table_name} SET {", ".join(columns_for_update)}"""
            result = await LemkPgUtils.execute_query(self.dsn, query)
            return result

        return self._run_async(func())

    def alter_table(self, table_name: str, column_name: str, action: str, column_type=None):
        """
        >>> db_conn.alter_table("demo", "date", "ALTER COLUMN", column_type="varchar")

        :param table_name: string with table name
        :param column_name: string with column name
        :param action: string with action (e.g. "ALTER COLUMN" or  "DROP COLUMN")
        :param column_type: additional attr with new column type (e.g. "varchar")
        :return: result if query success
        """

        async def func():
            query = (f"""ALTER TABLE {table_name} {action} {column_name}"""
                     f"""{' TYPE ' + column_type if column_type else ''}""")
            result = await LemkPgUtils.execute_query(self.dsn, query)
            return result

        return self._run_async(func())

    def raw_query(self, query: str):
        """
        >>> db_conn.raw_query("SELECT * FROM demo INNER JOIN datatable ON demo.trans = datatable.trans")

        :param query: string with query for manual execution
        :return: result if query success
        """

        async def func():
            result = await LemkPgUtils.get_query_result(self.dsn, query)
            return result

        return self._run_async(func())

    def get_with_join(self, table_name: str, join_table_name: str, join_type: str,
                      fields: list, on_condition: tuple, where_conditions_list=None):
        """
        >>> db_conn.get_with_join("demo", "datatable", "INNER JOIN", ["*"], ("demo.trans", "=", "datatable.trans"))

        :param table_name: string with table name
        :param join_table_name: string with joins table name
        :param join_type: string with join type (e.g. "INNER JOIN", or "FULL OUTER JOIN")
        :param fields: list with strings with columns names in it (e.g. "["trans", "date"], or ["*"] if all columns)"
        :param on_condition: tuple with condition. In tuple should be defined three values:
                 1) column for assert in ON clause (e.g. "demo.trans")
                 2) operand for assert column (e.g. "=", or "!=")
                 3) value for assert from joins table (e.g. "datatable.trans")
        :param where_conditions_list: list with tuples with conditions in it. In each tuple should be defined four values:
                 1) column for assert in WHERE clause (e.g. "date")
                 2) operand for assert column (e.g. "=", or "!=")
                 3) value for assert (e.g. "2006-01-05")
                 4) additional value if you need more then one conditions in where clause.
                    if one tuple in list - this value should be None. If more then one tuple in conditions_list -
                    this value should be string (e.g. "AND", or "OR")
        :return: result if query success
        """

        async def func():
            if join_type not in JOINS_LIST:
                message = f"Incorrect JOIN type. Please use one of the valid JOIN types: {', '.join(JOINS_LIST)}"
                raise LemkPgError(message)

            if where_conditions_list:
                conditions = LemkPgUtils.get_conditions(where_conditions_list)
                query = (f"""SELECT {", ".join(fields)} FROM {table_name} {join_type} {join_table_name}"""
                         f""" ON {on_condition[0]} {on_condition[1]} {on_condition[2]}"""
                         f""" WHERE {" ".join(conditions)}""")
            else:
                query = (f"""SELECT {", ".join(fields)} FROM {table_name} {join_type} {join_table_name}"""
                         f""" ON {on_condition[0]} {on_condition[1]} {on_condition[2]}""")
            result = await LemkPgUtils.get_query_result(self.dsn, query)
            return result

        return self._run_async(func())

    def inner_join(self, table_name: str, join_table_name: str,
                   on_condition: tuple, where_conditions_list=None, fields=None, all=True):
        """
        >>> db_conn.inner_join("demo", "datatable", ("demo.trans", "=", "datatable.trans"))

        :param table_name: string with table name
        :param join_table_name: string with joins table name
        :param on_condition: tuple with condition. In tuple should be defined three values:
                 1) column for assert in ON clause (e.g. "demo.trans")
                 2) operand for assert column (e.g. "=", or "!=")
                 3) value for assert from joins table (e.g. "datatable.trans")
        :param where_conditions_list: list with tuples with conditions in it. In each tuple should be defined four values:
                 1) column for assert in WHERE clause (e.g. "date")
                 2) operand for assert column (e.g. "=", or "!=")
                 3) value for assert (e.g. "2006-01-05")
                 4) additional value if you need more then one conditions in where clause.
                    if one tuple in list - this value should be None. If more then one tuple in conditions_list -
                    this value should be string (e.g. "AND", or "OR")
        :param fields: list with strings with columns names in it
         (e.g. "["trans", "date"]. Default None (get all columns -  if param all is True)"
        :param all: bool param - for check do we need all fields or not. Default - True (get all fields)
        :return: result if query success
        """

        async def func():
            query_fields = ("*" if not fields and all else f'{", ".join(fields)}')
            if where_conditions_list:
                conditions = LemkPgUtils.get_conditions(where_conditions_list)
                query = (f"""SELECT {query_fields} FROM {table_name} INNER JOIN {join_table_name}"""
                         f""" ON {on_condition[0]} {on_condition[1]} {on_condition[2]}"""
                         f""" WHERE {" ".join(conditions)}""")
            else:
                query = (f"""SELECT {query_fields} FROM {table_name} INNER JOIN {join_table_name}"""
                         f""" ON {on_condition[0]} {on_condition[1]} {on_condition[2]}""")
            result = await LemkPgUtils.get_query_result(self.dsn, query)
            return result

        return self._run_async(func())

    def left_join(self, table_name: str, join_table_name: str,
                  on_condition: tuple, where_conditions_list=None, fields=None, all=True):
        """
        >>> db_conn.left_join("demo", "datatable", ("demo.trans", "=", "datatable.trans"))

        :param table_name: string with table name
        :param join_table_name: string with joins table name
        :param on_condition: tuple with condition. In tuple should be defined three values:
                 1) column for assert in ON clause (e.g. "demo.trans")
                 2) operand for assert column (e.g. "=", or "!=")
                 3) value for assert from joins table (e.g. "datatable.trans")
        :param where_conditions_list: list with tuples with conditions in it. In each tuple should be defined four values:
                 1) column for assert in WHERE clause (e.g. "date")
                 2) operand for assert column (e.g. "=", or "!=")
                 3) value for assert (e.g. "2006-01-05")
                 4) additional value if you need more then one conditions in where clause.
                    if one tuple in list - this value should be None. If more then one tuple in conditions_list -
                    this value should be string (e.g. "AND", or "OR")
        :param fields: list with strings with columns names in it
         (e.g. "["trans", "date"]. Default None (get all columns -  if param all is True)"
        :param all: bool param - for check do we need all fields or not. Default - True (get all fields)
        :return: result if query success
        """

        async def func():
            query_fields = ("*" if not fields and all else f'{", ".join(fields)}')
            if where_conditions_list:
                conditions = LemkPgUtils.get_conditions(where_conditions_list)
                query = (f"""SELECT {query_fields} FROM {table_name} LEFT JOIN {join_table_name}"""
                         f""" ON {on_condition[0]} {on_condition[1]} {on_condition[2]}"""
                         f""" WHERE {" ".join(conditions)}""")
            else:
                query = (f"""SELECT {query_fields} FROM {table_name} LEFT JOIN {join_table_name}"""
                         f""" ON {on_condition[0]} {on_condition[1]} {on_condition[2]}""")
            result = await LemkPgUtils.get_query_result(self.dsn, query)
            return result

        return self._run_async(func())

    def right_join(self, table_name: str, join_table_name: str,
                   on_condition: tuple, where_conditions_list=None, fields=None, all=True):
        """
        >>> db_conn.right_join("demo", "datatable", ("demo.trans", "=", "datatable.trans"))

        :param table_name: string with table name
        :param join_table_name: string with joins table name
        :param on_condition: tuple with condition. In tuple should be defined three values:
                 1) column for assert in ON clause (e.g. "demo.trans")
                 2) operand for assert column (e.g. "=", or "!=")
                 3) value for assert from joins table (e.g. "datatable.trans")
        :param where_conditions_list: list with tuples with conditions in it. In each tuple should be defined four values:
                 1) column for assert in WHERE clause (e.g. "date")
                 2) operand for assert column (e.g. "=", or "!=")
                 3) value for assert (e.g. "2006-01-05")
                 4) additional value if you need more then one conditions in where clause.
                    if one tuple in list - this value should be None. If more then one tuple in conditions_list -
                    this value should be string (e.g. "AND", or "OR")
        :param fields: list with strings with columns names in it
         (e.g. "["trans", "date"]. Default None (get all columns -  if param all is True)"
        :param all: bool param - for check do we need all fields or not. Default - True (get all fields)
        :return: result if query success
        """

        async def func():
            query_fields = ("*" if not fields and all else f'{", ".join(fields)}')
            if where_conditions_list:
                conditions = LemkPgUtils.get_conditions(where_conditions_list)
                query = (f"""SELECT {query_fields} FROM {table_name} RIGHT JOIN {join_table_name}"""
                         f""" ON {on_condition[0]} {on_condition[1]} {on_condition[2]}"""
                         f""" WHERE {" ".join(conditions)}""")
            else:
                query = (f"""SELECT {query_fields} FROM {table_name} RIGHT JOIN {join_table_name}"""
                         f""" ON {on_condition[0]} {on_condition[1]} {on_condition[2]}""")
            result = await LemkPgUtils.get_query_result(self.dsn, query)
            return result

        return self._run_async(func())

    def full_join(self, table_name: str, join_table_name: str,
                  on_condition: tuple, where_conditions_list=None, fields=None, all=True):
        """
        >>> db_conn.full_join("demo", "datatable", ("demo.trans", "=", "datatable.trans"))

        :param table_name: string with table name
        :param join_table_name: string with joins table name
        :param on_condition: tuple with condition. In tuple should be defined three values:
                 1) column for assert in ON clause (e.g. "demo.trans")
                 2) operand for assert column (e.g. "=", or "!=")
                 3) value for assert from joins table (e.g. "datatable.trans")
        :param where_conditions_list: list with tuples with conditions in it. In each tuple should be defined four values:
                 1) column for assert in WHERE clause (e.g. "date")
                 2) operand for assert column (e.g. "=", or "!=")
                 3) value for assert (e.g. "2006-01-05")
                 4) additional value if you need more then one conditions in where clause.
                    if one tuple in list - this value should be None. If more then one tuple in conditions_list -
                    this value should be string (e.g. "AND", or "OR")
        :param fields: list with strings with columns names in it
         (e.g. "["trans", "date"]. Default None (get all columns -  if param all is True)"
        :param all: bool param - for check do we need all fields or not. Default - True (get all fields)
        :return: result if query success
        """

        async def func():
            query_fields = ("*" if not fields and all else f'{", ".join(fields)}')
            if where_conditions_list:
                conditions = LemkPgUtils.get_conditions(where_conditions_list)
                query = (f"""SELECT {query_fields} FROM {table_name} FULL OUTER JOIN {join_table_name}"""
                         f""" ON {on_condition[0]} {on_condition[1]} {on_condition[2]}"""
                         f""" WHERE {" ".join(conditions)}""")
            else:
                query = (f"""SELECT {query_fields} FROM {table_name} FULL OUTER JOIN {join_table_name}"""
                         f""" ON {on_condition[0]} {on_condition[1]} {on_condition[2]}""")
            result = await LemkPgUtils.get_query_result(self.dsn, query)
            return result

        return self._run_async(func())

    def delete_table(self, table_name: str):
        """
        >>> db_conn.delete_table("demo")

        :param table_name: string with table name
        :return: True if query success
        """

        async def func():
            query = f"""DROP TABLE IF EXISTS {table_name}"""
            await LemkPgUtils.execute_query(self.dsn, query)
            return True

        return self.run_async(func())

    def clear_table(self, table_name: str):
        """
        >>> db_conn.clear_table("demo")

        :param table_name: string with table name
        :return: True if query success
        """

        async def func():
            query = f"""TRUNCATE TABLE {table_name}"""
            await LemkPgUtils.execute_query(self.dsn, query)
            return True

        return self._run_async(func())

    def delete_records(self, table_name: str, conditions_list=None):
        """
        >>> db_conn.delete_records("demo", [("date", "=", "2006-01-05", None), ("symbol", "=", "A", "OR")])

        :param table_name: string with table name
        :param conditions_list: list with tuples with conditions in it. In each tuple should be defined four values:
                 1) column for assert in WHERE clause (e.g. "date")
                 2) operand for assert column (e.g. "=", or "!=")
                 3) value for assert (e.g. "2006-01-05")
                 4) additional value if you need more then one conditions in where clause.
                    if one tuple in list - this value should be None. If more then one tuple in conditions_list -
                    this value should be string (e.g. "AND", or "OR")
        :return: True if query success
        """

        async def func():
            if conditions_list:
                conditions = LemkPgUtils.get_conditions(conditions_list)
                query = f"""DELETE FROM {table_name} WHERE {" ".join(conditions)}"""
            else:
                query = f"""DELETE FROM {table_name}"""
            await LemkPgUtils.execute_query(self.dsn, query)
            return True

        return self._run_async(func())

    def count(self, table_name: str, column: str, conditions_list=None):
        """
        >>> db_conn.count("demo", "date")

        :param table_name: string with table name
        :param column: string with column name
        :param conditions_list: list with tuples with conditions in it. In each tuple should be defined four values:
                 1) column for assert in WHERE clause (e.g. "date")
                 2) operand for assert column (e.g. "=", or "!=")
                 3) value for assert (e.g. "2006-01-05")
                 4) additional value if you need more then one conditions in where clause.
                    if one tuple in list - this value should be None. If more then one tuple in conditions_list -
                    this value should be string (e.g. "AND", or "OR")
        :return: result if query success
        """

        async def func():
            if conditions_list:
                conditions = LemkPgUtils.get_conditions(conditions_list)
                query = f"""SELECT COUNT({column}) FROM {table_name} WHERE {" ".join(conditions)}"""
            else:
                query = f"""SELECT COUNT({column}) FROM {table_name}"""
            result = await LemkPgUtils.get_query_result(self.dsn, query)
            return result

        return self._run_async(func())

    def avg(self, table_name: str, column: str, conditions_list=None):
        """
        >>> db_conn.avg("demo", "id")

        :param table_name: string with table name
        :param column: string with column name
        :param conditions_list: list with tuples with conditions in it. In each tuple should be defined four values:
                 1) column for assert in WHERE clause (e.g. "date")
                 2) operand for assert column (e.g. "=", or "!=")
                 3) value for assert (e.g. "2006-01-05")
                 4) additional value if you need more then one conditions in where clause.
                    if one tuple in list - this value should be None. If more then one tuple in conditions_list -
                    this value should be string (e.g. "AND", or "OR")
        :return: result if query success
        """

        async def func():
            if conditions_list:
                conditions = LemkPgUtils.get_conditions(conditions_list)
                query = f"""SELECT AVG({column}) FROM {table_name} WHERE {" ".join(conditions)}"""
            else:
                query = f"""SELECT AVG({column}) FROM {table_name}"""
            result = await LemkPgUtils.get_query_result(self.dsn, query)
            return result

        return self._run_async(func())

    def sum(self, table_name: str, column: str, conditions_list=None):
        """
        >>> db_conn.sum("demo", "id")

        :param table_name: string with table name
        :param column: string with column name
        :param conditions_list: list with tuples with conditions in it. In each tuple should be defined four values:
                 1) column for assert in WHERE clause (e.g. "date")
                 2) operand for assert column (e.g. "=", or "!=")
                 3) value for assert (e.g. "2006-01-05")
                 4) additional value if you need more then one conditions in where clause.
                    if one tuple in list - this value should be None. If more then one tuple in conditions_list -
                    this value should be string (e.g. "AND", or "OR")
        :return: result if query success
        """

        async def func():
            if conditions_list:
                conditions = LemkPgUtils.get_conditions(conditions_list)
                query = f"""SELECT SUM({column}) FROM {table_name} WHERE {" ".join(conditions)}"""
            else:
                query = f"""SELECT SUM({column}) FROM {table_name}"""
            result = await LemkPgUtils.get_query_result(self.dsn, query)
            return result

        return self._run_async(func())

    def min(self, table_name: str, column: str, conditions_list=None):
        """
        >>> db_conn.min("demo", "date")

        :param table_name: string with table name
        :param column: string with column name
        :param conditions_list: list with tuples with conditions in it. In each tuple should be defined four values:
                 1) column for assert in WHERE clause (e.g. "date")
                 2) operand for assert column (e.g. "=", or "!=")
                 3) value for assert (e.g. "2006-01-05")
                 4) additional value if you need more then one conditions in where clause.
                    if one tuple in list - this value should be None. If more then one tuple in conditions_list -
                    this value should be string (e.g. "AND", or "OR")
        :return: result if query success
        """

        async def func():
            if conditions_list:
                conditions = LemkPgUtils.get_conditions(conditions_list)
                query = f"""SELECT MIN({column}) FROM {table_name} WHERE {" ".join(conditions)}"""
            else:
                query = f"""SELECT MIN({column}) FROM {table_name}"""
            result = await LemkPgUtils.get_query_result(self.dsn, query)
            return result

        return self._run_async(func())

    def max(self, table_name: str, column: str, conditions_list=None):
        """
        >>> db_conn.max("demo", "date")

        :param table_name: string with table name
        :param column: string with column name
        :param conditions_list: list with tuples with conditions in it. In each tuple should be defined four values:
                 1) column for assert in WHERE clause (e.g. "date")
                 2) operand for assert column (e.g. "=", or "!=")
                 3) value for assert (e.g. "2006-01-05")
                 4) additional value if you need more then one conditions in where clause.
                    if one tuple in list - this value should be None. If more then one tuple in conditions_list -
                    this value should be string (e.g. "AND", or "OR")
        :return: result if query success
        """

        async def func():
            if conditions_list:
                conditions = LemkPgUtils.get_conditions(conditions_list)
                query = f"""SELECT MAX({column}) FROM {table_name} WHERE {" ".join(conditions)}"""
            else:
                query = f"""SELECT MAX({column}) FROM {table_name}"""
            result = await LemkPgUtils.get_query_result(self.dsn, query)
            return result

        return self._run_async(func())


# AsyncVersion
class AsyncLemkPgApi:
    """
    AsyncLemkPgApi class give async API interface for quick access to PostgreSQL DB
    and return coroutine with fetching results.
    You can use CRUD and other DB operations with some methods above.
    This DB API will be work only with Python 3.5 and greater versions.
    """

    def __init__(self, db_name: str, db_user: str, db_password: str, db_host: str, *args, **kwargs):
        """
        You can create db_connect of AsyncLemkPgApi when you define all required attrs.
        Example of db_connect creation:
        >>> db_conn = AsyncLemkPgApi(db_name="demo_db", db_password="pass", db_user="postgres", db_host="127.0.0.1")

        :param db_name: string with name of the database
        :param db_user: string with user name
        :param db_password: string with password for selected user
        :param db_host: string with database host
        :param args: additional attr
        :param kwargs: additional attr
        """
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password
        self.db_host = db_host
        self.dsn = f"dbname={self.db_name} user={self.db_user} password={self.db_password} host={self.db_host}"

    async def create_table(self, table_name: str, fields: dict, primary_key=False):
        """
        >>> await db_conn.create_table("demo", {"id": "integer", "date": "text", "trans": "text", "symbol": "text"})

        :param table_name: string with table name
        :param fields: dict with new fields and their types (key - field name, value - type)
        :param  primary_key: bool value - default False - if True add autoincrement primary key
        :return: True if query success
        """
        new_fields = [f"{field[0]} {field[1]}" for field in fields.items()]
        if not primary_key:
            query = f"""CREATE TABLE IF NOT EXISTS {table_name} ({", ".join(new_fields)})"""
        else:
            query = f"""CREATE TABLE IF NOT EXISTS {table_name} (id SERIAL PRIMARY KEY, {", ".join(new_fields)})"""
        await LemkPgUtils.execute_query(self.dsn, query)
        return True

    async def insert(self, table_name: str, values: tuple, columns=None):
        """
        >>> await db_conn.insert("demo", (1, '2006-01-05', 'Some Text', 'A'))

        :param table_name: string with table name
        :param values: tuple with values
        :param columns: None or tuple with columns
        :return: True if query success
        """
        query = f"""INSERT INTO {table_name} {'(' + ', '.join(columns) + ')' if columns else ''} VALUES {values}"""
        await LemkPgUtils.execute_query(self.dsn, query)
        return True

    async def get_all(self, table_name: str, order_by=None, sort_type=None):
        """
        >>> await db_conn.get_all("demo")

        :param table_name: string with table name
        :param order_by: string with column for ordering
        :param sort_type: string with type of ordering (ASC / DESC)
        :return: result if query success
        """
        sort = f"{' ORDER BY ' + order_by + ' ' + sort_type if order_by and sort_type else ''}"
        query = f"""SELECT * FROM {table_name}{sort}"""
        result = await LemkPgUtils.get_query_result(self.dsn, query)
        return result

    async def get(self, table_name: str, fields: list,
                  conditions_list=None, distinct=False, order_by=None, sort_type=None):
        """
        >>> await db_conn.get("demo", ["date", "symbol"], conditions_list=[("date", "=", "2006-01-05", None)],
         distinct=True)

        :param table_name: string with table name
        :param fields: list with fields for selection
        :param conditions_list: list with tuples with conditions in it. In each tuple should be defined four values:
                 1) column for assert in WHERE clause (e.g. "date")
                 2) operand for assert column (e.g. "=", or "!=")
                 3) value for assert (e.g. "2006-01-05")
                 4) additional value if you need more then one conditions in where clause.
                    if one tuple in list - this value should be None. If more then one tuple in conditions_list -
                    this value should be string (e.g. "AND", or "OR")
        :param distinct: bool value. Default False. If True - get unique records
        :param order_by: string with column for ordering
        :param sort_type: string with type of ordering (ASC / DESC)
        :return: result if query success
        """
        dist = f"{'DISTINCT ' if distinct else ''}"
        sort = f"{' ORDER BY ' + order_by + ' ' + sort_type if order_by and sort_type else ''}"
        if conditions_list:
            conditions = LemkPgUtils.get_conditions(conditions_list)
            query = f"""SELECT {dist}{", ".join(fields)} FROM {table_name} WHERE {" ".join(
                conditions)}{sort}"""
        else:
            query = f"""SELECT {dist}{", ".join(fields)} FROM {table_name}{sort}"""
        result = await LemkPgUtils.get_query_result(self.dsn, query)
        return result

    async def update(self, table_name: str, fields: dict, conditions_list=None):
        """
        >>> await db_conn.update("demo", {"date": "2005-01-05", "symbol": "Adc"}, [("date", "=", "2006-01-05", None),
                                                                             ("symbol", "=", "A", "OR")])

        :param table_name: string with table name
        :param fields: dict with column name and their new value (key - column name, value - new column value)
        :param conditions_list: list with tuples with conditions in it. In each tuple should be defined four values:
                 1) column for assert in WHERE clause (e.g. "date")
                 2) operand for assert column (e.g. "=", or "!=")
                 3) value for assert (e.g. "2006-01-05")
                 4) additional value if you need more then one conditions in where clause.
                    if one tuple in list - this value should be None. If more then one tuple in conditions_list -
                    this value should be string (e.g. "AND", or "OR")
        :return: result if query success
        """
        columns_for_update = [f"{field[0]} = '{field[1]}'" for field in fields.items()]
        if conditions_list:
            conditions = LemkPgUtils.get_conditions(conditions_list)
            query = f"""UPDATE {table_name} SET {", ".join(columns_for_update)} WHERE {" ".join(
                conditions)}"""
        else:
            query = f"""UPDATE {table_name} SET {", ".join(columns_for_update)}"""
        result = await LemkPgUtils.execute_query(self.dsn, query)
        return result

    async def alter_table(self, table_name: str, column_name: str, action: str, column_type=None):
        """
        >>> await db_conn.alter_table("demo", "date", "ALTER COLUMN", column_type="varchar")

        :param table_name: string with table name
        :param column_name: string with column name
        :param action: string with action (e.g. "ALTER COLUMN" or  "DROP COLUMN")
        :param column_type: additional attr with new column type (e.g. "varchar")
        :return: result if query success
        """
        query = (f"""ALTER TABLE {table_name} {action} {column_name}"""
                 f"""{' TYPE ' + column_type if column_type else ''}""")
        result = await LemkPgUtils.execute_query(self.dsn, query)
        return result

    async def raw_query(self, query: str):
        """
        >>> await db_conn.raw_query("SELECT * FROM demo INNER JOIN datatable ON demo.trans = datatable.trans")

        :param query: string with query for manual execution
        :return: result if query success
        """
        result = await LemkPgUtils.get_query_result(self.dsn, query)
        return result

    async def get_with_join(self, table_name: str, join_table_name: str, join_type: str,
                            fields: list, on_condition: tuple, where_conditions_list=None):
        """
        >>> await db_conn.get_with_join("demo", "datatable", "INNER JOIN", ["*"], ("demo.trans", "=", "datatable.trans"))

        :param table_name: string with table name
        :param join_table_name: string with joins table name
        :param join_type: string with join type (e.g. "INNER JOIN", or "FULL OUTER JOIN")
        :param fields: list with strings with columns names in it (e.g. "["trans", "date"], or ["*"] if all columns)"
        :param on_condition: tuple with condition. In tuple should be defined three values:
                 1) column for assert in ON clause (e.g. "demo.trans")
                 2) operand for assert column (e.g. "=", or "!=")
                 3) value for assert from joins table (e.g. "datatable.trans")
        :param where_conditions_list: list with tuples with conditions in it. In each tuple should be defined four values:
                 1) column for assert in WHERE clause (e.g. "date")
                 2) operand for assert column (e.g. "=", or "!=")
                 3) value for assert (e.g. "2006-01-05")
                 4) additional value if you need more then one conditions in where clause.
                    if one tuple in list - this value should be None. If more then one tuple in conditions_list -
                    this value should be string (e.g. "AND", or "OR")
        :return: result if query success
        """
        if join_type not in JOINS_LIST:
            message = f"Incorrect JOIN type. Please use one of the valid JOIN types: {', '.join(JOINS_LIST)}"
            raise LemkPgError(message)

        if where_conditions_list:
            conditions = LemkPgUtils.get_conditions(where_conditions_list)
            query = (f"""SELECT {", ".join(fields)} FROM {table_name} {join_type} {join_table_name}"""
                     f""" ON {on_condition[0]} {on_condition[1]} {on_condition[2]}"""
                     f""" WHERE {" ".join(conditions)}""")
        else:
            query = (f"""SELECT {", ".join(fields)} FROM {table_name} {join_type} {join_table_name}"""
                     f""" ON {on_condition[0]} {on_condition[1]} {on_condition[2]}""")
        result = await LemkPgUtils.get_query_result(self.dsn, query)
        return result

    async def inner_join(self, table_name: str, join_table_name: str,
                         on_condition: tuple, where_conditions_list=None, fields=None, all=True):
        """
        >>> await db_conn.inner_join("demo", "datatable", ("demo.trans", "=", "datatable.trans"))

        :param table_name: string with table name
        :param join_table_name: string with joins table name
        :param on_condition: tuple with condition. In tuple should be defined three values:
                 1) column for assert in ON clause (e.g. "demo.trans")
                 2) operand for assert column (e.g. "=", or "!=")
                 3) value for assert from joins table (e.g. "datatable.trans")
        :param where_conditions_list: list with tuples with conditions in it. In each tuple should be defined four values:
                 1) column for assert in WHERE clause (e.g. "date")
                 2) operand for assert column (e.g. "=", or "!=")
                 3) value for assert (e.g. "2006-01-05")
                 4) additional value if you need more then one conditions in where clause.
                    if one tuple in list - this value should be None. If more then one tuple in conditions_list -
                    this value should be string (e.g. "AND", or "OR")
        :param fields: list with strings with columns names in it
         (e.g. "["trans", "date"]. Default None (get all columns -  if param all is True)"
        :param all: bool param - for check do we need all fields or not. Default - True (get all fields)
        :return: result if query success
        """
        query_fields = ("*" if not fields and all else f'{", ".join(fields)}')
        if where_conditions_list:
            conditions = LemkPgUtils.get_conditions(where_conditions_list)
            query = (f"""SELECT {query_fields} FROM {table_name} INNER JOIN {join_table_name}"""
                     f""" ON {on_condition[0]} {on_condition[1]} {on_condition[2]}"""
                     f""" WHERE {" ".join(conditions)}""")
        else:
            query = (f"""SELECT {query_fields} FROM {table_name} INNER JOIN {join_table_name}"""
                     f""" ON {on_condition[0]} {on_condition[1]} {on_condition[2]}""")
        result = await LemkPgUtils.get_query_result(self.dsn, query)
        return result

    async def left_join(self, table_name: str, join_table_name: str,
                        on_condition: tuple, where_conditions_list=None, fields=None, all=True):
        """
        >>> await db_conn.left_join("demo", "datatable", ("demo.trans", "=", "datatable.trans"))

        :param table_name: string with table name
        :param join_table_name: string with joins table name
        :param on_condition: tuple with condition. In tuple should be defined three values:
                 1) column for assert in ON clause (e.g. "demo.trans")
                 2) operand for assert column (e.g. "=", or "!=")
                 3) value for assert from joins table (e.g. "datatable.trans")
        :param where_conditions_list: list with tuples with conditions in it. In each tuple should be defined four values:
                 1) column for assert in WHERE clause (e.g. "date")
                 2) operand for assert column (e.g. "=", or "!=")
                 3) value for assert (e.g. "2006-01-05")
                 4) additional value if you need more then one conditions in where clause.
                    if one tuple in list - this value should be None. If more then one tuple in conditions_list -
                    this value should be string (e.g. "AND", or "OR")
        :param fields: list with strings with columns names in it
         (e.g. "["trans", "date"]. Default None (get all columns -  if param all is True)"
        :param all: bool param - for check do we need all fields or not. Default - True (get all fields)
        :return: result if query success
        """
        query_fields = ("*" if not fields and all else f'{", ".join(fields)}')
        if where_conditions_list:
            conditions = LemkPgUtils.get_conditions(where_conditions_list)
            query = (f"""SELECT {query_fields} FROM {table_name} LEFT JOIN {join_table_name}"""
                     f""" ON {on_condition[0]} {on_condition[1]} {on_condition[2]}"""
                     f""" WHERE {" ".join(conditions)}""")
        else:
            query = (f"""SELECT {query_fields} FROM {table_name} LEFT JOIN {join_table_name}"""
                     f""" ON {on_condition[0]} {on_condition[1]} {on_condition[2]}""")
        result = await LemkPgUtils.get_query_result(self.dsn, query)
        return result

    async def right_join(self, table_name: str, join_table_name: str,
                         on_condition: tuple, where_conditions_list=None, fields=None, all=True):
        """
        >>> await db_conn.right_join("demo", "datatable", ("demo.trans", "=", "datatable.trans"))

        :param table_name: string with table name
        :param join_table_name: string with joins table name
        :param on_condition: tuple with condition. In tuple should be defined three values:
                 1) column for assert in ON clause (e.g. "demo.trans")
                 2) operand for assert column (e.g. "=", or "!=")
                 3) value for assert from joins table (e.g. "datatable.trans")
        :param where_conditions_list: list with tuples with conditions in it. In each tuple should be defined four values:
                 1) column for assert in WHERE clause (e.g. "date")
                 2) operand for assert column (e.g. "=", or "!=")
                 3) value for assert (e.g. "2006-01-05")
                 4) additional value if you need more then one conditions in where clause.
                    if one tuple in list - this value should be None. If more then one tuple in conditions_list -
                    this value should be string (e.g. "AND", or "OR")
        :param fields: list with strings with columns names in it
         (e.g. "["trans", "date"]. Default None (get all columns -  if param all is True)"
        :param all: bool param - for check do we need all fields or not. Default - True (get all fields)
        :return: result if query success
        """
        query_fields = ("*" if not fields and all else f'{", ".join(fields)}')
        if where_conditions_list:
            conditions = LemkPgUtils.get_conditions(where_conditions_list)
            query = (f"""SELECT {query_fields} FROM {table_name} RIGHT JOIN {join_table_name}"""
                     f""" ON {on_condition[0]} {on_condition[1]} {on_condition[2]}"""
                     f""" WHERE {" ".join(conditions)}""")
        else:
            query = (f"""SELECT {query_fields} FROM {table_name} RIGHT JOIN {join_table_name}"""
                     f""" ON {on_condition[0]} {on_condition[1]} {on_condition[2]}""")
        result = await LemkPgUtils.get_query_result(self.dsn, query)
        return result

    async def full_join(self, table_name: str, join_table_name: str,
                        on_condition: tuple, where_conditions_list=None, fields=None, all=True):
        """
        >>> await db_conn.full_join("demo", "datatable", ("demo.trans", "=", "datatable.trans"))

        :param table_name: string with table name
        :param join_table_name: string with joins table name
        :param on_condition: tuple with condition. In tuple should be defined three values:
                 1) column for assert in ON clause (e.g. "demo.trans")
                 2) operand for assert column (e.g. "=", or "!=")
                 3) value for assert from joins table (e.g. "datatable.trans")
        :param where_conditions_list: list with tuples with conditions in it. In each tuple should be defined four values:
                 1) column for assert in WHERE clause (e.g. "date")
                 2) operand for assert column (e.g. "=", or "!=")
                 3) value for assert (e.g. "2006-01-05")
                 4) additional value if you need more then one conditions in where clause.
                    if one tuple in list - this value should be None. If more then one tuple in conditions_list -
                    this value should be string (e.g. "AND", or "OR")
        :param fields: list with strings with columns names in it
         (e.g. "["trans", "date"]. Default None (get all columns -  if param all is True)"
        :param all: bool param - for check do we need all fields or not. Default - True (get all fields)
        :return: result if query success
        """
        query_fields = ("*" if not fields and all else f'{", ".join(fields)}')
        if where_conditions_list:
            conditions = LemkPgUtils.get_conditions(where_conditions_list)
            query = (f"""SELECT {query_fields} FROM {table_name} FULL OUTER JOIN {join_table_name}"""
                     f""" ON {on_condition[0]} {on_condition[1]} {on_condition[2]}"""
                     f""" WHERE {" ".join(conditions)}""")
        else:
            query = (f"""SELECT {query_fields} FROM {table_name} FULL OUTER JOIN {join_table_name}"""
                     f""" ON {on_condition[0]} {on_condition[1]} {on_condition[2]}""")
        result = await LemkPgUtils.get_query_result(self.dsn, query)
        return result

    async def delete_table(self, table_name: str):
        """
        >>> await db_conn.delete_table("demo")

        :param table_name: string with table name
        :return: True if query success
        """
        query = f"""DROP TABLE IF EXISTS {table_name}"""
        await LemkPgUtils.execute_query(self.dsn, query)
        return True

    async def clear_table(self, table_name: str):
        """
        >>> await db_conn.clear_table("demo")

        :param table_name: string with table name
        :return: True if query success
        """
        query = f"""TRUNCATE TABLE {table_name}"""
        await LemkPgUtils.execute_query(self.dsn, query)
        return True

    async def delete_records(self, table_name: str, conditions_list=None):
        """
        >>> await db_conn.delete_records("demo", [("date", "=", "2006-01-05", None), ("symbol", "=", "A", "OR")])

        :param table_name: string with table name
        :param conditions_list: list with tuples with conditions in it. In each tuple should be defined four values:
                 1) column for assert in WHERE clause (e.g. "date")
                 2) operand for assert column (e.g. "=", or "!=")
                 3) value for assert (e.g. "2006-01-05")
                 4) additional value if you need more then one conditions in where clause.
                    if one tuple in list - this value should be None. If more then one tuple in conditions_list -
                    this value should be string (e.g. "AND", or "OR")
        :return: True if query success
        """
        if conditions_list:
            conditions = LemkPgUtils.get_conditions(conditions_list)
            query = f"""DELETE FROM {table_name} WHERE {" ".join(conditions)}"""
        else:
            query = f"""DELETE FROM {table_name}"""
        await LemkPgUtils.execute_query(self.dsn, query)
        return True

    async def count(self, table_name: str, column: str, conditions_list=None):
        """
        >>> await db_conn.count("demo", "date")

        :param table_name: string with table name
        :param column: string with column name
        :param conditions_list: list with tuples with conditions in it. In each tuple should be defined four values:
                 1) column for assert in WHERE clause (e.g. "date")
                 2) operand for assert column (e.g. "=", or "!=")
                 3) value for assert (e.g. "2006-01-05")
                 4) additional value if you need more then one conditions in where clause.
                    if one tuple in list - this value should be None. If more then one tuple in conditions_list -
                    this value should be string (e.g. "AND", or "OR")
        :return: result if query success
        """
        if conditions_list:
            conditions = LemkPgUtils.get_conditions(conditions_list)
            query = f"""SELECT COUNT({column}) FROM {table_name} WHERE {" ".join(conditions)}"""
        else:
            query = f"""SELECT COUNT({column}) FROM {table_name}"""
        result = await LemkPgUtils.get_query_result(self.dsn, query)
        return result

    async def avg(self, table_name: str, column: str, conditions_list=None):
        """
        >>> await db_conn.avg("demo", "id")

        :param table_name: string with table name
        :param column: string with column name
        :param conditions_list: list with tuples with conditions in it. In each tuple should be defined four values:
                 1) column for assert in WHERE clause (e.g. "date")
                 2) operand for assert column (e.g. "=", or "!=")
                 3) value for assert (e.g. "2006-01-05")
                 4) additional value if you need more then one conditions in where clause.
                    if one tuple in list - this value should be None. If more then one tuple in conditions_list -
                    this value should be string (e.g. "AND", or "OR")
        :return: result if query success
        """
        if conditions_list:
            conditions = LemkPgUtils.get_conditions(conditions_list)
            query = f"""SELECT AVG({column}) FROM {table_name} WHERE {" ".join(conditions)}"""
        else:
            query = f"""SELECT AVG({column}) FROM {table_name}"""
        result = await LemkPgUtils.get_query_result(self.dsn, query)
        return result

    async def sum(self, table_name: str, column: str, conditions_list=None):
        """
        >>> await db_conn.sum("demo", "id")

        :param table_name: string with table name
        :param column: string with column name
        :param conditions_list: list with tuples with conditions in it. In each tuple should be defined four values:
                 1) column for assert in WHERE clause (e.g. "date")
                 2) operand for assert column (e.g. "=", or "!=")
                 3) value for assert (e.g. "2006-01-05")
                 4) additional value if you need more then one conditions in where clause.
                    if one tuple in list - this value should be None. If more then one tuple in conditions_list -
                    this value should be string (e.g. "AND", or "OR")
        :return: result if query success
        """
        if conditions_list:
            conditions = LemkPgUtils.get_conditions(conditions_list)
            query = f"""SELECT SUM({column}) FROM {table_name} WHERE {" ".join(conditions)}"""
        else:
            query = f"""SELECT SUM({column}) FROM {table_name}"""
        result = await LemkPgUtils.get_query_result(self.dsn, query)
        return result

    async def min(self, table_name: str, column: str, conditions_list=None):
        """
        >>> await db_conn.min("demo", "date")

        :param table_name: string with table name
        :param column: string with column name
        :param conditions_list: list with tuples with conditions in it. In each tuple should be defined four values:
                 1) column for assert in WHERE clause (e.g. "date")
                 2) operand for assert column (e.g. "=", or "!=")
                 3) value for assert (e.g. "2006-01-05")
                 4) additional value if you need more then one conditions in where clause.
                    if one tuple in list - this value should be None. If more then one tuple in conditions_list -
                    this value should be string (e.g. "AND", or "OR")
        :return: result if query success
        """
        if conditions_list:
            conditions = LemkPgUtils.get_conditions(conditions_list)
            query = f"""SELECT MIN({column}) FROM {table_name} WHERE {" ".join(conditions)}"""
        else:
            query = f"""SELECT MIN({column}) FROM {table_name}"""
        result = await LemkPgUtils.get_query_result(self.dsn, query)
        return result

    async def max(self, table_name: str, column: str, conditions_list=None):
        """
        >>> await db_conn.max("demo", "date")

        :param table_name: string with table name
        :param column: string with column name
        :param conditions_list: list with tuples with conditions in it. In each tuple should be defined four values:
                 1) column for assert in WHERE clause (e.g. "date")
                 2) operand for assert column (e.g. "=", or "!=")
                 3) value for assert (e.g. "2006-01-05")
                 4) additional value if you need more then one conditions in where clause.
                    if one tuple in list - this value should be None. If more then one tuple in conditions_list -
                    this value should be string (e.g. "AND", or "OR")
        :return: result if query success
        """
        if conditions_list:
            conditions = LemkPgUtils.get_conditions(conditions_list)
            query = f"""SELECT MAX({column}) FROM {table_name} WHERE {" ".join(conditions)}"""
        else:
            query = f"""SELECT MAX({column}) FROM {table_name}"""
        result = await LemkPgUtils.get_query_result(self.dsn, query)
        return result
