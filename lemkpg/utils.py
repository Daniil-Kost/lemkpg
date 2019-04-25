import aiopg
import psycopg2


class LemkPgUtils:

    @classmethod
    def get_conditions(cls, conditions_list):

        if not isinstance(conditions_list, list):
            message = f"Variable condition_list should be list"
            raise LemkPgError(message)
        for cond in conditions_list:
            if not isinstance(cond, tuple):
                message = f"Variable condition_list should have tuples within"
                raise LemkPgError(message)

        conditions = [
            f"{condition[3] + ' ' if condition[3] is not None else ''}{condition[0]} {condition[1]} '{condition[2]}'"
            for condition in conditions_list]

        return conditions

    @classmethod
    async def get_query_result(cls, dsn, query):
        async with aiopg.create_pool(dsn) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(query)
                    result = []
                    try:
                        async for row in cursor:
                            result.append(row)
                            return result
                    except psycopg2.ProgrammingError as e:
                        print(e)
                        return None

    @classmethod
    async def execute_query(cls, dsn, query):
        async with aiopg.create_pool(dsn) as pool:
            async with pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(query)
                    return True
