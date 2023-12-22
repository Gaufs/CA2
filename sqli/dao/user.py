from hashlib import md5
from typing import NamedTuple, Optional


from aiopg import Connection


class User(NamedTuple):
    id: int
    first_name: str
    middle_name: Optional[str]
    last_name: str
    username: str
    pwd_hash: str
    is_admin: bool

    @classmethod
    def from_raw(cls, raw: tuple):
        return cls(*raw) if raw else None

    @staticmethod
    async def get(conn: Connection, id_: int):
        txt_sql = ('SELECT id, first_name, middle_name, last_name, '
                'username, pwd_hash, is_admin FROM users WHERE id = %(id_)s')
        param_sql = {'id_': id_}
        async with conn.cursor() as cur:
            await cur.execute(txt_sql, param_sql)
            return User.from_raw(await cur.fetchone())

    @staticmethod
    async def get_by_username(conn: Connection, username: str):
        sql = ('SELECT id, first_name, middle_name, last_name, '
                'username, pwd_hash, is_admin FROM users WHERE username = %(username)s')
        param = {'username': username}
        async with conn.cursor() as cur:
            await cur.execute(sql, param)
            return User.from_raw(await cur.fetchone())
    
    @staticmethod
    async def create(conn: Connection, first_name: str, middle_name: Optional[str], last_name: str, username: str, pwd_hash: str):
        q = ('INSERT INTO users (first_name, middle_name, last_name, username, pwd_hash, is_admin ) '
            'VALUES (%(first_name)s, %(middle_name)s,  %(last_name)s,  %(username)s,  md5(%(pwd_hash)s), FALSE)')
        params = { 'first_name': first_name,'middle_name': middle_name, 'last_name': last_name, 'username': username, 'pwd_hash': pwd_hash}
        async with conn.cursor() as cur:
            await cur.execute(q, params)
    
    @staticmethod
    async def delete(conn: Connection, id: int):
        q2 = ('DELETE FROM users WHERE id = %(id)s ')
        params2 = {'id': id}
        async with conn.cursor() as cur:
            await cur.execute(q2, params2)

    def check_password(self, password: str):
        return self.pwd_hash == md5(password.encode('utf-8')).hexdigest()
