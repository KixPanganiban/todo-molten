import sqlite3
from contextlib import contextmanager
from inspect import Parameter
from typing import Any, Callable, Iterator, List, Optional, Tuple, Union

from molten import (
    HTTP_201, HTTP_204, HTTP_403, HTTP_404, App, Component, Field, Header, HTTPError, Include,
    Middleware, Request, Response, ResponseRendererMiddleware, Route, schema
)
from molten.app import BaseApp
from molten.http.headers import HeadersDict


class DB:
    def __init__(self) -> None:
        self._db = sqlite3.connect(":memory:")
        self._db.row_factory = sqlite3.Row

        with self.get_cursor() as cursor:
            cursor.execute("create table todos(title text, completed bool, \"order\" int)")

    @contextmanager
    def get_cursor(self) -> Iterator[sqlite3.Cursor]:
        cursor = self._db.cursor()

        try:
            yield cursor
            self._db.commit()
        except Exception:
            self._db.rollback()
            raise
        finally:
            cursor.close()


class DBComponent:
    is_cacheable = True
    is_singleton = True

    def can_handle_parameter(self, parameter: Parameter) -> bool:
        return parameter.annotation is DB

    def resolve(self) -> DB:
        return DB()


@schema
class Todo:
    id: Optional[int] = Field(response_only=True)
    title: Optional[str]
    completed: Optional[bool]
    order: Optional[int]


class TodoManager:
    def __init__(self, db: DB) -> None:
        self.db = db

    def _map_bool(self, data: sqlite3.Row):
        data = dict(data)
        data['completed'] = False if data['completed'] == 0 else True
        return data

    def create(self, todo: Todo) -> Todo:
        with self.db.get_cursor() as cursor:
            cursor.execute("insert into todos(title, completed, \"order\") values(?, ?, ?)", [
                todo.title,
                todo.completed,
                todo.order
            ])

            todo.id = cursor.lastrowid
            return todo

    def get_all(self) -> List[Todo]:
        with self.db.get_cursor() as cursor:
            cursor.execute("select rowid as id, title, completed, \"order\" from todos order by \"order\" desc")
            return [Todo(**self._map_bool(data)) for data in cursor.fetchall()]

    def get_by_id(self, todo_id: int) -> Optional[Todo]:
        with self.db.get_cursor() as cursor:
            cursor.execute("select rowid as id, title, completed, \"order\" from todos where rowid = ? limit 1", [todo_id])
            data = cursor.fetchone()
            if data is None:
                return None

            return Todo(**self._map_bool(data))

    def update_by_id(self, todo_id: int, updates: Todo) -> Optional[Todo]:
        todo = self.get_by_id(todo_id)
        if todo:
            for attr_key in ('title', 'completed', 'order'):
                attr_val = getattr(updates, attr_key, None)
                if attr_val is not None:
                    setattr(todo, attr_key, attr_val)
            with self.db.get_cursor() as cursor:
                cursor.execute(
                    "update todos set title = ?, completed = ?, \"order\" = ? where rowid = ?",
                    [todo.title, todo.completed, todo.order, todo_id]
                    )
                return self.get_by_id(todo_id)
        return None

    def delete_by_id(self, todo_id: int) -> None:
        with self.db.get_cursor() as cursor:
            cursor.execute("delete from todos where rowid = ?", [todo_id])


class TodoManagerComponent:
    is_cacheable = True
    is_singleton = True

    def can_handle_parameter(self, parameter: Parameter) -> bool:
        return parameter.annotation is TodoManager

    def resolve(self, db: DB) -> TodoManager:
        return TodoManager(db)

def options_todos() -> str:
    return "ok"


def list_todos(manager: TodoManager) -> List[Todo]:
    return manager.get_all()


def get_todo(todo_id: str, manager: TodoManager) -> Todo:
    todo = manager.get_by_id(int(todo_id))
    if todo is None:
        raise HTTPError(HTTP_404, {"error": f"todo {todo_id} not found"})
    return todo


def create_todo(todo: Todo, manager: TodoManager) -> Tuple[str, Todo]:
    return HTTP_201, manager.create(todo)

def update_todo(todo_id: str, todo: Todo, manager: TodoManager) -> Todo:
    todo = manager.update_by_id(todo_id, todo)
    if todo is None:
        raise HTTPError(HTTP_404, {"error": f"todo {todo_id} not found"})
    return todo


def delete_todo(todo_id: str, manager: TodoManager) -> Tuple[str, None]:
    manager.delete_by_id(int(todo_id))
    return HTTP_204, None

class CORSMiddleware:
    def __call__(self, handler: Callable[..., Any]) -> Callable[..., Response]:
        def handle(app: BaseApp, request: Request) -> Response:
            headers: HeadersDict = {
                'access-control-allow-origin': '*',
                'access-control-allow-headers': '*'
            }
            response = handler()
            response.headers.add_all(headers)
            return response
        return handle



components: List[Component] = [
    DBComponent(),
    TodoManagerComponent(),
]


middleware: List[Middleware] = [
    CORSMiddleware(),
    ResponseRendererMiddleware()
]


routes: List[Union[Route, Include]] = [
    Include("/v1/todos", [
        Route("/", options_todos, method="OPTIONS"),
        Route("/", list_todos),
        Route("/", create_todo, method="POST"),
        Route("/{todo_id}", get_todo),
        Route("/{todo_id}", delete_todo, method="DELETE"),
        Route("/{todo_id}", update_todo, method="PATCH")
    ]),
]

app = App(
    components=components,
    middleware=middleware,
    routes=routes,
)