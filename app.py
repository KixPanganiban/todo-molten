# -*- encoding: utf-8 -*-
"""A Molten web server for Todobackend.com
"""
__author__ = "Kix Panganiban <github.com/kixpanganiban>"

import json
import sqlite3
from contextlib import contextmanager
from inspect import Parameter
from typing import Any, Callable, Iterator, List, Optional, Tuple, Union

from molten import (
    HTTP_201,
    HTTP_204,
    HTTP_403,
    HTTP_404,
    App,
    Component,
    Field,
    Header,
    HTTPError,
    Include,
    Middleware,
    Request,
    Response,
    ResponseRenderer,
    ResponseRendererMiddleware,
    Route,
    schema,
)
from molten.app import BaseApp
from molten.http.headers import HeadersDict
from molten.renderers import JSONRenderer


class DB:
    """Database adapter class, in this case uses a SQLite backend.
    """

    def __init__(self) -> None:
        self._db = sqlite3.connect("molten.db")
        self._db.row_factory = sqlite3.Row

        with self.get_cursor() as cursor:
            cursor.execute(
                'create table if not exists todos(title text, completed bool, "order" int)'
            )

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
    """Database component for Dependency Injection.
    """

    is_cacheable = True
    is_singleton = True

    def can_handle_parameter(self, parameter: Parameter) -> bool:
        return parameter.annotation is DB

    def resolve(self) -> DB:
        return DB()


@schema
class Todo:
    """Todo object definition for the API. Schema will automatically populate
    fields implicitly.
    """

    id: Optional[int] = Field(response_only=True)
    title: Optional[str]
    order: Optional[int]
    url: Optional[str] = Field(response_only=True)
    completed: Optional[bool] = Field(default=False)


class TodoManager:
    """TodoManager ORM abstraction for the DB.
    """

    def __init__(self, db: DB) -> None:
        self.db = db

    def _map_todo(self, data: sqlite3.Row):
        """Maps an sqlite3.Row object into a dict, converting the bool values
        and populating the url field.
        """
        data = dict(data)
        data["completed"] = False if data["completed"] == 0 else True
        data["url"] = f"https://todo-molten.herokuapp.com/v1/todos/{data['id']}"
        return data

    def create(self, todo: Todo) -> Todo:
        """Insert a todo into the DB.
        """
        with self.db.get_cursor() as cursor:
            cursor.execute(
                'insert into todos(title, completed, "order") values(?, ?, ?)',
                [todo.title, todo.completed, todo.order],
            )

            return self.get_by_id(cursor.lastrowid)

    def get_all(self) -> List[Todo]:
        """Retrieve all todos from the DB.
        """
        with self.db.get_cursor() as cursor:
            cursor.execute(
                'select rowid as id, title, completed, "order" from todos order by "order" desc'
            )
            return [Todo(**self._map_todo(data)) for data in cursor.fetchall()]

    def get_by_id(self, todo_id: int) -> Optional[Todo]:
        """Retrieve a single todo from the DB by id.
        """
        with self.db.get_cursor() as cursor:
            cursor.execute(
                'select rowid as id, title, completed, "order" from todos where rowid = ? limit 1',
                [todo_id],
            )
            data = cursor.fetchone()
            if data is None:
                return None

            return Todo(**self._map_todo(data))

    def update_by_id(self, todo_id: int, updates: Todo) -> Optional[Todo]:
        """Update a single todo in the DB by id. Uses existing todo to
        perform a partial update for fields that are not provided.
        """
        todo = self.get_by_id(todo_id)
        if todo:
            for attr_key in ("title", "completed", "order"):
                attr_val = getattr(updates, attr_key, None)
                if attr_val is not None:
                    setattr(todo, attr_key, attr_val)
            with self.db.get_cursor() as cursor:
                cursor.execute(
                    'update todos set title = ?, completed = ?, "order" = ? where rowid = ?',
                    [todo.title, todo.completed, todo.order, todo_id],
                )
                return self.get_by_id(todo_id)
        return None

    def delete_by_id(self, todo_id: int) -> None:
        """Delete a single todo by id.
        """
        with self.db.get_cursor() as cursor:
            cursor.execute("delete from todos where rowid = ?", [todo_id])

    def delete_all(self) -> None:
        """Delete all todos in the table.
        """
        with self.db.get_cursor() as cursor:
            cursor.execute("delete from todos")


class TodoManagerComponent:
    """TodoManager component for Dependency Injection.
    """

    is_cacheable = True
    is_singleton = True

    def can_handle_parameter(self, parameter: Parameter) -> bool:
        return parameter.annotation is TodoManager

    def resolve(self, db: DB) -> TodoManager:
        return TodoManager(db)


def options_todos() -> str:
    """Handles OPTIONS /v1/todos
    """
    return HTTP_204, None


def list_todos(manager: TodoManager) -> List[Todo]:
    """Handles GET /v1/todos
    """
    return manager.get_all()


def options_todo(todo_id: str) -> str:
    """Handles OPTIONS /v1/todos/{id}
    """
    return HTTP_204, None


def get_todo(todo_id: str, manager: TodoManager) -> Todo:
    """Handles GET /v1/todos/{id}
    """
    todo = manager.get_by_id(int(todo_id))
    if todo is None:
        raise HTTPError(HTTP_404, {"error": f"todo {todo_id} not found"})
    return todo


def create_todo(todo: Todo, manager: TodoManager) -> Tuple[str, Todo]:
    """Handles POST /v1/todos
    """
    return HTTP_201, manager.create(todo)


def update_todo(todo_id: str, todo: Todo, manager: TodoManager) -> Todo:
    """Handles PATCH /v1/todos/{id}
    """
    todo = manager.update_by_id(todo_id, todo)
    if todo is None:
        raise HTTPError(HTTP_404, {"error": f"todo {todo_id} not found"})
    return todo


def delete_todo(todo_id: str, manager: TodoManager) -> Tuple[str, None]:
    """Handles DELETE /v1/todos/{id}
    """
    manager.delete_by_id(int(todo_id))
    return HTTP_204, None


def delete_all(manager: TodoManager) -> Tuple[str, None]:
    """Handles DELETE /v1/todos/
    """
    manager.delete_all()
    return []


class CORSMiddleware:
    """Middleware to inject CORS headers.
    """

    def __call__(self, handler: Callable[..., Any]) -> Callable[..., Response]:
        def handle(app: BaseApp, request: Request) -> Response:
            headers: HeadersDict = {
                "access-control-allow-origin": "*",
                "access-control-allow-headers": "Accept, Content-Type",
                "access-control-allow-methods": "*",
            }
            response = handler()
            response.headers.add_all(headers)
            return response

        return handle


class PlainTextRenderer(JSONRenderer):
    """A plaintext response renderer, subclassed from JSONRenderer, to comply
    with Todobackend's plaintext accept header.
    """

    mime_type = "text/plaint"

    def can_render_response(self, accept: str) -> bool:
        return accept.startswith("text/plain")

    def render(self, status: str, response_data: Any) -> Response:
        content = json.dumps(response_data, default=self.default)
        return Response(status, content=content, headers={"content-type": "text/plain"})


components: List[Component] = [DBComponent(), TodoManagerComponent()]


middleware: List[Middleware] = [CORSMiddleware(), ResponseRendererMiddleware()]

renderers: List[ResponseRenderer] = [JSONRenderer(), PlainTextRenderer()]


routes: List[Union[Route, Include]] = [
    Include(
        "/v1/todos",
        [
            Route("/", options_todos, method="OPTIONS"),
            Route("/", list_todos),
            Route("/", create_todo, method="POST"),
            Route("/", delete_all, method="DELETE"),
            Route("/{todo_id}", options_todo, method="OPTIONS"),
            Route("/{todo_id}", get_todo),
            Route("/{todo_id}", delete_todo, method="DELETE"),
            Route("/{todo_id}", update_todo, method="PATCH"),
        ],
    )
]

app = App(
    components=components, middleware=middleware, renderers=renderers, routes=routes
)
