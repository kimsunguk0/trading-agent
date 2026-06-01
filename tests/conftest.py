from __future__ import annotations

import asyncio
import inspect
import importlib.util
import sys
import types


if importlib.util.find_spec("asyncpg") is None and "asyncpg" not in sys.modules:
    asyncpg = types.ModuleType("asyncpg")

    async def _missing_connect(*_args, **_kwargs):
        raise RuntimeError("asyncpg is not installed in this test environment")

    asyncpg.connect = _missing_connect
    asyncpg.Connection = object
    sys.modules["asyncpg"] = asyncpg


if importlib.util.find_spec("websockets") is None and "websockets" not in sys.modules:
    websockets = types.ModuleType("websockets")

    def _missing_ws_connect(*_args, **_kwargs):
        raise RuntimeError("websockets is not installed in this test environment")

    websockets.connect = _missing_ws_connect
    sys.modules["websockets"] = websockets


if importlib.util.find_spec("openai") is None and "openai" not in sys.modules:
    openai = types.ModuleType("openai")

    class AsyncOpenAI:
        def __init__(self, *_args, **_kwargs) -> None:
            self.chat = types.SimpleNamespace(
                completions=types.SimpleNamespace(create=self._missing_create)
            )

        async def _missing_create(self, *_args, **_kwargs):
            raise RuntimeError("openai is not installed in this test environment")

    openai.AsyncOpenAI = AsyncOpenAI
    sys.modules["openai"] = openai


if importlib.util.find_spec("redis") is None and "redis" not in sys.modules:
    redis_mod = types.ModuleType("redis")
    redis_asyncio = types.ModuleType("redis.asyncio")

    class _MissingRedis:
        async def aclose(self) -> None:
            return None

    def from_url(*_args, **_kwargs):
        return _MissingRedis()

    redis_asyncio.from_url = from_url
    redis_mod.asyncio = redis_asyncio
    sys.modules["redis"] = redis_mod
    sys.modules["redis.asyncio"] = redis_asyncio


if importlib.util.find_spec("fastapi") is None and "fastapi" not in sys.modules:
    fastapi = types.ModuleType("fastapi")

    class APIRouter:
        def get(self, *_args, **_kwargs):
            return lambda func: func

        def post(self, *_args, **_kwargs):
            return lambda func: func

    class FastAPI:
        def __init__(self, *_args, **_kwargs) -> None:
            pass

        def on_event(self, *_args, **_kwargs):
            return lambda func: func

        def get(self, *_args, **_kwargs):
            return lambda func: func

        def include_router(self, *_args, **_kwargs) -> None:
            return None

    fastapi.APIRouter = APIRouter
    fastapi.FastAPI = FastAPI
    sys.modules["fastapi"] = fastapi


if importlib.util.find_spec("qdrant_client") is None and "qdrant_client" not in sys.modules:
    qdrant_client = types.ModuleType("qdrant_client")

    class QdrantClient:
        def __init__(self, *_args, **_kwargs) -> None:
            pass

    qdrant_client.QdrantClient = QdrantClient
    sys.modules["qdrant_client"] = qdrant_client

    http_mod = types.ModuleType("qdrant_client.http")
    models_mod = types.ModuleType("qdrant_client.http.models")

    class Distance:
        COSINE = "Cosine"

    class VectorParams:
        def __init__(self, *, size: int, distance: str) -> None:
            self.size = size
            self.distance = distance

    class PointStruct:
        def __init__(self, *, id: str, vector: list[float], payload: dict) -> None:
            self.id = id
            self.vector = vector
            self.payload = payload

    models_mod.Distance = Distance
    models_mod.VectorParams = VectorParams
    models_mod.PointStruct = PointStruct
    http_mod.models = models_mod
    sys.modules["qdrant_client.http"] = http_mod
    sys.modules["qdrant_client.http.models"] = models_mod


if importlib.util.find_spec("telegram") is None and "telegram" not in sys.modules:
    telegram = types.ModuleType("telegram")

    class Bot:
        pass

    class Update:
        pass

    telegram.Bot = Bot
    telegram.Update = Update
    sys.modules["telegram"] = telegram

    telegram_ext = types.ModuleType("telegram.ext")

    class ApplicationBuilder:
        def token(self, *_args, **_kwargs):
            return self

        def build(self):
            return types.SimpleNamespace(
                add_handler=lambda *_args, **_kwargs: None,
                run_polling=lambda *_args, **_kwargs: None,
            )

    class CommandHandler:
        def __init__(self, *_args, **_kwargs) -> None:
            pass

    class ContextTypes:
        DEFAULT_TYPE = object

    telegram_ext.ApplicationBuilder = ApplicationBuilder
    telegram_ext.CommandHandler = CommandHandler
    telegram_ext.ContextTypes = ContextTypes
    sys.modules["telegram.ext"] = telegram_ext


def pytest_configure(config) -> None:
    config.addinivalue_line("markers", "asyncio: run coroutine test in an event loop")


def pytest_pyfunc_call(pyfuncitem):
    if not inspect.iscoroutinefunction(pyfuncitem.obj):
        return None

    kwargs = {
        name: pyfuncitem.funcargs[name]
        for name in pyfuncitem._fixtureinfo.argnames
        if name in pyfuncitem.funcargs
    }
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    loop.run_until_complete(pyfuncitem.obj(**kwargs))
    return True


def pytest_runtest_setup(item) -> None:
    try:
        asyncio.get_event_loop()
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())
