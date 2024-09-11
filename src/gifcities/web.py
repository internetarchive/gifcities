from elasticsearch import Elasticsearch
import pydantic
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import RedirectResponse, Response
from starlette.routing import Route, Mount
from starlette.templating import Jinja2Templates
from starlette.staticfiles import StaticFiles

from gifcities.config import settings

MAX_PAGE_SIZE = 50
DEFAULT_PAGE_SIZE = 20

tmpls = Jinja2Templates(directory='src/gifcities/templates')

es_client = Elasticsearch(settings.ELASTICSEARCH_URL)

async def index(request: Request) -> Response:
    return tmpls.TemplateResponse(request, "index.html", {"page_size": DEFAULT_PAGE_SIZE})

async def about(request: Request) -> Response:
    return tmpls.TemplateResponse(request, "about.html")

async def favicon(request: Request) -> RedirectResponse:
    return RedirectResponse(status_code=301, url="/static/favicon.ico")

class GeocitiesPage(pydantic.BaseModel):
    url: str
    timestamp: str

class GifUse(pydantic.BaseModel):
    url: str
    timestamp: str
    path: str
    filename: str
    page: GeocitiesPage|None

class Gif(pydantic.BaseModel):
    checksum: str
    page_count: int
    uses: list[GifUse]

async def search(request: Request) -> Response:
    q = request.query_params.get('q', 'geocities')
    o = request.query_params.get('offset', "0")
    ps = request.query_params.get('page_size', str(DEFAULT_PAGE_SIZE))
    page_size = DEFAULT_PAGE_SIZE
    offset = 0
    try:
        page_size = int(ps)
    except ValueError:
        pass

    try:
        offset = int(o)
    except ValueError:
        pass

    if page_size > MAX_PAGE_SIZE:
        page_size = MAX_PAGE_SIZE

    query = {
        "nested": {
        "path": "uses",
        "query": {
            "multi_match": {
                "query": q,
                "fields": ["uses.filename^3", "uses.path"]
                }
            }
        }
    }

    resp = es_client.search(
            index=settings.ELASTICSEARCH_INDEX,
            from_=offset,
            size=page_size,
            sort='page_count:desc',
            query=query)

    results: list[Gif] = []

    # TODO use async query to ES

    for h in resp['hits']['hits']:
        doc = h['_source']
        uses = []
        for u in doc['uses']:
            page = None
            if u['page'] != None:
                page = GeocitiesPage(url=u['page']['url'], timestamp=u['page']['timestamp'])

            uses.append(GifUse(url=u['url'],
                               timestamp=u['timestamp'],
                               path=u['path'],
                               filename=u['filename'],
                               page=page))

        results.append(Gif(
            checksum=doc['checksum'],
            page_count=doc['page_count'],
            uses=uses))

    # TODO debugging
    del resp['hits']['hits']
    print(resp)

    ctx = {
        "q": q,
        "results": results,
        "current_page": int(offset / page_size) + 1,
        "settings": settings,
        "offset": offset,
        "total_pages": int(resp['hits']['total']['value'] / page_size) + 1,
        "page_size": page_size,
    }

    return tmpls.TemplateResponse(request, "results.html", ctx)


app = Starlette(debug=settings.DEBUG, routes=[
    Route('/', index),
    Route('/about', about),
    Route('/favicon.ico', favicon),
    Route('/search', search),
    Mount('/static', StaticFiles(directory='src/gifcities/static'), name='static'),
])

#if settings.SENTRY_DSN:
#    logger.info("Sentry integration enabled")
#    sentry_sdk.init(
#        dsn=settings.SENTRY_DSN,
#        environment=settings.SCHOLAR_ENV,
#        max_breadcrumbs=10,
#        release=GIT_REVISION,
#    )
#    app.add_middleware(SentryAsgiMiddleware)
