import contextlib
import json
import logging
import urllib.parse
from enum import StrEnum
from functools import lru_cache
from typing import Any, AsyncIterator, TypedDict

import numpy
import open_clip
import pydantic
import torch
from elasticsearch import Elasticsearch
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import HTMLResponse, RedirectResponse, Response
from starlette.routing import Route, Mount
from starlette.templating import Jinja2Templates
from starlette.staticfiles import StaticFiles

from gifcities.config import settings

MAX_PAGE_SIZE = 50
DEFAULT_PAGE_SIZE = 25
DEFAULT_MNSFW_THRESHOLD = 0.5

tmpls = Jinja2Templates(directory='src/gifcities/templates')

class EmbeddedQuery(pydantic.BaseModel):
    query: str
    embedding: list[float]

class QueryEmbedder:
    def __init__(self, model_name, pretrain):
        if model_name == "":
            raise ValueError("model_name required")
        if pretrain == "":
            raise ValueError("pretrain required")
        self.model_name = model_name
        self.pretrain = pretrain
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.model, _, preprocess = open_clip.create_model_and_transforms(
                self.model_name, self.pretrain, device=self.device)
        self.tokenizer = open_clip.get_tokenizer(self.model_name)

    def calculate_embedding(self, text) -> EmbeddedQuery:
        return self.calculate(text, self.model, self.tokenizer, self.device)

    # TODO clip is not good about types; model/tokenizer can be various classes
    # without a common hierarchy :(
    def calculate(self, text: str, model: Any, tokenizer: Any, device: str):
        with torch.no_grad():
            tokenized_text = tokenizer([text]).to(device)
            text_features = model.encode_text(tokenized_text)
            text_features = text_features / text_features.norm(dim=-1, keepdim=True)
            return EmbeddedQuery(
                    query=text,
                    embedding=text_features.cpu().numpy().tolist()[0])


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
    height: int
    width: int
    mnsfw: float


class SearchFlavor(StrEnum):
    LEXICAL = "lexical"
    SEMANTIC = "semantic"
    HYBRID = "hybrid"

@lru_cache
def vectorize_query(qe: QueryEmbedder, q: str) -> list[float]:
    return qe.calculate_embedding(q).embedding


async def search(request: Request) -> Response:
    q = request.query_params.get('q', 'geocities')
    o = request.query_params.get('offset', "0")
    ps = request.query_params.get('page_size', str(DEFAULT_PAGE_SIZE))
    flavor = request.query_params.get('flavor', SearchFlavor.LEXICAL)
    mt = request.query_params.get('mnsfw', str(DEFAULT_MNSFW_THRESHOLD))
    page_size = DEFAULT_PAGE_SIZE
    offset = 0
    mnsfw_threshold = DEFAULT_MNSFW_THRESHOLD
    try:
        page_size = int(ps)
    except ValueError:
        pass

    try:
        offset = int(o)
    except ValueError:
        pass

    try:
        mnsfw_threshold = float(mt)
    except ValueError as e:
        pass

    if page_size > MAX_PAGE_SIZE:
        page_size = MAX_PAGE_SIZE

    request.state.logger.info(
    f"page size: {page_size} q: {q} flavor: {flavor} offset: {offset} mnsfw_threshold {mnsfw_threshold}")

    post_filter = {
            "range": {
                "mnsfw": {
                    "lte": mnsfw_threshold,
                    "gte": 0.0,
                },
            },
    }

    query_args = {
            'index': settings.ELASTICSEARCH_INDEX,
            'from': offset,
            'size': page_size,
            'post_filter': post_filter,
            }

    if flavor == SearchFlavor.SEMANTIC:
        query_args['knn'] = {
            "field": "vecs.vector",
            "query_vector": vectorize_query(request.state.query_embedder, q),
            # number of top results to pull from each shard's results (though
            # we have only one shard)
            "k": 1000,
            # candidates per shard; lower is faster/less accurate; we only have
            # one shard so there is no point in it differing from k
            "num_candidates": 1000,
        }
    elif flavor == SearchFlavor.LEXICAL:
        query_args['sort'] = 'page_count:desc'
        query_args['query'] = {
            "nested": {
            "path": "uses",
            "query": {
                "multi_match": {
                    "query": q,
                    "fields": ["uses.filename^3", "uses.path"]
                    }
                },
            }
        }
    elif flavor == SearchFlavor.HYBRID:
        query_args['query'] = {
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
        query_args['knn'] = {
            "field": "vecs.vector",
            "query_vector": vectorize_query(request.state.query_embedder, q),
            # number of top results to pull from each shard's results (though
            # we have only one shard)
            "k": 1000,
            # candidates per shard; lower is faster/less accurate; we only have
            # one shard so there is no point in it differing from k
            "num_candidates": 1000,
        }
    else:
        return HTMLResponse(content="unsupported search flavor", status_code=400)

    resp = request.state.es_client.search(**query_args)
    results: list[Gif] = []

    # TODO use async query to ES

    expected_mspec = f"{settings.EMBEDDING_MODEL}/{settings.EMBEDDING_PRETRAIN}"
    mspec = ""

    for h in resp['hits']['hits']:
        doc = h['_source']
        mspec = doc.get("mspec")

        results.append(Gif(
            checksum=h['_source']['checksum'],
            page_count=h['_source']['page_count'],
            width=h['_source']['width'],
            height=h['_source']['height'],
            mnsfw=h['_source']['mnsfw'],
            uses=[]))

    if mspec != "" and expected_mspec != mspec:
        request.state.logger.warn(f"mspec mismatch: expected {expected_mspec}, got {mspec}. This may degrade semantic search results")

    results.sort(key=lambda r: r.height, reverse=True)

    del resp['hits']['hits']
    request.state.logger.info(resp)

    ctx = {
        "q": q,
        "qu": urllib.parse.quote_plus(q),
        "results": results,
        "current_page": int(offset / page_size) + 1,
        "settings": settings,
        "offset": offset,
        "total_pages": int(resp['hits']['total']['value'] / page_size) + 1,
        "page_size": page_size,
        "flavor": flavor,
        "mnsfw": mnsfw_threshold,
    }

    return tmpls.TemplateResponse(request, "results.html", ctx)

async def detail(request: Request) -> Response:
    checksum = request.path_params['checksum']
    if len(checksum) != 32:
        return HTMLResponse(content="what?", status_code=400)

    # TODO validate checksum
    # TODO use async query to ES

    query = {
            "term": {
                "checksum": {
                    "value": checksum,
                    },
                },
            }

    # TODO is there a better way to do this...
    resp = request.state.es_client.search(
            index=settings.ELASTICSEARCH_INDEX,
            size=1,
            query=query)

    if resp['hits']['total']['value'] == 0:
        # TODO gif laden 404 page
        return HTMLResponse(content="idk that gif, sorry", status_code=404)

    result = resp['hits']['hits'][0]['_source']

    del resp['hits']['hits']
    request.state.logger.info(resp)

    uses = []
    for u in result['uses']:
        page = None
        if u['page'] != None:
            page = GeocitiesPage(url=u['page']['url'], timestamp=u['page']['timestamp'])

        uses.append(GifUse(url=u['url'],
                           timestamp=u['timestamp'],
                           path=u['path'],
                           filename=u['filename'],
                           page=page))

    ctx = {
        "settings": settings,
        "checksum": checksum,
        "page_count": result['page_count'],
        "mnsfw": result['mnsfw'],
        "uses": uses,
        "doc": json.dumps(result, sort_keys=True, indent=2),
    }

    return tmpls.TemplateResponse(request, "details.html", ctx)


class State(TypedDict):
    query_embedder: QueryEmbedder
    logger: logging.Logger
    es_client: Elasticsearch

@contextlib.asynccontextmanager
async def lifespan(app: Starlette) -> AsyncIterator[State]:

    es_client = Elasticsearch(
        settings.ELASTICSEARCH_URL,
        ca_certs=settings.ELASTICSEARCH_CERT,
        basic_auth=(settings.ELASTICSEARCH_USER, settings.ELASTICSEARCH_PASSWORD),
        request_timeout=settings.ELASTICSEARCH_TIMEOUT,
        )

    yield {'logger': logging.getLogger("gifcities"),
           'es_client': es_client,
           'query_embedder': QueryEmbedder(
        settings.EMBEDDING_MODEL, settings.EMBEDDING_PRETRAIN)}

app = Starlette(debug=settings.DEBUG, lifespan=lifespan, routes=[
    Route('/', index),
    Route('/about', about),
    Route('/favicon.ico', favicon),
    Route('/search', search),
    Route('/detail/{checksum}', detail),
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
