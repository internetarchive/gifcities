# gifcities.org

This repository has code for:

- the (new) gifcities.org web app
- tools for translating a large manifest of gifs into a form suitable for ingestion into ES

This code isn't much use on its own as it needs a running elasticsearch 8.15 host as well as access to s3 compatible storage full of gifs.

It also needs access to the gif manifest which can be found on my dev machine (`~nsmith/gifcities-gifs.txt`).

for local development I used docker:

```
docker network create gifcities
docker pull elasticsearch:8.15.0
docker run --name es01 --net gifcities -p 9200:9200 -it -m 1GB docker.elastic.co/elasticsearch/elasticsearch:8.15.0
```

seaweedfs hosted gifs are accessible (for now) via `blobs.fatcat.wiki/gifcities` using gif checksum as key.

to run the web app locally: `make serve`.

a venv will be created at `.venv` in the root of the repo.
