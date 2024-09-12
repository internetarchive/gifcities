#!/bin/env bash

# this file depends on gifcities.jsonl which is in turn based on gifcities-gifs.txt.
#
# The latter is the original gifs manifest file I was handed when I started
# this project. It's been hand edited to update the snapshot to a gif to one
# that exists in wayback since the old snapshot was lost to bitrot.
#
# gifcities.jsonl is derived from gifcities-gifs.txt. it collates checksums
# together and tracks each use of a given gif in a `uses` key. The schema used
# here is the schema we want to index in elasticsearch. If you want to change
# the ES schema, rederive this file.
#
# gifcities.jsol is create with the Go code in this directory. To regenerate it:
#
# go run . manifest

set -e

if [ -z "$ES_URL" ]; then
  ES_URL=http://localhost:9200
fi

if [ -z "$ES_INDEX" ]; then
  ES_INDEX=gifcities
fi

es_cmd="curl $ES_URL/$ES_INDEX"

if grep "https" ES_URL; then
  if [ -z "$ES_AUTH" ]; then
    echo "you probably want to set ES_AUTH"
    exit
  fi

  if [ -z "$ES_CERT" ]; then
    echo "you probably want to set ES_AUTH"
    exit
  fi

  es_cmd="curl -u $ES_AUTH --cacert $ES_CERT $ES_URL/$ES_INDEX"
fi

if [ ! -f ./data/gifcities.jsonl ]; then
  echo "expected to see gifcities.jsonl in ./data; aborting"
  exit
fi

printf "ES_URL=%s\n" $ES_URL
printf "ES_INDEX=%s\n" $ES_INDEX
echo
echo "this will:"
echo "- create $ES_INDEX index at $ES_URL"
echo "- create ./tmp"
echo "- prepare bulk upload .jsonl file"
echo "- split that file up into 1000 line chunk files"
echo "- upload each of those to $ES_URL/$INDEX"
echo
echo "if previously run you may want to 'curl -XDELETE $ES_URL/$ES_INDEX'"
echo
echo "interstitial files are kept and reused for idempotency; to start clean: 'rm -rf tmp'"
echo
read -p "wanna do it? " -n 1 -r
echo
if [[ $REPLY =~ ^[Nn]$ ]]
then
  echo "i'm out"
  exit
fi

echo "Creating index..."

$es_cmd -XPUT -H 'Content-Type: application/json' -d'
{
  "mappings": {
    "properties": {
      "vecs": {
        "type": "nested",
        "properties": {
          "vector": {
            "type": "dense_vector"
          }
        }
      },
      "uses": {
        "type": "nested"
      }
    }
  }
}
'

mkdir -p tmp
cd tmp

if [ -f bulk.jsonl ]; then
  echo "reusing existing bulk.jsonl"
else
  echo "creating bulk.jsonl"
  cat ../data/gifcities.jsonl | while read x; do \
    echo '{"index":{}}' >> bulk.jsonl; \
    echo $x >> bulk.jsonl; \
  done
fi

if [ -f xaa ]; then
  echo "looks like bulk.jsonl already split"
else
  echo "splitting bulk.jsonl"
  split bulk.jsonl
fi

echo "POSTing to index"

ls x* | while read x; do \
  curl -v -H "Content-Type: application/x-ndjson"    \
  -XPOST $ES_URL/$ES_INDEX/_bulk --data-binary "@$x"; \
done
