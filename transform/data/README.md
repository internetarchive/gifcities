hi, I'm Nate, how are you?

This directory is empty if you just cloned this project. The primary file you'll want in here is called `gifcities-gifs.txt` and as of 2024 September there was a copy at [http://nsmith-dev.us.archive.org/gifcities-gifs.txt](nsmith-dev).

This file is the base manifest for the content at [https://gifcities.org](gifcities).

It is used to derive `gifcities.jsonl` which in turn is used to populate elasticsearch.

The actual images are stored in scholar's s3 servers on `svc171` and `svc314` in a bucket called `gifcities`. Each gif is stored with its checksum as a key.

The rough flow for going from manifest->searchable (from the `transform` directory):

- `go run . manifest` to create `data/gifcities.jsonl`
- `./ingest.sh` to:
  - create ES index
  - upload all the gif records

If you want to change the schema that goes into elasticsearch, edit `main.go` to output different stuff to `gifcities.jsonl`.

Note that configuration for the index (ie, mapping) is in `ingest.sh`
