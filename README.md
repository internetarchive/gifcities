Wayback Machine Site Search
===========================

## Start Redis

```
#on wbgrp-svc060
sudo docker run --name gifcities -p 6379:6379 -d redis
```

On each app node, do the following:
## Installation

```
#git clone repo and then
cd gifcities/
cd /var/tmp/
git clone git@git.archive.org:vinay/gifcities.git
cd gifcities
docker_image_tag=gifcities:backend
sudo docker build -t $docker_image_tag .
```

### Run backend service

```
GIFINDEX=http://wwwb-search01.us.archive.org:9200/geocities-gifs/
REDISHOST=wbgrp-svc060
REDISPORT=6379
sudo docker run -i -t -p 8091:8091 -v /var/tmp/gifcities:/var/tmp/gifcities/ -e GIFINDEX=$GIFINDEX -e REDISHOST=$REDISHOST -e REDISPORT=$REDISPORT  $docker_image_tag

```
