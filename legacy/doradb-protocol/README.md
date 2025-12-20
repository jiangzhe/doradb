# doradb Protocol

This is implementation of mysql on-wire protocol for doradb server.

Start a local docker image for unit tests.

```bash
docker run -it -d --rm -p 13306:3306 -e MYSQL_ROOT_PASSWORD=password mysql:8.0.30-debian
```