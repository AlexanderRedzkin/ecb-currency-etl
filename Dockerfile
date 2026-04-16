FROM ubuntu:latest
LABEL authors="alexanderedkin"

ENTRYPOINT ["top", "-b"]