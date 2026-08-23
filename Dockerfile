FROM mongo:4.4.30

COPY ./entrypoint.sh entrypoint.sh
RUN chmod u+x entrypoint.sh
ENTRYPOINT ./entrypoint.sh
