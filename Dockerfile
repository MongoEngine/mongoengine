FROM mongo:5

COPY ./entrypoint.sh entrypoint.sh
RUN chmod u+x entrypoint.sh
ENTRYPOINT ./entrypoint.sh
