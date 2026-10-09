ARG BUILD_FROM
FROM ${BUILD_FROM}

# Node One build of the IDisposable Wyze bridge (fork). Node One runs the
# bridge in external go2rtc mode, so only the bridge and its Gwell helper
# ship here: Node One's own go2rtc is the video engine. ffmpeg carries the
# helper's RTSP publish into that engine.
RUN apk add --no-cache ca-certificates tzdata ffmpeg && \
    mkdir -p /config /media/wyze_bridge/snapshots /media/wyze_bridge/recordings

COPY bin/wyze-bridge /usr/local/bin/wyze-bridge
COPY bin/gwell-proxy /usr/local/bin/gwell-proxy
COPY run.sh /
RUN chmod +x /run.sh /usr/local/bin/wyze-bridge /usr/local/bin/gwell-proxy

CMD [ "/run.sh" ]
