ARG BUILD_FROM
FROM ${BUILD_FROM}

# This build runs the bridge in external go2rtc mode (GO2RTC_URL), so only
# the bridge and its Gwell helper ship here; the go2rtc it feeds runs
# elsewhere. ffmpeg carries the helper's RTSP publish into that go2rtc.
RUN apk add --no-cache ca-certificates tzdata ffmpeg && \
    mkdir -p /config /media/wyze_bridge/snapshots /media/wyze_bridge/recordings

COPY bin/wyze-bridge /usr/local/bin/wyze-bridge
COPY bin/gwell-proxy /usr/local/bin/gwell-proxy
COPY run.sh /
RUN chmod +x /run.sh /usr/local/bin/wyze-bridge /usr/local/bin/gwell-proxy

CMD [ "/run.sh" ]
