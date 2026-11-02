FROM mcr.microsoft.com/dotnet/aspnet:10.0
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates iproute2 iptables libfaketime coreutils \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY server/ /app/server/
COPY client/ /app/client/
COPY start.sh /app/start.sh
RUN chmod +x /app/start.sh
ENTRYPOINT ["/app/start.sh"]
