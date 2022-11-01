FROM clojure:openjdk-14-slim-buster

ENV CLOJURE_VERSION=1.10.1.619
WORKDIR /tmp
RUN \
apt-get update && \
apt-get install -y curl make rlwrap wget && \
rm -rf /var/lib/apt/lists/* && \
wget https://download.clojure.org/install/linux-install-$CLOJURE_VERSION.sh && \
sha256sum linux-install-$CLOJURE_VERSION.sh && \
echo "28b1652686426cdf856f83551b8ca01ff949b03bc9a533d270204d6511a8ca9d *linux-install-$CLOJURE_VERSION.sh" | sha256sum -c - && \
chmod +x linux-install-$CLOJURE_VERSION.sh && \
./linux-install-$CLOJURE_VERSION.sh

RUN \ 
rm ./linux-install-$CLOJURE_VERSION.sh

RUN \ 
wget https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein -O /bin/lein && \ 
chmod uog+x /bin/lein

# Cleanup
RUN apt-get purge -y --auto-remove curl wget