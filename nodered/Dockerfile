FROM nodered/node-red:latest

USER root
RUN npm install --prefix /usr/src/node-red \
    node-red-contrib-influxdb \
    node-red-dashboard \
    node-red-node-email

USER node-red
