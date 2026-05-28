1. mqtt(cella frigo) -> mosquitto ->influxdb
2. coap->gateway(fabbrica) -> gateway->influxdb
 entrambi i dati va ad amqp in code specifiche (rabbitmq fa da server amqp)  rabbit con excenge,virtualhost e code di allarme e ok per la visualizzazione 
 sottoscrittori alla code per inviare una mail,lettura code con nodered  utilizzare anche traefik per la gestione della struttura 


 