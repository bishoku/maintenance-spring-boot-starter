"""Namespaced fixtures; no cluster-wide permissions, ingress or host ports."""

def service(name, selector, ports):
    return {'apiVersion':'v1','kind':'Service','metadata':{'name':name},
            'spec':{'selector':selector,'ports':[{'name':n,'port':p,'targetPort':p} for n,p in ports]}}

def pod(name, image, env=None, ports=(), command=None, args=None, ready=None, live=None, startup=None, labels=None, memory='256Mi'):
    container={'name':name,'image':image,'imagePullPolicy':'IfNotPresent',
               'env':[{'name':k,'value':str(v)} for k,v in (env or {}).items()],
               'ports':[{'containerPort':p} for p in ports],
               'resources':{'requests':{'cpu':'100m','memory':memory},'limits':{'memory':memory}}}
    for k,v in [('command',command),('args',args),('readinessProbe',ready),('livenessProbe',live),('startupProbe',startup)]:
        if v is not None: container[k]=v
    return {'apiVersion':'v1','kind':'Pod','metadata':{'name':name,'labels':labels or {'app':name}},
            'spec':{'automountServiceAccountToken':False,'terminationGracePeriodSeconds':20,'containers':[container]}}

def http_probe(path, period=1, failure=1):
    return {'httpGet':{'path':path,'port':18080},'periodSeconds':period,'timeoutSeconds':1,'failureThreshold':failure}

def infrastructure():
    kafka=pod('kafka','apache/kafka:3.9.1',{
        'KAFKA_NODE_ID':1,'KAFKA_PROCESS_ROLES':'broker,controller',
        'KAFKA_LISTENERS':'PLAINTEXT://:9092,CONTROLLER://:9093',
        'KAFKA_ADVERTISED_LISTENERS':'PLAINTEXT://kafka:9092',
        'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP':'PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT',
        'KAFKA_INTER_BROKER_LISTENER_NAME':'PLAINTEXT','KAFKA_CONTROLLER_LISTENER_NAMES':'CONTROLLER',
        'KAFKA_CONTROLLER_QUORUM_VOTERS':'1@localhost:9093',
        'KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR':1,'KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS':0,
        'KAFKA_HEAP_OPTS':'-Xms256m -Xmx512m'},[9092],memory='1Gi',
        ready={'exec':{'command':['/opt/kafka/bin/kafka-topics.sh','--bootstrap-server','localhost:9092','--list']},
               'periodSeconds':5,'timeoutSeconds':5,'failureThreshold':12})
    rabbit=pod('rabbit','rabbitmq:4.1.0-management',{
        'RABBITMQ_DEFAULT_USER':'benchmark','RABBITMQ_DEFAULT_PASS':'benchmark-local-only'},[5672],memory='512Mi',
        ready={'exec':{'command':['rabbitmq-diagnostics','-q','ping']},'periodSeconds':5,'timeoutSeconds':5,'failureThreshold':12})
    client=pod('client','python:3.12-alpine',command=['python','-c','import time; time.sleep(86400)'],memory='64Mi')
    kafka_service=service('kafka',{'app':'kafka'},[('kafka',9092)])
    # Kafka admin readiness resolves advertised metadata through this bootstrap Service.
    # Only the broker bootstrap bypasses readiness; application Service keeps defaults.
    kafka_service['spec']['publishNotReadyAddresses']=True
    return [kafka_service,kafka,
            service('rabbit',{'app':'rabbit'},[('amqp',5672)]),rabbit,client,
            service('sample',{'app':'maintenance-sample'},[('http',18080)])]

def applications(image):
    pods = [pod(name,image,{
        'POD_NAME':name,'SERVER_ADDRESS':'0.0.0.0','SERVER_PORT':18080,
        'SPRING_KAFKA_BOOTSTRAP_SERVERS':'kafka:9092',
        'SPRING_RABBITMQ_HOST':'rabbit','SPRING_RABBITMQ_PORT':5672,
        'MAINTENANCE_DRAIN_DELAY':'1s','MAINTENANCE_QUEUE_TIMEOUT':'10s'},[18080],
        ready=http_probe('/actuator/health/readiness'),
        live=http_probe('/actuator/health/liveness',period=2,failure=3),
        startup=http_probe('/actuator/health/liveness',period=2,failure=90),
        labels={'app':'maintenance-sample'},memory='512Mi') for name in ['sample-a','sample-b']]

    for item in pods:
        item['spec']['securityContext']={'runAsUser':10001,'runAsGroup':10001,'fsGroup':10001}
        item['spec']['volumes']=[{'name':'application','emptyDir':{}}]
        container=item['spec']['containers'][0]
        container['volumeMounts']=[{'name':'application','mountPath':'/app'}]
        container['command']=['sh','-c','until [ -f /app/.ready ]; do sleep 1; done; exec java -XX:MaxRAMPercentage=65 -jar /app/benchmark.jar']
    return pods
