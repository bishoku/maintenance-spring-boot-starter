"""Executed inside client pod. Service tests use real ClusterIP TCP connections."""
import base64, concurrent.futures, http.client, json, sys, time, urllib.parse, urllib.request, urllib.error

def request(url, data=None, timeout=5):
    start=time.monotonic()
    req=urllib.request.Request(url,data=None if data is None else json.dumps(data).encode(),
                               headers={'Content-Type':'application/json','Connection':'close'})
    try:
        with urllib.request.urlopen(req,timeout=timeout) as response:
            body=response.read().decode()
            try: body=json.loads(body)
            except ValueError: pass
            return {'status':response.status,'body':body,'elapsed':time.monotonic()-start}
    except urllib.error.HTTPError as error:
        return {'status':error.code,'body':error.read().decode(),'elapsed':time.monotonic()-start}
    except OSError as error:
        return {'status':0,'error':str(error),'elapsed':time.monotonic()-start}

def main(args):
    if args['op']=='rabbit-seed':
        base=args['url']
        headers={'Authorization':'Basic '+base64.b64encode(b'benchmark:benchmark-local-only').decode(),'Content-Type':'application/json'}
        def call(path,method,data=None):
            req=urllib.request.Request(base+path,method=method,headers=headers,data=None if data is None else json.dumps(data).encode())
            with urllib.request.urlopen(req,timeout=10) as response:
                body=response.read(); return json.loads(body) if body else {}
        call('/api/queues/%2F/maintenance-benchmark','PUT',{'durable':False,'auto_delete':False,'arguments':{}})
        call('/api/queues/%2F/maintenance-benchmark/contents','DELETE')
        responses=[call('/api/exchanges/%2F/amq.default/publish','POST',{'routing_key':'maintenance-benchmark','payload':token,'payload_encoding':'string','properties':{}}) for token in args['tokens']]
        return {'published':len(responses),'routed':all(r.get('routed') is True for r in responses)}
    if args['op']=='early-exit':
        end=time.monotonic()+60
        while time.monotonic()<end:
            status=request(args['url']+'/bench/state',timeout=1)
            if status['status']==200 and status['body'].get('stopInProgress'):
                return {'during':status,'exit':request(args['url']+'/actuator/maintenance',{'enabled':False},timeout=30)}
            time.sleep(.05)
        raise TimeoutError('startup stop barrier was not observed')
    if args['op']=='request': return request(args['url'],args.get('data'),args.get('timeout',5))
    if args['op']=='batch':
        # Each request has Connection: close; conntrack reuse cannot hide routing changes.
        return [request(args['url'],timeout=args.get('timeout',2)) for _ in range(args.get('count',30))]
    if args['op']=='persistent':
        url=urllib.parse.urlsplit(args['url'])
        connection=http.client.HTTPConnection(url.hostname,url.port,timeout=40)
        results=[]
        try:
            for path in ['/work?ms=15000','/work']:
                connection.request('GET',path)
                response=connection.getresponse()
                results.append({'status':response.status,'body':json.loads(response.read()),'will_close':response.will_close})
            return results
        finally: connection.close()
    raise ValueError(args['op'])
if __name__=='__main__': print(json.dumps(main(json.loads(sys.argv[1]))))
