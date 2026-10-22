"""Executed inside client pod. Service tests use real ClusterIP TCP connections."""
import concurrent.futures, http.client, json, sys, time, urllib.parse, urllib.request, urllib.error

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
