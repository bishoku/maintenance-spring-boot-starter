#!/usr/bin/env python3
"""Real MicroK8s Service/EndpointSlice benchmark, isolated in a new namespace."""
import xml.etree.ElementTree as ET
import argparse, concurrent.futures, threading, datetime, hashlib, json, pathlib, re, subprocess, sys, time, zipfile
from manifest import infrastructure, applications
ROOT=pathlib.Path(__file__).resolve().parents[2]
STARTER_VERSION=ET.parse(ROOT/'pom.xml').getroot().find('{http://maven.apache.org/POM/4.0.0}version').text
CLIENT=(ROOT/'benchmark/kubernetes/client.py').read_text()

class Runner:
    def __init__(self,args):
        self.args=args
        self.out=ROOT/'benchmark/reports'/args.label
        self.out.mkdir(parents=True,exist_ok=False)
        self.namespace='maintenance-bench-'+args.label
        self.report={'contract':'kubernetes-readiness-v2','timestamp':datetime.datetime.now(datetime.timezone.utc).isoformat(),
                     'deployment':'public-jre-emptydir-jar-upload','namespace':self.namespace,'versions':args.versions,'results':[]}
        self.created=False
        self.counter=0
        self.log_lock=threading.Lock()
    def save(self):
        (self.out/'results.json').write_text(json.dumps(self.report,indent=2))
        rows=['# '+self.args.label,'','Contract: Kubernetes owns traffic routing; the library only signals readiness.','',
              '| Version | Check | Result |','|---|---|---|']
        rows += [f"| {r['version']} | {r['test']} | {'PASS' if r['passed'] else 'FAIL'} |" for r in self.report['results']]
        if 'error' in self.report: rows += ['', 'Run error: '+self.report['error']]
        (self.out/'summary.md').write_text('\n'.join(rows)+'\n')
    def record(self,version,name,ok,detail=None):
        self.report['results'].append({'version':version,'test':name,'passed':bool(ok),'detail':detail})
        print(version,name,'PASS' if ok else 'FAIL',flush=True); self.save()
    def cmd(self,command,input=None,timeout=300):
        with self.log_lock:
            self.counter+=1
            counter=self.counter
        result=subprocess.run(command,cwd=ROOT,input=input,text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=timeout)
        (self.out/f'command-{counter:04d}.log').write_text(' '.join(command)+'\n'+result.stdout+'\n'+result.stderr)
        if result.returncode: raise RuntimeError(f"Command failed ({result.returncode}): {' '.join(command[:8])}; see command-{counter:04d}.log: {result.stderr[-1200:]}")
        return result.stdout
    def kubectl(self,*args,input=None,timeout=120):
        return self.cmd(['microk8s','kubectl','--request-timeout=60s','-n',self.namespace,*args],input,timeout)
    def get(self,kind,name=None):
        return json.loads(self.kubectl('get',kind,*([name] if name else []),'-o','json'))
    def rpc(self,op,**kwargs):
        return json.loads(self.kubectl('exec','client','--','python','-c',CLIENT,json.dumps({'op':op,**kwargs}),timeout=90))
    def http(self,base,path,data=None): return self.rpc('request',url=base+path,timeout=30,**({} if data is None else {'data':data}))
    def wait(self,predicate,timeout=60):
        end=time.monotonic()+timeout
        while time.monotonic()<end:
            if predicate(): return
            time.sleep(.4)
        raise TimeoutError('condition not observed within '+str(timeout)+' seconds')
    def ready_names(self):
        slices=self.get('endpointslices')
        return {e['targetRef']['name'] for s in slices['items']
                if s['metadata']['labels'].get('kubernetes.io/service-name')=='sample'
                for e in s.get('endpoints',[]) if e.get('conditions',{}).get('ready') is True}
    def snapshot(self,version,label):
        data={'pods':self.get('pods'),'slices':self.get('endpointslices')}
        (self.out/(version+'-'+label+'.json')).write_text(json.dumps(data,indent=2))
        return data
    def apply(self,items):
        self.kubectl('apply','-f','-',input=json.dumps({'apiVersion':'v1','kind':'List','items':items}))
    def build(self):
        self.cmd(['mvn','-B','-Dspring-boot.version=3.5.9','-Dmaven.test.skip=true','-Dmaven.javadoc.skip=true','clean','install'],timeout=600)
    def app_image(self,version):
        command=['mvn','-B','-f','benchmark/sample/pom.xml',f'-Dmaintenance.version={STARTER_VERSION}',f'-Dspring-boot.version={version}','clean','package']
        if version.startswith('4.'): command.append('-Dboot4')
        self.cmd(command,timeout=600)
        with zipfile.ZipFile(ROOT/'benchmark/sample/target/benchmark.jar') as jar:
            name=next(n for n in jar.namelist() if n.startswith('BOOT-INF/lib/maintenance-spring-boot-starter-'))
            self.report.setdefault('consumed_starter_sha256',{})[version]=hashlib.sha256(jar.read(name)).hexdigest()
        jar=ROOT/'benchmark/sample/target/benchmark.jar'
        self.report.setdefault('sample_sha256',{})[version]=hashlib.sha256(jar.read_bytes()).hexdigest()
        return 'eclipse-temurin:17-jre-jammy'
    def scenario(self,version):
        items=applications(self.app_image(version))
        for item in items:
            for env in item['spec']['containers'][0]['env']:
                if env['name']=='SPRING_KAFKA_BOOTSTRAP_SERVERS': env['value']=self.kafka_ip+':9092'
                if env['name']=='SPRING_RABBITMQ_HOST': env['value']=self.rabbit_ip
        self.apply(items)
        for name in ['sample-a','sample-b']:
            self.wait(lambda:self.get('pod',name)['status']['phase']=='Running',timeout=300)
            self.kubectl('cp','benchmark/sample/target/benchmark.jar',name+':/app/benchmark.jar')
            actual=self.kubectl('exec',name,'--','sha256sum','/app/benchmark.jar').split()[0]
            self.record(version,name+'-jar-integrity',actual==self.report['sample_sha256'][version],actual)
            self.kubectl('exec',name,'--','touch','/app/.ready')
        self.kubectl('wait','pod/sample-a','pod/sample-b','--for=condition=Ready','--timeout=180s',timeout=200)
        self.wait(lambda:self.ready_names()=={'sample-a','sample-b'})
        before=self.snapshot(version,'before')
        pods={p['metadata']['name']:p for p in before['pods']['items'] if p['metadata']['name'].startswith('sample-')}
        urls={n:'http://'+p['status']['podIP']+':18080' for n,p in pods.items()}
        service='http://'+self.get('service','sample')['spec']['clusterIP']+':18080'
        def sample(expected,label,count=40):
            responses=self.rpc('batch',url=service+'/work',count=count)
            seen={r.get('body',{}).get('instance') for r in responses if r['status']==200 and isinstance(r.get('body'),dict)}
            self.record(version,label,all(r['status']==200 for r in responses) and seen==expected,{'seen':sorted(seen),'responses':responses})
        def enter(name,expected):
            started=time.monotonic()
            response=self.http(urls[name],'/actuator/maintenance',{'enabled':True,'reason':'Kubernetes benchmark'})
            self.record(version,name+'-enter',response['status']==200 and response['body'].get('active') is True,response)
            self.wait(lambda:self.ready_names()==expected)
            self.record(version,name+'-endpointslice-ready-set',True,{'ready':sorted(expected),'observed_seconds_since_command':time.monotonic()-started})
        def management_read(name,label):
            response=self.http(urls[name],'/actuator/maintenance')
            body=response.get('body')
            self.record(version,label,name not in self.ready_names()
                        and response['status']==200 and isinstance(body,dict)
                        and body.get('active') is True,response)
        def exit(name,expected):
            management_read(name,name+'-management-readable-before-exit')
            response=self.http(urls[name],'/actuator/maintenance',{'enabled':False})
            body=response.get('body',{})
            self.record(version,name+'-exit',response['status']==200 and isinstance(body,dict)
                        and body.get('active') is False
                        and body.get('details',{}).get('phase')=='INACTIVE'
                        and body.get('details',{}).get('partialFailure') is False,response)
            status=self.http(urls[name],'/actuator/maintenance')
            readiness=self.http(urls[name],'/actuator/health/readiness')
            self.record(version,name+'-exit-confirmed-by-management-and-readiness',
                        status['status']==200 and isinstance(status.get('body'),dict)
                        and status['body'].get('active') is False
                        and status['body'].get('details',{}).get('phase')=='INACTIVE'
                        and readiness['status']==200,{'management':status,'readiness':readiness})
            self.wait(lambda:self.ready_names()==expected)
        sample({'sample-a','sample-b'},'service-reaches-both-ready-pods')
        enter('sample-a',{'sample-b'})
        snap=self.snapshot(version,'one-unready')
        p=next(p for p in snap['pods']['items'] if p['metadata']['name']=='sample-a')
        self.record(version,'kubelet-observes-not-ready',any(c['type']=='Ready' and c['status']=='False' for c in p['status']['conditions']),p['status']['conditions'])
        self.record(version,'readiness-503-liveness-200',self.http(urls['sample-a'],'/actuator/health/readiness')['status']==503 and self.http(urls['sample-a'],'/actuator/health/liveness')['status']==200)
        self.record(version,'unready-pod-direct-http-still-200',self.http(urls['sample-a'],'/work')['status']==200)
        sample({'sample-b'},'service-excludes-maintenance-pod')
        management_read('sample-a','management-readable-while-service-excludes-pod')
        self.http(urls['sample-a'],'/bench/mode/exit-fail',{})
        response=self.http(urls['sample-a'],'/actuator/maintenance',{'enabled':False})
        self.record(version,'failed-exit-retains-readiness-refusal',response['body'].get('details',{}).get('phase')=='FAILED' and self.http(urls['sample-a'],'/actuator/health/readiness')['status']==503,response)
        sample({'sample-b'},'failed-exit-does-not-rejoin-service')
        self.http(urls['sample-a'],'/bench/mode/ok',{});exit('sample-a',{'sample-a','sample-b'})
        sample({'sample-a','sample-b'},'service-restores-recovered-pod')
        enter('sample-b',{'sample-a'})
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            existing=pool.submit(self.rpc,'persistent',url=service)
            self.wait(lambda:self.http(urls['sample-a'],'/bench/state')['body']['work']>=1,timeout=10)
            enter('sample-a',set())
            self.snapshot(version,'all-unready')
            new_requests=self.rpc('batch',url=service+'/work',count=3,timeout=2)
            self.record(version,'no-ready-endpoints-no-new-service-traffic',all(r['status']!=200 for r in new_requests),new_requests)
            self.record(version,'both-unready-pods-remain-directly-reachable',all(self.http(urls[n],'/work')['status']==200 for n in urls))
            responses=existing.result(timeout=40)
            self.record(version,'existing-request-not-aborted',responses[0]['status']==200 and responses[0]['body']['instance']=='sample-a',responses[0])
            self.record(version,'established-keepalive-can-still-reach-unready-pod',not responses[0]['will_close'] and responses[1]['status']==200 and responses[1]['body']['instance']=='sample-a',responses)
        after=self.snapshot(version,'still-running')
        for name,original in pods.items():
            current=next(p for p in after['pods']['items'] if p['metadata']['name']==name)
            self.record(version,name+'-no-restart-or-replacement',current['metadata']['uid']==original['metadata']['uid']
                        and current['status']['phase']=='Running'
                        and [c['restartCount'] for c in current['status']['containerStatuses']]==[c['restartCount'] for c in original['status']['containerStatuses']])
        exit('sample-a',{'sample-a'});sample({'sample-a'},'single-pod-recovery-routes-again')
        exit('sample-b',{'sample-a','sample-b'});sample({'sample-a','sample-b'},'full-recovery-routes-again')
        self.snapshot(version,'recovered')
    def run(self):
        try:
            status=self.cmd(['microk8s','status'],timeout=30)
            self.report['initial_microk8s_status']=status
            if 'microk8s is running' not in status:
                if not self.args.start: raise RuntimeError('MicroK8s is stopped. Start it in your terminal (sudo microk8s start), or pass --start if permitted.')
                self.cmd(['microk8s','start'],timeout=60)
            self.wait(lambda: 'microk8s is running' in self.cmd(['microk8s','status'],timeout=30),timeout=120)
            self.report['cluster_version']=json.loads(self.cmd(['microk8s','kubectl','version','-o','json']))
            # create, never apply/reuse an existing namespace
            self.cmd(['microk8s','kubectl','create','namespace',self.namespace]);self.created=True
            self.build()
            items=infrastructure()
            self.apply([item for item in items if item['kind']=='Service'])
            self.kafka_ip=self.get('service','kafka')['spec']['clusterIP']
            self.rabbit_ip=self.get('service','rabbit')['spec']['clusterIP']
            for item in items:
                if item['kind']=='Pod' and item['metadata']['name']=='kafka':
                    for env in item['spec']['containers'][0]['env']:
                        if env['name']=='KAFKA_ADVERTISED_LISTENERS': env['value']='PLAINTEXT://'+self.kafka_ip+':9092'
            self.apply([item for item in items if item['kind']=='Pod'])
            self.kubectl('wait','pod/kafka','pod/rabbit','pod/client','--for=condition=Ready','--timeout=180s',timeout=200)
            for version in self.args.versions:
                self.scenario(version)
                self.kubectl('delete','pod','sample-a','sample-b','--wait=true','--timeout=90s')
            self.report['completed']=True
        except Exception as error:
            self.report['error']=str(error); self.report['completed']=False
            print('ERROR:',error,flush=True)
            if self.created:
                try:
                    self.snapshot('failure','diagnostics')
                    for pod in self.get('pods')['items']:
                        self.kubectl('logs',pod['metadata']['name'],'--tail=150')
                except Exception: pass
        finally:
            if self.created and not self.args.keep:
                try:
                    self.cmd(['microk8s','kubectl','delete','namespace',self.namespace,'--wait=true','--timeout=120s'],timeout=140)
                    self.report['namespace_cleaned']=True
                except Exception as error: self.report['cleanup_error']=str(error)
            self.save()
        return 0 if self.report.get('completed') and all(r['passed'] for r in self.report['results']) and 'cleanup_error' not in self.report else 1

def main():
    p=argparse.ArgumentParser();p.add_argument('--label',required=True);p.add_argument('--versions',nargs='+',default=['3.5.9','4.0.2'])
    p.add_argument('--start',action='store_true');p.add_argument('--keep',action='store_true',help='retain test namespace for debugging')
    a=p.parse_args()
    if not re.fullmatch('[a-z0-9][a-z0-9-]{0,30}',a.label): p.error('label: lowercase alphanumeric/hyphen, maximum 31 chars')
    if any(not re.fullmatch(r'[0-9]+\.[0-9]+\.[0-9]+(?:[-.][A-Za-z0-9]+)*',v) for v in a.versions): p.error('invalid version')
    return Runner(a).run()
if __name__=='__main__': sys.exit(main())
