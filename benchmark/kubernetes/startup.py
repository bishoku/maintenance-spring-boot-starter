#!/usr/bin/env python3
"""Startup maintenance acceptance tests using real MicroK8s brokers and a pinned binary."""
import argparse, json, os, re, sys, time, uuid
from run import Runner
from manifest import applications

class StartupRunner(Runner):
    def scenario(self,version):
        image=self.app_image(version)
        self.kubectl('exec','kafka','--','/opt/kafka/bin/kafka-topics.sh','--bootstrap-server','localhost:9092','--create','--if-not-exists','--topic','maintenance-benchmark','--partitions','1','--replication-factor','1')
        rabbit='http://'+self.get('pod','rabbit')['status']['podIP']+':15672'
        cases=[
            ('flat',{},['--maintenance.start-in-maintenance=true']),
            ('nested',{},['--maintenance.startup.enabled=true']),
            ('env-flat',{'MAINTENANCE_START_IN_MAINTENANCE':'true'},[]),
            ('env-nested',{'MAINTENANCE_STARTUP_ENABLED':'true'},[]),
            ('cold-window',{'MAINTENANCE_START_IN_MAINTENANCE':'true','BENCHMARK_STARTED_DELAY_MS':'2000'},[]),
            ('queue-failure',{'MAINTENANCE_START_IN_MAINTENANCE':'true','BENCHMARK_INITIAL_MODE':'queue-fail'},[]),
            ('early-exit',{'MAINTENANCE_START_IN_MAINTENANCE':'true','BENCHMARK_QUEUE_STOP_DELAY_MS':'8000'},[]),
            ('property-override',{'MAINTENANCE_START_IN_MAINTENANCE':'true'},['--maintenance.start-in-maintenance=false']),
        ]
        self.report['startup_strategy']='deferred' if self.args.expect_deferred_startup else 'pause'
        if self.args.cases:
            cases=[item for item in cases if item[0] in self.args.cases]
        self.report['cases']=[item[0] for item in cases]
        for case,extra,arguments in cases:
            prefix=version+'-'+case
            tokens=[prefix+'-'+str(uuid.uuid4()) for _ in range(8)]
            seed=self.rpc('rabbit-seed',url=rabbit,tokens=tokens)
            if not seed['routed']: raise RuntimeError('Rabbit seed was not routed')
            self.kubectl('exec','-i','kafka','--','/opt/kafka/bin/kafka-console-producer.sh','--bootstrap-server','localhost:9092','--topic','maintenance-benchmark',input='\n'.join(tokens)+'\n')
            self.report.setdefault('seeded_messages',{})[prefix]=tokens
            item=applications(image)[0]
            env={e['name']:e['value'] for e in item['spec']['containers'][0]['env']}
            env.update({'SPRING_KAFKA_BOOTSTRAP_SERVERS':self.kafka_ip+':9092','SPRING_RABBITMQ_HOST':self.rabbit_ip,
                        'BENCHMARK_KAFKA_GROUP':'startup-'+str(uuid.uuid4()),'BENCHMARK_STARTUP_PROBE':'true',
                        'MAINTENANCE_STARTUP_REASON':'review-'+case})
            if version.startswith('2.'): env['SPRING_PROFILES_ACTIVE']='boot2'
            env.update(extra)
            container=item['spec']['containers'][0]
            container['env']=[{'name':k,'value':v} for k,v in env.items()]
            container['command']=['sh','-c','until [ -f /app/.ready ]; do sleep 1; done; exec java -XX:MaxRAMPercentage=65 -jar /app/benchmark.jar "$@"','startup-probe',*arguments]
            self.apply([item])
            self.wait(lambda:self.get('pod','sample-a')['status']['phase']=='Running',timeout=180)
            self.kubectl('cp','benchmark/sample/target/benchmark.jar','sample-a:/app/benchmark.jar')
            actual=self.kubectl('exec','sample-a','--','sha256sum','/app/benchmark.jar').split()[0]
            if actual!=self.report['sample_sha256'][version]: raise RuntimeError('Pod sample JAR differs from local sample')
            url='http://'+self.get('pod','sample-a')['status']['podIP']+':18080'
            service='http://'+self.get('service','sample')['spec']['clusterIP']+':18080'
            self.kubectl('exec','sample-a','--','touch','/app/.ready')
            early=self.rpc('early-exit',url=url) if case=='early-exit' else None
            def diagnostics():
                body=self.http(url,'/bench/startup').get('body')
                return body if isinstance(body,dict) else {}
            self.wait(lambda:diagnostics().get('ready') is True,timeout=120)
            self.report.setdefault('pod_java',{})[version]=self.kubectl('exec','sample-a','--','java','--version')
            time.sleep(1)
            state=self.http(url,'/actuator/maintenance')
            business=self.http(url,'/bench/state')['body']
            diag=diagnostics()
            probe=self.http(url,'/actuator/health/readiness')
            pod_before=self.get('pod','sample-a')
            snapshot={'maintenance':state,'business':business,'diagnostics':diag,'readiness':probe,'early_exit':early,
                      'pod':pod_before,'slices':self.get('endpointslices')}
            (self.out/(prefix+'-startup.json')).write_text(json.dumps(snapshot,indent=2))
            def check(name,passed,detail=None): self.record(version,case+'/'+name,passed,detail)
            check('management-accessible',state['status']==200,state)
            check('liveness-healthy',self.http(url,'/actuator/health/liveness')['status']==200)
            check('startup-scheduler-obeys-config-from-first-tick',diag.get('firstTickBlocked') is (case!='property-override') and diag.get('ticks',0)>0,diag)
            if case in ('early-exit','property-override'):
                check('inactive-state-after-exit-or-explicit-false',state['body'].get('active') is False and state['body'].get('details',{}).get('phase')=='INACTIVE',state)
                check('readiness-accepts',probe['status']==200,probe)
                check('consumers-running-after-exit-or-explicit-false',business['kafka']['running'] and not business['kafka']['paused'] and business['rabbit']['running'],business)
                check('service-routes-after-exit-or-explicit-false',self.rpc('request',url=service+'/work',timeout=2)['status']==200)
                if early: check('early-management-exit-accepted',early['exit']['status']==200 and early['exit']['body'].get('active') is False,early)
            else:
                expected_phase='FAILED' if case=='queue-failure' else 'ACTIVE'
                check('startup-phase-reports-outcome',state['body'].get('details',{}).get('phase')==expected_phase,state)
                if case=='queue-failure':
                    check('startup-error-visible-in-management',state['body'].get('details',{}).get('partialFailure') is True,state)
                check('startup-reason-bound',state['body'].get('reason')=='review-'+case,state['body'])
                check('readiness-refuses',probe['status']==503,probe)
                check('no-eligible-service-backends',self.ready_names()==set(),sorted(self.ready_names()))
                check('no-new-service-traffic',self.rpc('request',url=service+'/work',timeout=2)['status']!=200)
                if self.args.expect_deferred_startup:
                    check('kafka-and-rabbit-startup-deferred',not business['kafka']['running'] and not business['rabbit']['running'],business)
                else:
                    check('kafka-paused-and-rabbit-stopped',business['kafka']['running'] and business['kafka']['paused'] and not business['rabbit']['running'],business)
                observed=[e for e in diag.get('consumed',[]) if e['message'] in tokens]
                for broker in ['kafka','rabbit']:
                    check(broker+'-no-cold-start-consumption',not any(e['broker']==broker for e in observed),observed)
                check('gated-scheduled-work-not-executed',diag.get('executed')==0,diag)
                if self.args.expect_deferred_startup:
                    check('user-disabled-listeners-remain-stopped',not business['kafka']['idleRunning'] and not business['rabbit']['idleRunning'],business)
            if case=='early-exit' and not (business['kafka']['running'] and not business['kafka']['paused'] and business['rabbit']['running']):
                repeated=self.http(url,'/actuator/maintenance',{'enabled':False})
                after_repeat=self.http(url,'/bench/state')['body']
                check('repeated-exit-restores-consumers',after_repeat['kafka']['running'] and not after_repeat['kafka']['paused'] and after_repeat['rabbit']['running'],{'exit':repeated,'business':after_repeat})
                self.kubectl('logs','sample-a','--tail=250')
                self.kubectl('delete','pod','sample-a','--wait=true','--timeout=90s')
                continue
            # A new backlog must remain unconsumed after the settled startup phase.
            self.http(url,'/bench/mode/ok',{})
            pending=prefix+'-pending-'+str(uuid.uuid4())
            response=self.http(url,'/bench/send/'+pending,{})
            check('messages-enqueued',response['status']==200,response)
            time.sleep(.5)
            before_exit=self.http(url,'/bench/state')['body']
            if case not in ('early-exit','property-override'):
                check('settled-maintenance-does-not-consume',all(pending not in before_exit[b]['messages'] for b in ['kafka','rabbit']),before_exit)
            result=self.http(url,'/actuator/maintenance',{'enabled':False})
            check('management-exit-completes',result['status']==200 and result['body'].get('active') is False and result['body'].get('details',{}).get('phase')=='INACTIVE' and result['body'].get('details',{}).get('partialFailure') is False,result)
            self.wait(lambda:all(pending in self.http(url,'/bench/state')['body'][b]['messages'] for b in ['kafka','rabbit']),timeout=30)
            check('pending-messages-consumed-after-exit',True)
            if self.args.expect_deferred_startup:
                self.wait(lambda:all(set(tokens+[pending]).issubset(set(self.http(url,'/bench/state')['body'][b]['messages'])) for b in ['kafka','rabbit']),timeout=30)
                check('all-cold-start-messages-consumed-after-exit',True)
                restored=self.http(url,'/bench/state')['body']
                check('user-disabled-listeners-still-stopped-after-exit',not restored['kafka']['idleRunning'] and not restored['rabbit']['idleRunning'],restored)
            check('readiness-recovers',self.http(url,'/actuator/health/readiness')['status']==200)
            self.wait(lambda:self.ready_names()=={'sample-a'},timeout=30)
            check('service-routes-after-recovery',self.rpc('request',url=service+'/work',timeout=2)['status']==200)
            self.wait(lambda:diagnostics().get('executed',0)>0,timeout=10)
            check('gated-scheduled-work-resumes',True)
            after=self.get('pod','sample-a')
            check('pod-not-replaced-or-restarted',after['metadata']['uid']==pod_before['metadata']['uid'] and [c['restartCount'] for c in after['status']['containerStatuses']]==[0])
            (self.out/(prefix+'-recovered.json')).write_text(json.dumps({'maintenance':self.http(url,'/actuator/maintenance'),'business':self.http(url,'/bench/state'),'pod':after,'slices':self.get('endpointslices')},indent=2))
            if self.args.expect_deferred_startup:
                entered=self.http(url,'/actuator/maintenance',{'enabled':True,'reason':'second maintenance cycle'})
                paused=self.http(url,'/bench/state')['body']
                check('second-maintenance-uses-kafka-pause-and-rabbit-stop',entered['status']==200 and paused['kafka']['running'] and paused['kafka']['paused'] and not paused['rabbit']['running'],paused)
                marker=prefix+'-second-'+str(uuid.uuid4())
                sent=self.http(url,'/bench/send/'+marker,{})
                time.sleep(.5)
                blocked=self.http(url,'/bench/state')['body']
                check('second-maintenance-blocks-consumption',sent['status']==200 and all(marker not in blocked[b]['messages'] for b in ['kafka','rabbit']),blocked)
                check('second-maintenance-readiness-refuses',self.http(url,'/actuator/health/readiness')['status']==503)
                exited=self.http(url,'/actuator/maintenance',{'enabled':False})
                self.wait(lambda:all(marker in self.http(url,'/bench/state')['body'][b]['messages'] for b in ['kafka','rabbit']),timeout=30)
                final=self.http(url,'/bench/state')['body']
                check('second-exit-resumes-consumers',exited['status']==200 and final['kafka']['running'] and not final['kafka']['paused'] and final['rabbit']['running'],exited)
                check('second-exit-preserves-user-disabled-listeners',not final['kafka']['idleRunning'] and not final['rabbit']['idleRunning'],final)
                check('second-exit-readiness-recovers',self.http(url,'/actuator/health/readiness')['status']==200)
                (self.out/(prefix+'-second-cycle.json')).write_text(json.dumps({'maintenance':exited,'business':final,'pod':self.get('pod','sample-a')},indent=2))
            self.kubectl('logs','sample-a','--tail=250')
            self.kubectl('delete','pod','sample-a','--wait=true','--timeout=90s')

def main():
    p=argparse.ArgumentParser();p.add_argument('--label',required=True);p.add_argument('--versions',nargs='+',default=['3.5.9','4.0.2'])
    p.add_argument('--java-home');p.add_argument('--skip-build-starter',action='store_true');p.add_argument('--expected-starter-sha256',required=True)
    p.add_argument('--starter-version',help='consume an isolated, preinstalled candidate')
    p.add_argument('--expect-deferred-startup',action='store_true',help='assert zero-start semantics and verify a second dynamic maintenance cycle')
    p.add_argument('--cases',nargs='+',choices=['flat','nested','env-flat','env-nested','cold-window','queue-failure','early-exit','property-override'])
    p.add_argument('--start',action='store_true');p.add_argument('--keep',action='store_true')
    args=p.parse_args()
    if not re.fullmatch('[a-z0-9][a-z0-9-]{0,30}',args.label): p.error('invalid label')
    if args.java_home:
        os.environ['JAVA_HOME']=args.java_home;os.environ['PATH']=args.java_home+'/bin:'+os.environ['PATH']
    return StartupRunner(args).run()
if __name__=='__main__': sys.exit(main())
