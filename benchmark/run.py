#!/usr/bin/env python3
"""Real-process, real-broker contract tests. Python standard library only."""
import xml.etree.ElementTree as ET
import argparse, concurrent.futures, datetime, hashlib, json, os, pathlib, re, subprocess, time, urllib.request, urllib.error, uuid, socket, zipfile
ROOT = pathlib.Path(__file__).resolve().parents[1]
STARTER_VERSION=ET.parse(ROOT/'pom.xml').getroot().find('{http://maven.apache.org/POM/4.0.0}version').text
BASE = 'http://127.0.0.1:18080'

def http(path, data=None):
    request = urllib.request.Request(BASE + path, data=None if data is None else json.dumps(data).encode(),
                                     headers={'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            body = response.read()
            return response.status, json.loads(body) if body else {}
    except urllib.error.HTTPError as error:
        body = error.read()
        try: body = json.loads(body)
        except ValueError: body = body.decode()
        return error.code, body

def until(predicate, timeout=25):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        try:
            if predicate(): return
        except (OSError, ValueError): pass
        time.sleep(.1)
    raise AssertionError('condition did not become true within %ss' % timeout)

def state(): return http('/bench/state')[1]
def mode(value): http('/bench/mode/' + value, {})
def transition(enabled, reason=True):
    body = {'enabled': enabled}
    if reason: body['reason'] = 'benchmark'
    return http('/actuator/maintenance', body)
def ready(): return http('/actuator/health/readiness')[0]
def send():
    token = str(uuid.uuid4()); assert http('/bench/send/' + token, {})[0] == 200; return token

def lifecycle(check):
    transition(True)
    s = state()
    check('kafka-paused-without-stop', s['kafka']['running'] and s['kafka']['paused'], s)
    check('rabbit-stopped', not s['rabbit']['running'], s)
    check('readiness-refuses-liveness-alive', ready() == 503 and http('/actuator/health/liveness')[0] == 200)
    token = send(); time.sleep(.5)
    check('no-consumption-during-maintenance', all(token not in state()[b]['messages'] for b in ['kafka','rabbit']))
    check('direct-http-unfiltered-during-maintenance', http('/work')[0] == 200)
    transition(False)
    until(lambda: all(token in state()[b]['messages'] for b in ['kafka','rabbit']))
    check('queued-messages-consumed-after-exit', True)
    check('idle-consumers-preserved', not state()['kafka']['idleRunning'] and not state()['rabbit']['idleRunning'], state())
    check('exit-hooks-before-resume', not state()['exitSawRunning'])

def optional_reason(check):
    code, body = transition(True, False)
    check('optional-reason', code == 200 and body.get('active') is True, {'status':code,'body':body})

def entry_failure(check):
    mode('enter-fail'); transition(True)
    s = http('/actuator/maintenance')[1]
    check('failed-entry-is-not-active', s.get('active') is False and s.get('details',{}).get('phase') == 'FAILED', s)
    check('failed-entry-retains-readiness-refusal', ready() == 503 and http('/work')[0] == 200)
    mode('ok'); transition(True)
    s = http('/actuator/maintenance')[1]
    check('failed-entry-retry', s.get('active') is True and not s.get('details',{}).get('partialFailure',False)
          and s.get('details',{}).get('hooks',{}).get('failed',0) == 0, s)

def exit_failure(check):
    transition(True); mode('exit-fail'); transition(False)
    check('failed-exit-retains-readiness-refusal', ready() == 503 and http('/work')[0] == 200, http('/actuator/maintenance')[1])
    check('failed-exit-consumers-remain-paused', state()['kafka']['paused'] and not state()['rabbit']['running'], state())
    mode('ok'); transition(False)
    check('failed-exit-retry', ready() == 200 and http('/work')[0] == 200)

def timeout(check):
    mode('slow'); transition(True); time.sleep(1.7)
    check('timed-out-hook-cancelled', state()['lateHooks'] == 0 and state()['interruptions'] == 1, state())
    check('timeout-is-failed-transition', http('/actuator/maintenance')[1].get('details',{}).get('phase') == 'FAILED')

def queue_failure(check):
    mode('queue-fail'); transition(True)
    s = http('/actuator/maintenance')[1]
    metric = http('/actuator/metrics/maintenance.transition.duration?tag=status:partial_failure')
    check('broker-failure-metric', metric[0] == 200 and any(m['statistic'] == 'COUNT' and m['value'] >= 1 for m in metric[1].get('measurements',[])), metric)
    check('broker-failure-propagated', s.get('details',{}).get('partialFailure') is True and not s.get('active'), s)

def propagation_grace(check):
    with concurrent.futures.ThreadPoolExecutor() as pool:
        work = pool.submit(http, '/work?ms=2500')
        until(lambda: state()['work'] == 1)
        _, result = transition(True)
        check('configured-propagation-grace', result.get('details',{}).get('drainDelayMs') == 300)
        check('no-library-http-drain-barrier', not work.done() and state()['work'] == 1)
        check('inflight-request-not-aborted', work.result()[0] == 200)

def concurrency(check):
    with concurrent.futures.ThreadPoolExecutor() as pool:
        results = list(pool.map(lambda _: transition(True)[0], range(8)))
    check('concurrent-entry-idempotent', all(x == 200 for x in results) and http('/actuator/maintenance')[1]['active'], results)

def readonly(check):
    check('read-only-get', http('/actuator/maintenance')[0] == 200)
    check('read-only-post-rejected', transition(True)[0] in (404,405))
    check('read-only-no-state-change', ready() == 200)

def disabled(check):
    check('disabled-endpoint-hidden', http('/actuator/maintenance')[0] == 404 and transition(True)[0] == 404)

def readiness_disabled(check):
    _, result = transition(True)
    check('readiness-disabled-keeps-probe-up', ready() == 200)
    check('readiness-disabled-skips-grace', result.get('details',{}).get('readiness') == 'DISABLED'
          and 'drainDelayMs' not in result.get('details',{}))

def broker_inflight(check):
    mode('broker-slow'); token = send()
    until(lambda: all(state()[b]['inFlight'] == 1 for b in ['kafka','rabbit']))
    transition(True)
    check('inflight-broker-handlers-finish', all(state()[b]['inFlight'] == 0 and token in state()[b]['messages'] for b in ['kafka','rabbit']), state())
    check('broker-drain-completed', http('/actuator/maintenance')[1].get('active') is True)

SCENARIOS = [lifecycle, optional_reason, entry_failure, exit_failure, timeout, queue_failure, propagation_grace, concurrency, readonly, disabled, readiness_disabled, broker_inflight]

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--label', required=True)
    parser.add_argument('--versions', nargs='+', default=['3.5.9','4.0.2'])
    parser.add_argument('--skip-build-starter', action='store_true')
    parser.add_argument('--expected-starter-sha256', help='require this exact nested starter binary')
    parser.add_argument('--java-home', help='JDK used to build and run the sample')
    parser.add_argument('--scenarios', nargs='+', choices=[s.__name__ for s in SCENARIOS])
    args=parser.parse_args()
    if args.java_home:
        os.environ['JAVA_HOME']=args.java_home
        os.environ['PATH']=args.java_home+'/bin:'+os.environ['PATH']
    if args.expected_starter_sha256 and not re.fullmatch('[0-9a-f]{64}', args.expected_starter_sha256):
        parser.error('expected-starter-sha256 must contain 64 lowercase hexadecimal characters')
    if any(not re.fullmatch(r'[0-9]+\.[0-9]+\.[0-9]+(?:[-.][A-Za-z0-9]+)*', v) for v in args.versions):
        parser.error('invalid Spring Boot version')
    if args.scenarios and 'readonly' in args.scenarios and any(v.startswith('2.') for v in args.versions):
        parser.error('readonly is outside the Boot 2.x compatibility contract')
    if not all(c.isalnum() or c in '-_.' for c in args.label): parser.error('label must be a simple directory name')
    out=ROOT/'benchmark'/'reports'/args.label
    out.mkdir(parents=True, exist_ok=False)
    with socket.socket() as probe:
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        probe.bind(('127.0.0.1', 18080))
    report={'contract':'kubernetes-readiness-v2', 'timestamp':datetime.datetime.now(datetime.timezone.utc).isoformat(),
            'commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
            'dirty':bool(subprocess.check_output(['git','status','--porcelain'],cwd=ROOT,text=True)),
            'java':subprocess.run(['java','-version'],capture_output=True,text=True).stderr.strip(),
            'starter_version':STARTER_VERSION,'expected_starter_sha256':args.expected_starter_sha256,
            'skipped_scenarios':[],'results':[]}
    def record(version,name,ok,detail=None):
        report['results'].append({'version':version,'test':name,'passed':bool(ok),'detail':detail})
        print(f"{version} {name}: {'PASS' if ok else 'FAIL'}", flush=True)
        (out/'results.json').write_text(json.dumps(report,indent=2))
    def command(cmd,log):
        with (out/log).open('w') as output:
            return subprocess.run(cmd,cwd=ROOT,stdout=output,stderr=subprocess.STDOUT,timeout=1200).returncode==0
    try:
        if not args.skip_build_starter:
            ok=command(['mvn','-B','-Dspring-boot.version=3.5.9','-Dmaven.test.skip=true','-Dmaven.javadoc.skip=true','clean','install'],'starter-build.log')
            record('3.5.9','starter-build',ok)
            if not ok: return 1
        jar=ROOT/'target'/f'maintenance-spring-boot-starter-{STARTER_VERSION}.jar'
        if not args.skip_build_starter and jar.exists(): report['starter_sha256']=hashlib.sha256(jar.read_bytes()).hexdigest()
        for version in args.versions:
            cmd=['mvn','-B','-f','benchmark/sample/pom.xml',f'-Dmaintenance.version={STARTER_VERSION}',f'-Dspring-boot.version={version}','clean','package']
            if version.startswith('4.'): cmd.append('-Dboot4')
            ok=command(cmd,version+'-sample-build.log'); record(version,'sample-build',ok)
            if not ok: continue
            with zipfile.ZipFile(ROOT/'benchmark/sample/target/benchmark.jar') as archive:
                library = next(n for n in archive.namelist() if n.startswith('BOOT-INF/lib/maintenance-spring-boot-starter-'))
                digest = hashlib.sha256(archive.read(library)).hexdigest()
                report.setdefault('consumed_starter_sha256',{})[version] = digest
                report.setdefault('runtime_libraries',{})[version]=sorted(n.removeprefix('BOOT-INF/lib/') for n in archive.namelist() if n.startswith('BOOT-INF/lib/') and n.endswith('.jar'))
            identical=(not args.expected_starter_sha256 or digest==args.expected_starter_sha256) and len(set(report['consumed_starter_sha256'].values()))==1
            record(version,'starter-binary-identity',identical,digest)
            if not identical: continue
            for scenario in SCENARIOS:
                if args.scenarios and scenario.__name__ not in args.scenarios: continue
                if version.startswith('2.') and scenario.__name__=='readonly':
                    report['skipped_scenarios'].append({'version':version,'scenario':'readonly','reason':'Boot 2.x has no Actuator access mode; excluded by compatibility contract'})
                    continue
                with (out/(version+'-'+scenario.__name__+'.log')).open('w') as log:
                    group=f'maintenance-benchmark-{uuid.uuid4()}'
                    report.setdefault('consumer_groups',{}).setdefault(version,{})[scenario.__name__]=group
                    launch=['java','-jar','benchmark/sample/target/benchmark.jar',f'--benchmark.kafka.group={group}']
                    if version.startswith('2.'): launch += ['--spring.profiles.active=boot2']
                    if scenario.__name__ == 'propagation_grace': launch += ['--maintenance.queues.enabled=false','--maintenance.hooks.enabled=false','--maintenance.drain-delay=300ms']
                    if scenario.__name__ == 'readiness_disabled': launch += ['--maintenance.readiness.enabled=false','--maintenance.drain-delay=2s']
                    if scenario.__name__ == 'readonly': launch += ['--management.endpoint.maintenance.access=read-only']
                    if scenario.__name__ == 'disabled': launch += ['--management.endpoint.maintenance.enabled=false' if version.startswith('2.') else '--management.endpoint.maintenance.access=none']
                    process=subprocess.Popen(launch,cwd=ROOT,stdout=log,stderr=subprocess.STDOUT)
                    try:
                        def started():
                            if process.poll() is not None: raise RuntimeError('sample exited; inspect application log')
                            return http('/bench/state')[0] == 200
                        until(started,60)
                        token=send(); until(lambda: all(token in state()[b]['messages'] for b in ['kafka','rabbit']))
                        scenario(lambda name,ok,detail=None:record(version,name,ok,detail))
                    except Exception as error: record(version,scenario.__name__+'-execution',False,str(error))
                    finally:
                        process.terminate()
                        try: process.wait(timeout=12)
                        except subprocess.TimeoutExpired: process.kill(); process.wait()
    finally:
        (out/'results.json').write_text(json.dumps(report,indent=2))
        lines=['# '+args.label,'',report['timestamp'],'','| Version | Contract | Result |','|---|---|---|']
        lines += [f"| {r['version']} | {r['test']} | {'PASS' if r['passed'] else 'FAIL'} |" for r in report['results']]
        if report['skipped_scenarios']:
            lines += ['', 'Excluded scenarios:']+[f"- {s['version']} / {s['scenario']}: {s['reason']}" for s in report['skipped_scenarios']]
        (out/'summary.md').write_text('\n'.join(lines)+'\n')
    return 0 if all(r['passed'] for r in report['results']) else 1
if __name__=='__main__': raise SystemExit(main())
