#!/usr/bin/env python3
"""Audit the starter's actual transitive consumer graph, excluding optional build dependencies."""
import argparse
import concurrent.futures
import datetime
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import time
import urllib.request
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[2]
NS = 'http://maven.apache.org/POM/4.0.0'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--label', required=True)
    parser.add_argument('--starter-version', required=True)
    parser.add_argument('--boot-version', help='optional consumer BOM; omitted for the library-default graph')
    parser.add_argument('--java-home')
    parser.add_argument('--scan', action='store_true', help='query OSV for each resolved runtime component')
    args = parser.parse_args()
    if not re.fullmatch(r'[a-zA-Z0-9._-]+', args.label) or args.label in ('.', '..'):
        parser.error('invalid label')
    if args.java_home:
        os.environ['JAVA_HOME'] = args.java_home
        os.environ['PATH'] = args.java_home + '/bin:' + os.environ['PATH']
    out = ROOT / 'benchmark/reports' / args.label
    out.mkdir(parents=True, exist_ok=False)
    report = {'timestamp': datetime.datetime.now(datetime.timezone.utc).isoformat(),
              'starter_version': args.starter_version, 'consumer_boot_bom': args.boot_version,
              'scope': 'minimal downstream Maven consumer; no application BOM unless explicitly selected',
              'scan_requested': args.scan, 'completed': False}
    try:
        with tempfile.TemporaryDirectory(prefix='maintenance-consumer-audit-') as project:
            ET.register_namespace('', NS)
            pom = ET.Element('project', xmlns=NS)
            for key, value in [('modelVersion', '4.0.0'), ('groupId', 'benchmark.security'),
                               ('artifactId', 'consumer-audit'), ('version', '1.0-SNAPSHOT')]:
                ET.SubElement(pom, key).text = value
            if args.boot_version:
                managed = ET.SubElement(ET.SubElement(pom, 'dependencyManagement'), 'dependencies')
                dependency = ET.SubElement(managed, 'dependency')
                for key, value in [('groupId', 'org.springframework.boot'), ('artifactId', 'spring-boot-dependencies'),
                                   ('version', args.boot_version), ('type', 'pom'), ('scope', 'import')]:
                    ET.SubElement(dependency, key).text = value
            dep = ET.SubElement(ET.SubElement(pom, 'dependencies'), 'dependency')
            for key, value in [('groupId', 'com.bishokudev'), ('artifactId', 'maintenance-spring-boot-starter'),
                               ('version', args.starter_version)]:
                ET.SubElement(dep, key).text = value
            path = Path(project) / 'pom.xml'
            ET.ElementTree(pom).write(path, encoding='utf-8', xml_declaration=True)
            (out / 'consumer-pom.xml').write_bytes(path.read_bytes())
            with (out / 'maven.log').open('w') as log:
                subprocess.run(['mvn', '-B', '--no-transfer-progress', '-f', str(path),
                                'org.apache.maven.plugins:maven-dependency-plugin:3.7.0:tree',
                                '-Dscope=runtime', '-DoutputType=json', f'-DoutputFile={out / "tree.json"}'],
                               cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, timeout=300, check=True)
        components = {}
        def walk(node):
            key = f'{node["groupId"]}:{node["artifactId"]}'
            if node.get('scope') in ('compile', 'runtime') and key != 'com.bishokudev:maintenance-spring-boot-starter':
                components[key] = node['version']
            for child in node.get('children', []):
                walk(child)
        walk(json.loads((out / 'tree.json').read_text()))
        for required in ('org.springframework.boot:spring-boot-autoconfigure',
                         'org.springframework.boot:spring-boot', 'org.springframework:spring-core'):
            if required not in components:
                raise AssertionError(f'incomplete consumer runtime graph: missing {required}')
        report['components'] = dict(sorted(components.items()))
        report['optional_broker_dependencies_absent'] = all(
            name not in components for name in ['org.springframework.kafka:spring-kafka', 'org.springframework.amqp:spring-rabbit'])
        if not report['optional_broker_dependencies_absent']:
            raise AssertionError('optional broker dependencies leaked into the minimal consumer')
        if args.boot_version:
            assert components['org.springframework.boot:spring-boot-autoconfigure'] == args.boot_version
            assert components['org.springframework.boot:spring-boot'] == args.boot_version
            report['consumer_boot_bom_respected'] = True
        if args.scan:
            def query(item):
                name, version = item
                request = urllib.request.Request('https://api.osv.dev/v1/query',
                    data=json.dumps({'package': {'ecosystem': 'Maven', 'name': name}, 'version': version}).encode(),
                    headers={'Content-Type': 'application/json'}, method='POST')
                for attempt in range(3):
                    try:
                        with urllib.request.urlopen(request, timeout=30) as response:
                            result = json.loads(response.read())
                        return {'component': name, 'version': version,
                                'vulnerabilities': [v for v in result.get('vulns', []) if not v.get('withdrawn')]}
                    except Exception:
                        if attempt == 2:
                            raise
                        time.sleep(1 + attempt)
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
                report['osv_results'] = list(pool.map(query, sorted(components.items())))
            report['advisory_ids'] = sorted({v['id'] for r in report['osv_results'] for v in r['vulnerabilities']})
            report['components_with_findings'] = sum(bool(r['vulnerabilities']) for r in report['osv_results'])
        report['completed'] = True
    except Exception as error:
        report['error'] = str(error)
        raise
    finally:
        (out / 'results.json').write_text(json.dumps(report, indent=2) + '\n')
        print(json.dumps({k: v for k, v in report.items() if k not in ('osv_results',)}, indent=2), flush=True)
    return 1 if report.get('advisory_ids') else 0


if __name__ == '__main__':
    raise SystemExit(main())
