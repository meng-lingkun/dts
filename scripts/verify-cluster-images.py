"""Validate offline Docker archives against expected tags and rootfs digests."""
import argparse
import gzip
import hashlib
import json
from pathlib import Path
import tarfile

parser=argparse.ArgumentParser()
parser.add_argument('--images',required=True)
parser.add_argument('--list',required=True)
parser.add_argument('--report',required=True)
parser.add_argument('--source-prefix',default='')
args=parser.parse_args()
report=[]
for ref in Path(args.list).read_text().split():
    filename=ref.replace('/','_').replace(':','_')+'.tar'
    path=Path(args.images)/filename
    with tarfile.open(path) as archive:
        manifest=json.load(archive.extractfile('manifest.json'))
        if len(manifest)!=1 or ref not in manifest[0]['RepoTags']: raise RuntimeError('Wrong tag: '+ref)
        item=manifest[0]
        config_bytes=archive.extractfile(item['Config']).read()
        config=json.loads(config_bytes)
        digest=hashlib.sha256(config_bytes).hexdigest()
        if digest not in item['Config']: raise RuntimeError('Config digest mismatch: '+ref)
        if config['architecture']!='amd64' or config['os']!='linux': raise RuntimeError('Wrong platform: '+ref)
        diff_ids=config['rootfs']['diff_ids']
        if len(diff_ids)!=len(item['Layers']): raise RuntimeError('Layer count mismatch: '+ref)
        for layer,expected in zip(item['Layers'],diff_ids):
            stream=archive.extractfile(layer)
            prefix=stream.read(2); stream.seek(0)
            reader=gzip.GzipFile(fileobj=stream) if prefix==b'\x1f\x8b' else stream
            h=hashlib.sha256()
            while chunk:=reader.read(1024*1024): h.update(chunk)
            if 'sha256:'+h.hexdigest()!=expected: raise RuntimeError('Rootfs layer checksum mismatch: '+layer)
    record=dict(image=ref,source=args.source_prefix+ref,archive=filename,config_digest='sha256:'+digest,platform='linux/amd64')
    report.append(record)
    print('Verified '+ref,flush=True)
Path(args.report).write_text(json.dumps(report,indent=2)+'\n')
