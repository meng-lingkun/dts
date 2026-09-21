"""Fetch pinned upstream node tools and Kylin 2403 RPM dependency closure.

Run on the build machine; target installation performs no network downloads.
RPM dependency selection is validated by yum/dnf on the target before runtime setup.
"""
import argparse
import concurrent.futures
import gzip
import hashlib
import json
import os
from pathlib import Path
import subprocess
import xml.etree.ElementTree as ET

def fetch(url, target, sha=None):
    target = Path(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists() and sha and hashlib.sha256(target.read_bytes()).hexdigest() == sha:
        return target
    temporary = target.with_name(target.name + '.download')
    command = ['curl', '-fsSL', '--retry', '4', '--max-time', '600']
    if os.name == 'nt':
        command = ['curl.exe', '--ssl-revoke-best-effort', *command[1:]]
    subprocess.run([*command, '-o', str(temporary), url], check=True)
    if sha and hashlib.sha256(temporary.read_bytes()).hexdigest() != sha:
        raise RuntimeError('SHA256 mismatch: ' + url)
    temporary.replace(target)
    return target

def tools(root, github_prefix=''):
    assets = [
        ('containerd/containerd', 'v2.3.5', 'containerd-static-2.3.5-linux-amd64.tar.gz', 'containerd-static-2.3.5-linux-amd64.tar.gz.sha256sum'),
        ('opencontainers/runc', 'v1.5.1', 'runc.amd64', 'runc.sha256sum'),
        ('containernetworking/plugins', 'v1.9.1', 'cni-plugins-linux-amd64-v1.9.1.tgz', 'cni-plugins-linux-amd64-v1.9.1.tgz.sha256'),
        ('kubernetes-sigs/cri-tools', 'v1.32.0', 'crictl-v1.32.0-linux-amd64.tar.gz', 'crictl-v1.32.0-linux-amd64.tar.gz.sha256'),
    ]
    records = []
    for repo, version, filename, checksum in assets:
        base = f'https://github.com/{repo}/releases/download/{version}/'
        checks = fetch(base+checksum, root/'upstream-checksums'/checksum).read_text()
        lines = [line for line in checks.splitlines() if len(line.split()) > 1 and line.split()[-1].lstrip('*') == filename]
        if not lines and len(checks.split()) == 1: lines = [checks]
        if len(lines) != 1: raise RuntimeError('Ambiguous checksum: '+filename)
        sha = lines[0].split()[0]
        download_url=github_prefix+base+filename
        fetch(download_url, root/'runtime'/filename, sha)
        records.append(dict(url=base+filename,download_url=download_url, sha256=sha, file='runtime/'+filename))
    manifest_url = 'https://raw.githubusercontent.com/flannel-io/flannel/v0.28.9/Documentation/kube-flannel.yml'
    manifest = fetch(manifest_url, root/'network/kube-flannel.yaml')
    records.append(dict(url=manifest_url,sha256=hashlib.sha256(manifest.read_bytes()).hexdigest(),file='network/kube-flannel.yaml'))
    (root/'upstream-tools.json').write_text(json.dumps(records, indent=2)+'\n')
    print('Upstream node tools verified', flush=True)

def rpms(root):
    base='https://update.cs2c.com.cn/NS/V10/V10SP3-2403/os/adv/lic/base/x86_64/'
    repomd=fetch(base+'repodata/repomd.xml',root/'rpm-metadata/repomd.xml')
    ns={'r':'http://linux.duke.edu/metadata/repo','c':'http://linux.duke.edu/metadata/common','rpm':'http://linux.duke.edu/metadata/rpm'}
    primary=ET.parse(repomd).find("r:data[@type='primary']",ns)
    location=primary.find('r:location',ns).attrib['href']
    checksum=primary.find('r:checksum',ns)
    if checksum.attrib['type'] != 'sha256': raise RuntimeError('Metadata requires SHA256')
    compressed=fetch(base+location,root/'rpm-metadata/primary.xml.gz',checksum.text)
    packages=[]
    for element in ET.fromstring(gzip.decompress(compressed.read_bytes())).findall('c:package',ns):
        arch=element.findtext('c:arch',namespaces=ns)
        if arch not in ('x86_64','noarch'): continue
        name=element.findtext('c:name',namespaces=ns)
        fmt=element.find('c:format',ns)
        provides={name}|{x.attrib['name'] for x in fmt.findall('rpm:provides/rpm:entry',ns)}|{x.text for x in fmt.findall('c:file',ns)}
        requires={x.attrib['name'] for x in fmt.findall('rpm:requires/rpm:entry',ns)}
        ch=element.find('c:checksum',ns)
        if ch.attrib['type']!='sha256': raise RuntimeError('RPM requires SHA256')
        packages.append(dict(name=name,arch=arch,provides=provides,requires=requires,sha256=ch.text,location=element.find('c:location',ns).attrib['href'],element=element))
    providers={}
    for p in packages:
        for capability in p['provides']: providers.setdefault(capability,[]).append(p)
    seeds=['conntrack-tools','iptables','iproute','socat','ethtool','ebtables','util-linux','kmod','glibc','libseccomp','systemd','nfs-utils','openssl','curl','ansible','rsync','openssh-clients','grubby']
    pending=list(seeds); selected={}; missing=set()
    while pending:
        capability=pending.pop()
        if capability.startswith('rpmlib('): continue
        candidates=providers.get(capability,[])
        if not candidates:
            missing.add(capability); continue
        candidates.sort(key=lambda p:(p['name']!=capability,'-devel' in p['name'],len(p['name']),p['name']))
        p=candidates[0]
        if p['location'] in selected: continue
        selected[p['location']]=p
        pending.extend(p['requires'])
    if missing: raise RuntimeError('Unresolved RPM capabilities: '+repr(sorted(missing)))
    print(f'Downloading {len(selected)} Kylin RPMs from official 2403 base repository',flush=True)
    def download(p):
        fetch(base+p['location'],root/'rpms'/Path(p['location']).name,p['sha256'])
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool: list(pool.map(download,selected.values()))
    records=[dict(name=p['name'],file='rpms/'+Path(p['location']).name,sha256=p['sha256'],url=base+p['location']) for p in selected.values()]
    (root/'kylin-rpms.json').write_text(json.dumps(dict(repository=base,seeds=seeds,packages=records),indent=2)+'\n')
    # Build a file:// repository restricted to the downloaded dependency set.
    # yum/dnf performs the actual RPM version/conflict resolution on the target.
    # libdnf/librepo on Kylin V10 rejects repository roots serialized as
    # ``ns0:metadata``/``ns0:repomd`` even though they are namespace-equivalent
    # XML.  Register the canonical default namespace and the conventional RPM
    # prefix before serializing each metadata document.
    ET.register_namespace('', ns['c'])
    ET.register_namespace('rpm', ns['rpm'])
    metadata=ET.Element('{'+ns['c']+'}metadata',packages=str(len(selected)))
    for p in selected.values():
        element=p['element']
        element.find('c:location',ns).set('href',Path(p['location']).name)
        metadata.append(element)
    raw=ET.tostring(metadata,encoding='utf-8',xml_declaration=True)
    packed=gzip.compress(raw,mtime=0)
    repodata=root/'rpms/repodata'; repodata.mkdir(exist_ok=True)
    (repodata/'primary.xml.gz').write_bytes(packed)
    ET.register_namespace('', ns['r'])
    repomd=ET.Element('{'+ns['r']+'}repomd')
    data=ET.SubElement(repomd,'{'+ns['r']+'}data',type='primary')
    ET.SubElement(data,'{'+ns['r']+'}checksum',type='sha256').text=hashlib.sha256(packed).hexdigest()
    ET.SubElement(data,'{'+ns['r']+'}open-checksum',type='sha256').text=hashlib.sha256(raw).hexdigest()
    ET.SubElement(data,'{'+ns['r']+'}location',href='repodata/primary.xml.gz')
    ET.SubElement(data,'{'+ns['r']+'}timestamp').text='1715148809'
    ET.SubElement(data,'{'+ns['r']+'}size').text=str(len(packed))
    ET.SubElement(data,'{'+ns['r']+'}open-size').text=str(len(raw))
    repomd_path=repodata/'repomd.xml'
    ET.ElementTree(repomd).write(repomd_path,encoding='utf-8',xml_declaration=True)
    repomd_bytes=repomd_path.read_bytes()
    primary_bytes=gzip.decompress((repodata/'primary.xml.gz').read_bytes())
    expected_repomd=b'<repomd xmlns="http://linux.duke.edu/metadata/repo"'
    expected_primary=b'<metadata xmlns="http://linux.duke.edu/metadata/common"'
    if expected_repomd not in repomd_bytes or b'<ns0:repomd' in repomd_bytes:
        raise RuntimeError('repomd.xml must use the canonical default namespace')
    if expected_primary not in primary_bytes or b'<ns0:metadata' in primary_bytes:
        raise RuntimeError('primary.xml must use the canonical default namespace')
    parsed=ET.parse(repomd_path).getroot()
    if parsed.tag != '{'+ns['r']+'}repomd':
        raise RuntimeError('invalid repomd.xml root element')
    print('RPM downloads and SHA256 verification complete',flush=True)

if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--output',required=True)
    parser.add_argument('--part',choices=['tools','rpms','all'],default='all')
    parser.add_argument('--github-prefix',default='',help='optional binary download proxy; official checksums are always verified')
    args=parser.parse_args(); root=Path(args.output); root.mkdir(parents=True,exist_ok=True)
    if args.part in ('tools','all'): tools(root,args.github_prefix)
    if args.part in ('rpms','all'): rpms(root)
