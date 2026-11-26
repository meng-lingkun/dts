"""Build a single-node Compose release from the matching DTS Kubernetes release."""
import argparse
import gzip
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import shutil
import tarfile
import tempfile
import urllib.request


HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
ENGINE = "docker-29.7.2.tgz"
PLUGIN = "docker-compose-linux-x86_64"
RUNTIME = {
    ENGINE: ("https://download.docker.com/linux/static/stable/x86_64/" + ENGINE,
             "803d433f226db4776e1768fd319fc6c6e4935a456acf84fcc0080818b854bc8f"),
    PLUGIN: ("https://github.com/docker/compose/releases/download/v2.40.2/" + PLUGIN,
             "6c964d9655cd629ef43c5dc75d9612c2da319237debee54a7aef217e9f362b88"),
}


def sha(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def select_images(source, destination, wanted):
    with tempfile.TemporaryFile() as expanded:
        with gzip.open(source, "rb") as compressed:
            shutil.copyfileobj(compressed, expanded)
        expanded.seek(0)
        with tarfile.open(fileobj=expanded) as base, gzip.open(destination, "wb", compresslevel=1) as packed:
            selected = [m for m in json.load(base.extractfile("manifest.json")) if set(m["RepoTags"]) & wanted]
            if {t for m in selected for t in m["RepoTags"]} != wanted:
                raise ValueError("application tag set differs from release")
            copied = set()
            with tarfile.open(fileobj=packed, mode="w|") as out:
                for item in selected:
                    for path in [item["Config"], *item["Layers"]]:
                        if path not in copied:
                            out.addfile(base.getmember(path), base.extractfile(path))
                            copied.add(path)
                data = json.dumps(selected).encode()
                info = tarfile.TarInfo("manifest.json")
                info.size = len(data)
                out.addfile(info, io.BytesIO(data))
    return selected


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package", type=Path, required=True, help="extracted full Kubernetes release")
    parser.add_argument("--runtime-cache", type=Path, help="directory containing pinned Engine and Compose")
    parser.add_argument("--out", type=Path, default=ROOT / "dist")
    args = parser.parse_args()
    version = (ROOT / "VERSION").read_text().strip()
    if (args.package / "VERSION").read_text().strip() != version:
        raise ValueError("Kubernetes release version mismatch")
    inventory = dict((name.removeprefix("./"), checksum) for checksum, name in
                     (line.split("  ", 1) for line in (args.package / "SHA256SUMS").read_text().splitlines()))
    def checked(path):
        rel = PurePosixPath(path)
        if rel.is_absolute() or ".." in rel.parts or "\\" in path or ":" in path:
            raise ValueError("invalid package path")
        candidate = (args.package / path).resolve()
        if not candidate.is_relative_to(args.package.resolve()):
            raise ValueError("file outside package")
        if sha(candidate) != inventory[path]:
            raise ValueError("input package checksum mismatch: " + path)
        return candidate

    name = f"dts-compose-offline-{version}-linux-amd64"
    args.out.mkdir(parents=True, exist_ok=True)
    archive = args.out / (name + ".tar.gz")
    checksum = args.out / (archive.name + ".sha256")
    if archive.exists() or checksum.exists():
        raise FileExistsError(f"{archive}: use a fresh output directory")
    with tempfile.TemporaryDirectory(dir=args.out) as tmp:
        work = Path(tmp)
        stage = work / name
        for directory in ("runtime", "images", "migrations", "scripts", "bin", "docs", "tools/release-security"):
            (stage / directory).mkdir(parents=True)
        for filename in ("README.md", "compose.yaml", "nginx.conf", "install.sh", "install-runtime.sh", "install-kylin-deps.sh", "uninstall.sh", "verify.sh"):
            (stage / filename).write_bytes((HERE / filename).read_bytes().replace(b"\r\n", b"\n"))
        (stage / "runtime/docker.service").write_bytes((HERE / "docker.service").read_bytes().replace(b"\r\n", b"\n"))
        for path in ("VERSION", "bin/dtsctl", "migrations/schema.sql", "scripts/migrate-metadata.sh", "scripts/recovery-set.sh",
                     "migrations/083_v015_rc50_atomic_control.sql", "migrations/084_v015_rc50_user_sessions.sql",
                     "migrations/085_v015_rc50_personal_tokens.sql", "docs/SUPPORT_MATRIX_V0.15_RC50.md",
                     f"tools/dts-s3-recovery-{version}-linux-amd64.tar.gz",
                     f"tools/dts-s3-recovery-{version}-linux-amd64.tar.gz.sha256",
                     "docs/openapi.json", "docs/ENTERPRISE_HARDENING_RC50.md", "docs/OFFLINE_RELEASE_SECURITY.md", "docs/DTS_RENAME_UPGRADE.md", "tools/release-security/release.py"):
            shutil.copyfile(checked(path), stage / path)
        for path in sorted(inventory):
            if path.startswith("cluster/rpms/") or path == "cluster/kylin-rpms.json":
                destination = stage / "kylin" / path.removeprefix("cluster/")
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(checked(path), destination)
        if not (stage / "kylin/rpms/repodata/repomd.xml").is_file():
            raise ValueError("missing offline Kylin RPM repository")
        images = checked("images/all-images.tar.gz")
        selected = select_images(images, stage / "images/application.tar.gz",
                                 {"dts/server:" + version, "dts/web:" + version, "dts/postgres:17"})
        for filename, (url, expected) in RUNTIME.items():
            dest = stage / "runtime" / filename
            if args.runtime_cache:
                shutil.copyfile(args.runtime_cache / filename, dest)
            else:
                with urllib.request.urlopen(url, timeout=180) as src, dest.open("wb") as output:
                    shutil.copyfileobj(src, output)
            if sha(dest) != expected:
                raise ValueError("runtime checksum mismatch: " + filename)
        manifest = {"version": version, "deployment": "compose", "platform": "linux/amd64",
                    "docker_engine": "29.7.2", "docker_compose": "2.40.2",
                    "images": {tag: item["Config"] for item in selected for tag in item["RepoTags"]}}
        (stage / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", newline="\n")
        checks = [f"{sha(p)}  {p.relative_to(stage).as_posix()}\n" for p in sorted(stage.rglob("*")) if p.is_file()]
        (stage / "SHA256SUMS").write_text("".join(checks), encoding="ascii", newline="\n")
        def linux_mode(info):
            info.mode = 0o755 if info.isdir() or info.name.endswith((".sh", "/dtsctl", "/" + PLUGIN)) else 0o644
            return info
        built = work / archive.name
        with tarfile.open(built, "w:gz", compresslevel=1) as tar:
            tar.add(stage, arcname=name, filter=linux_mode)
        value = sha(built)
        built.rename(archive)
        checksum.write_text(f"{value}  {archive.name}\n", encoding="ascii", newline="\n")
        print(f"{archive}: {value}")


if __name__ == "__main__":
    main()
