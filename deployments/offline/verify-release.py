"""Verify an exported release and its Docker images offline, without a daemon."""
import argparse
import gzip
import hashlib
import json
from pathlib import Path, PurePosixPath
import shutil
import tarfile
import tempfile


def sha(reader):
    return hashlib.file_digest(reader, "sha256").hexdigest()


def check_images(compressed, version, cluster_refs, binaries=None, web=None):
    wanted = {"dts/server:" + version, "dts/web:" + version, "dts/postgres:17"} | cluster_refs
    with tempfile.TemporaryFile() as scratch:
        with gzip.GzipFile(fileobj=compressed) as gz:
            shutil.copyfileobj(gz, scratch)
        scratch.seek(0)
        with tarfile.open(fileobj=scratch) as image_tar:
            manifest = json.load(image_tar.extractfile("manifest.json"))
            tags = [tag for item in manifest for tag in item["RepoTags"]]
            if len(tags) != len(set(tags)) or set(tags) != wanted:
                raise ValueError("unexpected image set: " + str(tags))
            seen = {}
            ids = {}
            for item in manifest:
                config_bytes = image_tar.extractfile(item["Config"]).read()
                if item["Config"] != "sha256:" + hashlib.sha256(config_bytes).hexdigest():
                    raise ValueError("config digest mismatch")
                config = json.loads(config_bytes)
                if (config["architecture"], config["os"]) != ("amd64", "linux"):
                    raise ValueError("unexpected image platform")
                for layer, diff in zip(item["Layers"], config["rootfs"]["diff_ids"], strict=True):
                    if layer not in seen:
                        body = image_tar.extractfile(layer)
                        if body.read(2) == b"\x1f\x8b":
                            body.seek(0)
                            reader = gzip.GzipFile(fileobj=body)
                        else:
                            body.seek(0)
                            reader = body
                        seen[layer] = "sha256:" + sha(reader)
                    if seen[layer] != diff:
                        raise ValueError("layer content mismatch: " + layer)
                for tag in item["RepoTags"]:
                    ids[tag] = item["Config"]
                    assets, prefix = None, None
                    if binaries and tag == "dts/server:" + version:
                        assets, prefix = binaries, "usr/local/bin/"
                    if web and tag == "dts/web:" + version:
                        assets, prefix = web, "usr/share/nginx/html/"
                    if assets:
                        expected = {path.relative_to(assets).as_posix(): path for path in assets.rglob("*") if path.is_file()}
                        compared = set()
                        with tarfile.open(fileobj=image_tar.extractfile(item["Layers"][-1]), mode="r|*") as app_layer:
                            for member in app_layer:
                                if not member.isfile() or not member.name.startswith(prefix):
                                    continue
                                name = member.name[len(prefix):]
                                if name not in expected:
                                    raise ValueError("unexpected application file: " + name)
                                with expected[name].open("rb") as original:
                                    h = hashlib.sha256()
                                    data = app_layer.extractfile(member)
                                    while chunk := data.read(1 << 20):
                                        h.update(chunk)
                                    if h.hexdigest() != sha(original):
                                        raise ValueError("application file differs from current build: " + name)
                                compared.add(name)
                        if compared != expected.keys():
                            raise ValueError("missing application files from " + str(assets))
            return ids


def check_release(path, binaries=None, web=None):
    with path.open("rb") as stream:
        expected_sha, filename = Path(str(path) + ".sha256").read_text().strip().split("  ", 1)
        if filename != path.name or sha(stream) != expected_sha:
            raise ValueError("outer archive checksum mismatch")
    with tempfile.TemporaryFile() as images:
        special = {}
        expected, actual = {}, {}
        root = path.name.removesuffix(".tar.gz")
        with tarfile.open(path, mode="r|gz") as release:
            for member in release:
                member_path = PurePosixPath(member.name)
                if not member_path.parts or member_path.parts[0] != root or ".." in member_path.parts:
                    raise ValueError("invalid archive member: " + str(member_path))
                if member.isdir():
                    continue
                name = member_path.relative_to(root).as_posix()
                if not member.isfile() or name in actual:
                    raise ValueError("unexpected member type or duplicate: " + name)
                data = release.extractfile(member)
                if name == "SHA256SUMS":
                    for line in data.read().decode("ascii").splitlines():
                        checksum, rel = line.split("  ", 1)
                        expected[rel.removeprefix("./")] = checksum
                elif name.startswith("images/"):
                    if images.tell():
                        raise ValueError("expected one merged image archive")
                    shutil.copyfileobj(data, images)
                    images.seek(0)
                    actual[name] = sha(images)
                elif name in ("manifest.json", "VERSION", "cluster/cluster-images.txt"):
                    special[name] = data.read()
                    actual[name] = hashlib.sha256(special[name]).hexdigest()
                else:
                    # Streaming tarfile readers do not always implement file_digest's readinto.
                    h = hashlib.sha256()
                    while chunk := data.read(1 << 20):
                        h.update(chunk)
                    actual[name] = h.hexdigest()
        if actual != expected:
            raise ValueError("per-file inventory differs")
        version = special["VERSION"].decode().strip()
        recovery_tool = f"tools/dts-s3-recovery-{version}-linux-amd64.tar.gz"
        for required in ("scripts/migrate-metadata.sh", "scripts/recovery-set.sh", "migrations/schema.sql", "bin/dtsctl",
                         "migrations/083_v015_rc50_atomic_control.sql", "migrations/084_v015_rc50_user_sessions.sql",
                         "migrations/085_v015_rc50_personal_tokens.sql", "docs/openapi.json", "docs/SUPPORT_MATRIX_V0.15_RC50.md",
                         recovery_tool, recovery_tool + ".sha256"):
            if required not in actual:
                raise ValueError("missing operational file: " + required)
        manifest = json.loads(special["manifest.json"])
        if manifest["version"] != version:
            raise ValueError("version mismatch")
        cluster = set(special.get("cluster/cluster-images.txt", b"").decode().split())
        if manifest["deployment"] == "compose":
            for required in ("compose.yaml", "nginx.conf", "runtime/docker-compose-linux-x86_64",
                             "runtime/docker-" + manifest["docker_engine"] + ".tgz"):
                if required not in actual:
                    raise ValueError("missing Compose deployment file: " + required)
        if manifest["deployment"] == "kubernetes" and not cluster:
            raise ValueError("missing cluster payload")
        images.seek(0)
        ids = check_images(images, version, cluster, binaries, web)
        if manifest["deployment"] == "compose" and manifest["images"] != ids:
            raise ValueError("Compose manifest image ID mismatch")
        return {"archive": path.name, "sha256": expected_sha, "files": len(actual), "images": ids}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", nargs="+", type=Path)
    parser.add_argument("--binaries", type=Path, help="compare packaged executable bytes with local Linux build")
    parser.add_argument("--web", type=Path, help="compare packaged web assets with local frontend build")
    args = parser.parse_args()
    print(json.dumps([check_release(path, args.binaries, args.web) for path in args.archive], indent=2))
