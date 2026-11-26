"""Reuse an integrity-checked DTS Kubernetes runtime on an offline build machine.

Application executables, web assets and nginx configuration always come from the
current build. Only supports the layer layout produced by offline-image-builder.
"""
import argparse
import gzip
import hashlib
import io
import json
import shutil
import tarfile
import tempfile
from pathlib import Path


def digest(stream):
    return hashlib.file_digest(stream, "sha256").hexdigest()


def add_bytes(archive, name, data, mode=0o644):
    info = tarfile.TarInfo(name)
    info.size, info.mode = len(data), mode
    archive.addfile(info, io.BytesIO(data))


def write_app_layer(base, item, kind, output, binaries, web, nginx, version):
    old_config = base.extractfile(item["Config"]).read()
    if item["Config"] != "sha256:" + hashlib.sha256(old_config).hexdigest():
        raise ValueError("invalid source config digest")
    config = json.loads(old_config)
    if (config["os"], config["architecture"]) != ("linux", "amd64"):
        raise ValueError("expected Linux amd64 app image")
    if kind == "server" and config["config"].get("User") != "10001:10001":
        raise ValueError("cached server lacks expected non-root runtime configuration")
    files = {f"usr/local/bin/{p.name}": p for p in binaries.iterdir() if p.is_file()}
    if kind == "web":
        files = {"usr/share/nginx/html/" + p.relative_to(web).as_posix(): p
                 for p in web.rglob("*") if p.is_file()}
        if "usr/share/nginx/html/index.html" not in files:
            raise ValueError("missing built web index")
        files["etc/nginx/conf.d/default.conf"] = nginx

    with tempfile.TemporaryFile() as raw:
        with tarfile.open(fileobj=raw, mode="w") as new:
            with tarfile.open(fileobj=base.extractfile(item["Layers"][-1]), mode="r|*") as old:
                old_files = set()
                for member in old:
                    name = member.name.removeprefix("./")
                    if kind == "web":
                        if name.startswith("usr/share/nginx/html/") or name == "etc/nginx/conf.d/default.conf":
                            old_files.add(name)
                            continue
                        raise ValueError("unexpected cached web layer entry: " + name)
                    if name.startswith("usr/local/bin/dts"):
                        old_files.add(name)
                        continue
                    new.addfile(member, old.extractfile(member) if member.isfile() else None)
                if kind == "server" and ("usr/local/bin/dts-server" not in old_files or not old_files <= files.keys()):
                    raise ValueError("cached server binary inventory differs from current build")
                if kind == "web" and "usr/share/nginx/html/index.html" not in old_files:
                    raise ValueError("unrecognized cached web layer")
            if kind == "web":
                for path in sorted(web.rglob("*")):
                    if path.is_dir():
                        info = tarfile.TarInfo("usr/share/nginx/html/" + path.relative_to(web).as_posix())
                        info.type, info.mode = tarfile.DIRTYPE, 0o755
                        new.addfile(info)
            for name, path in sorted(files.items()):
                info = tarfile.TarInfo(name)
                info.mode = 0o755 if kind == "server" else 0o644
                info.size = path.stat().st_size
                with path.open("rb") as data:
                    new.addfile(info, data)
        raw.seek(0)
        config["rootfs"]["diff_ids"][-1] = "sha256:" + digest(raw)
        raw.seek(0)
        with tempfile.TemporaryFile() as compressed:
            with gzip.GzipFile(fileobj=compressed, mode="wb", compresslevel=3, mtime=0) as gz:
                shutil.copyfileobj(raw, gz)
            size = compressed.tell()
            compressed.seek(0)
            layer = digest(compressed) + ".tar.gz"
            compressed.seek(0)
            info = tarfile.TarInfo(layer)
            info.size = size
            output.addfile(info, compressed)
    config_data = json.dumps(config, separators=(",", ":")).encode()
    config_name = "sha256:" + hashlib.sha256(config_data).hexdigest()
    add_bytes(output, config_name, config_data)
    return {"Config": config_name, "Layers": item["Layers"][:-1] + [layer],
            "RepoTags": ["dts/" + kind + ":" + version]}


def refresh(args):
    cache, stage = args.cache.resolve(), args.stage.resolve()
    if stage == cache or stage.is_relative_to(cache):
        raise ValueError("cache and output must be separate")
    inventory = {}
    for line in (cache / "SHA256SUMS").read_text("ascii").splitlines():
        sha, path = line.split("  ", 1)
        inventory[path.removeprefix("./")] = sha
    def verified(rel):
        source = (cache / rel).resolve()
        if not source.is_relative_to(cache):
            raise ValueError("invalid cache path")
        with source.open("rb") as handle:
            if digest(handle) != inventory[rel]:
                raise ValueError("cached file checksum mismatch: " + rel)
        return source

    runtime_files = {"runtime/kubectl", "runtime/kubelet", "runtime/kubeadm",
                     "cluster/kylin-rpms.json", "cluster/image-provenance.json", "cluster/upstream-tools.json"}
    runtime_files.update(p for p in inventory if p.startswith(("cluster/runtime/", "cluster/network/", "cluster/rpms/")))
    for rel in sorted(runtime_files):
        dest = stage / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(verified(rel), dest)

    cluster_list = Path(__file__).with_name("cluster-images.txt")
    cluster = set(cluster_list.read_text().split())
    if set(verified("cluster/cluster-images.txt").read_text().split()) != cluster:
        raise ValueError("cached Kubernetes version differs from the requested cluster image list")
    old_version = verified("VERSION").read_text().strip()
    old_apps = {f"dts/{kind}:{old_version}": kind for kind in ("server", "web")}
    provenance = {"cached_version": old_version, "cached_archive_sha256": inventory["images/all-images.tar.gz"],
                  "app_image_inputs": {}, "app_image_outputs": {}}
    with tempfile.TemporaryFile() as expanded:
        with gzip.open(verified("images/all-images.tar.gz"), "rb") as source:
            shutil.copyfileobj(source, expanded)
        expanded.seek(0)
        target = stage / "images/all-images.tar.gz"
        with tarfile.open(fileobj=expanded) as base, gzip.open(target, "wb", compresslevel=1) as packed:
            manifest = json.load(base.extractfile("manifest.json"))
            tags = [tag for item in manifest for tag in item["RepoTags"]]
            if len(tags) != len(set(tags)) or set(tags) != cluster | set(old_apps) | {"dts/postgres:17"}:
                raise ValueError("cached image tags do not match runtime requirements")
            copied, updated = set(), []
            with tarfile.open(fileobj=packed, mode="w|") as result:
                for item in manifest:
                    kind = old_apps.get(item["RepoTags"][0])
                    tag = item["RepoTags"][0]
                    fresh = None
                    try:
                        source = base
                        if args.runtime_images and (kind or tag == "dts/postgres:17"):
                            archive = f"dts-{kind}-{args.version}.tar" if kind else "postgres-17.tar"
                            fresh = tarfile.open(args.runtime_images / archive)
                            replacements = json.load(fresh.extractfile("manifest.json"))
                            wanted = f"dts/{kind}:{args.version}" if kind else tag
                            if len(replacements) != 1 or replacements[0]["RepoTags"] != [wanted]:
                                raise ValueError("wrong tag in refreshed image " + archive)
                            source, item = fresh, replacements[0]
                        if kind or tag == "dts/postgres:17":
                            provenance["app_image_inputs"][item["RepoTags"][0]] = item["Config"]
                        if kind:
                            updated.append(write_app_layer(source, item, kind, result, args.bin, args.web, args.nginx, args.version))
                            members = item["Layers"][:-1]
                        else:
                            updated.append(item)
                            members = [item["Config"], *item["Layers"]]
                        if kind or tag == "dts/postgres:17":
                            output = updated[-1]
                            provenance["app_image_outputs"][output["RepoTags"][0]] = output["Config"]
                        for member in members:
                            if member not in copied:
                                result.addfile(source.getmember(member), source.extractfile(member))
                                copied.add(member)
                    finally:
                        if fresh:
                            fresh.close()
                add_bytes(result, "manifest.json", json.dumps(updated).encode())
    (stage / "runtime/image-cache-provenance.json").write_text(json.dumps(provenance, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for arg in ("cache", "stage", "bin", "web", "nginx"):
        parser.add_argument("--" + arg, type=Path, required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--runtime-images", type=Path, help="use fresh server/web/postgres Docker archives with the cached Kubernetes cluster")
    refresh(parser.parse_args())
