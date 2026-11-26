"""Inventory, scan and authenticate a DTS offline tarball without a registry."""
import argparse
from datetime import datetime, timezone
import gzip
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import sys
import tarfile
import tempfile


def digest(path):
    with path.open("rb") as src:
        return hashlib.file_digest(src, "sha256").hexdigest()


def write_json(path, obj):
    with path.open("x", encoding="utf-8", newline="\n") as dst:
        json.dump(obj, dst, indent=2, sort_keys=True, ensure_ascii=True)
        dst.write("\n")


def component(name, sha, kind="file"):
    return {"type": kind, "name": name, "hashes": [{"alg": "SHA-256", "content": sha}]}


def inventory(package, images=None):
    sha = digest(package)
    files, seen = [], set()
    with tarfile.open(package, mode="r|gz") as tar:
        for info in tar:
            if info.isdir():
                continue
            name = PurePosixPath(info.name)
            if (not info.isfile() or name.is_absolute() or ".." in name.parts or
                    name in seen or "\\" in info.name or ":" in info.name):
                raise ValueError("unexpected or duplicate package member: " + info.name)
            seen.add(name)
            src = tar.extractfile(info)
            if images is not None and "images" in name.parts and name.name.endswith(".tar.gz"):
                # Inner layer paths are read from the archive, never extracted to disk.
                dest = images / f"{len(list(images.iterdir()))}.tar.gz"
                with dest.open("xb") as dst:
                    shutil.copyfileobj(src, dst)
                member_sha = digest(dest)
            else:
                member_sha = hashlib.file_digest(src, "sha256").hexdigest()
            files.append(component(str(name), member_sha))
    if not files:
        raise ValueError("empty release package")
    return {"bomFormat": "CycloneDX", "specVersion": "1.5", "version": 1,
            "metadata": {"component": component(package.name, sha, "application")},
            "components": files}


def split_images(bundle, work):
    with tempfile.TemporaryFile(dir=work) as expanded:
        with gzip.open(bundle, "rb") as compressed:
            shutil.copyfileobj(compressed, expanded)
        expanded.seek(0)
        with tarfile.open(fileobj=expanded, mode="r:") as base:
            names = {m.name for m in base.getmembers()}
            if len(names) != len(base.getmembers()):
                raise ValueError("duplicate Docker archive members")
            rows = json.load(base.extractfile("manifest.json"))
            for row in rows:
                config = base.getmember(row["Config"])
                if not config.isfile():
                    raise ValueError("invalid image configuration")
                image_id = hashlib.file_digest(base.extractfile(config), "sha256").hexdigest()
                tags = row["RepoTags"]
                if not tags or not all(isinstance(t, str) and t for t in tags):
                    raise ValueError("missing image tag")
                single = work / "single.tar"
                with tarfile.open(single, mode="w") as out:
                    for path in dict.fromkeys([row["Config"], *row["Layers"]]):
                        m = base.getmember(path)
                        if not m.isfile() or PurePosixPath(path).is_absolute() or ".." in PurePosixPath(path).parts:
                            raise ValueError("invalid layer member")
                        out.addfile(m, base.extractfile(m))
                    data = json.dumps([row]).encode()
                    manifest = tarfile.TarInfo("manifest.json")
                    manifest.size = len(data)
                    out.addfile(manifest, io.BytesIO(data))
                yield image_id, tags, single


def inspect_scan(path, image_id):
    report = json.loads(path.read_bytes())
    if (report.get("SchemaVersion") != 2 or report.get("ArtifactType") != "container_image" or
            report.get("Metadata", {}).get("ImageID") != "sha256:" + image_id):
        raise ValueError("unexpected Trivy image report")
    return sum(1 for result in (report.get("Results") or [])
               for finding in (result.get("Vulnerabilities") or [])
               if finding.get("Severity") in {"HIGH", "CRITICAL"})


def scan_summary(out, manifest):
    totals = {"HIGH": 0, "CRITICAL": 0, "with_fixed_version": 0, "without_fixed_version": 0}
    images = []
    unique = set()
    for image in manifest["images"]:
        report = json.loads((out / image["scan"]).read_bytes())
        counts = dict.fromkeys(totals, 0)
        groups = {}
        for target in report.get("Results") or []:
            for finding in target.get("Vulnerabilities") or []:
                severity = finding.get("Severity")
                if severity not in {"HIGH", "CRITICAL"}:
                    continue
                unique.add(finding["VulnerabilityID"])
                package = (target.get("Type", ""), finding["PkgName"], finding["InstalledVersion"])
                group = groups.setdefault(package, {"HIGH": 0, "CRITICAL": 0,
                                          "with_fixed_version": 0, "without_fixed_version": 0, "fixes": set()})
                fixed = finding.get("FixedVersion")
                fix_status = "with_fixed_version" if fixed else "without_fixed_version"
                for scope in (counts, group, totals):
                    scope[severity] += 1
                    scope[fix_status] += 1
                if fixed:
                    group["fixes"].add(fixed)
        components = []
        for (kind, name, version), result in sorted(groups.items()):
            components.append({"type": kind, "package": name, "installed": version, **result,
                               "fixes": sorted(result["fixes"])})
        images.append({"tags": image["tags"], "config_sha256": image["config_sha256"],
                       "report": image["scan"], "sbom": image["sbom"], "counts": counts, "components": components})
    return {"scanned_at_utc": datetime.now(timezone.utc).isoformat(), "package": manifest["package"],
            "sha256": manifest["sha256"], "trivy": manifest["trivy"], "counts": totals,
            "unique_vulnerability_ids": len(unique), "images": images}


def scan(args):
    args.out.mkdir(parents=True, exist_ok=False)
    with tempfile.TemporaryDirectory() as temp:
        work = Path(temp)
        images = work / "images"
        images.mkdir()
        bom = inventory(args.package, images)
        write_json(args.out / "package.cdx.json", bom)
        reports = []
        archive_sha = bom["metadata"]["component"]["hashes"][0]["content"]
        for bundle in sorted(images.iterdir()):
            for image_id, tags, single in split_images(bundle, work):
                prefix = f"image-{len(reports):03d}"
                sbom = args.out / (prefix + ".cdx.json")
                scanfile = args.out / (prefix + ".trivy.json")
                cmd = [args.trivy, "image", "--input", str(single), "--no-progress"]
                if args.offline_db:
                    cmd += ["--offline-scan", "--skip-db-update", "--skip-java-db-update", "--skip-check-update"]
                subprocess.run([*cmd, "--format", "cyclonedx", "--output", sbom], check=True)
                if json.loads(sbom.read_bytes()).get("bomFormat") != "CycloneDX":
                    raise ValueError("missing image SBOM")
                if args.action == "sbom":
                    reports.append({"config_sha256": image_id, "tags": sorted(tags),
                                    "sbom": sbom.name, "sbom_sha256": digest(sbom)})
                    continue
                subprocess.run([*cmd, "--scanners", "vuln", "--format", "json", "--output", scanfile], check=True)
                findings = inspect_scan(scanfile, image_id)
                reports.append({"config_sha256": image_id, "tags": sorted(tags),
                                "sbom": sbom.name, "sbom_sha256": digest(sbom),
                                "scan": scanfile.name, "scan_sha256": digest(scanfile),
                                "high_critical": findings})
    if not reports or digest(args.package) != archive_sha:
        raise ValueError("no scanned images or package changed during scan")
    if args.action == "sbom":
        write_json(args.out / "image-inventory.json", {"package_sha256": archive_sha, "images": reports})
        return
    manifest = {"schema": "dts-offline-release/v1",
               "package": args.package.name, "sha256": archive_sha,
               "inventory_sha256": digest(args.out / "package.cdx.json"), "images": reports,
               "trivy": subprocess.check_output([args.trivy, "--version"], text=True).strip()}
    summary = args.out / "scan-summary.json"
    assessment = scan_summary(args.out, manifest)
    write_json(summary, assessment)
    manifest["summary_sha256"] = digest(summary)
    write_json(args.out / "release.json", manifest)
    findings = assessment["counts"]
    if findings["HIGH"] or findings["CRITICAL"]:
        raise ValueError(f"release blocked: {findings['CRITICAL']} critical, {findings['HIGH']} high; see {summary}")


def validate(package, out):
    m = json.loads((out / "release.json").read_bytes())
    if (m.get("schema") != "dts-offline-release/v1" or m["package"] != package.name or
            m["sha256"] != digest(package) or not m["images"] or
            digest(out / "package.cdx.json") != m["inventory_sha256"] or
            digest(out / "scan-summary.json") != m["summary_sha256"]):
        raise ValueError("package or inventory mismatch")
    for idx, image in enumerate(m["images"]):
        prefix = f"image-{idx:03d}"
        if image["scan"] != prefix + ".trivy.json" or image["sbom"] != prefix + ".cdx.json":
            raise ValueError("unexpected evidence path")
        if (digest(out / image["sbom"]) != image["sbom_sha256"] or
                digest(out / image["scan"]) != image["scan_sha256"] or
                image["high_critical"] != 0 or inspect_scan(out / image["scan"], image["config_sha256"])):
            raise ValueError("image evidence mismatch or HIGH/CRITICAL vulnerabilities")
    return m


def compare_signing_inventory(package, out, manifest):
    # Re-derive coverage from the release bytes. A copied or accidentally
    # shortened evidence directory must never omit an image from the signing gate.
    with tempfile.TemporaryDirectory() as tmp:
        work = Path(tmp)
        images = work / "images"
        images.mkdir()
        bom = inventory(package, images)
        if bom != json.loads((out / "package.cdx.json").read_bytes()):
            raise ValueError("signed inventory does not describe the release contents")
        actual = sorted((image_id, sorted(tags)) for bundle in images.iterdir()
                        for image_id, tags, _ in split_images(bundle, work))
        claimed = sorted((i["config_sha256"], sorted(i["tags"])) for i in manifest["images"])
        if actual != claimed:
            raise ValueError("scan evidence does not cover the exact release images")


def signature(args):
    # Verify the exact manifest bytes against an independently supplied public
    # key before opening any evidence files referenced by the manifest.
    sign = args.action == "sign"
    manifest = args.out / "release.json"
    sig = args.out / "release.ed25519"
    cmd = [args.openssl, "pkeyutl", "-rawin", "-inkey", args.key, "-in", manifest]
    if sign:
        signed_manifest = validate(args.package, args.out)
        compare_signing_inventory(args.package, args.out, signed_manifest)
        # Restrict the key algorithm, including when OpenSSL supports others.
        pub = subprocess.check_output([args.openssl, "pkey", "-in", args.key, "-pubout", "-outform", "DER"])
        if len(pub) != 44 or pub[:12] != bytes.fromhex("302a300506032b6570032100"):
            raise ValueError("release signing requires an Ed25519 key")
        data = subprocess.check_output([*cmd, "-sign"])
        with sig.open("xb") as output:
            output.write(data)
    else:
        pub = subprocess.check_output([args.openssl, "pkey", "-pubin", "-in", args.key, "-pubout", "-outform", "DER"])
        if len(pub) != 44 or pub[:12] != bytes.fromhex("302a300506032b6570032100"):
            raise ValueError("release verification requires a pinned Ed25519 public key")
        subprocess.run([*cmd, "-verify", "-pubin", "-sigfile", sig], check=True)
        if args.action == "verify-image":
            m = json.loads(manifest.read_bytes())
            ids = {img["config_sha256"] for img in m["images"] if args.tag in img["tags"]}
            if m.get("schema") != "dts-offline-release/v1" or ids != {args.image_id.removeprefix("sha256:")}:
                raise ValueError("local tag or immutable image ID differs from signed release")
        else:
            validate(args.package, args.out)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    commands = p.add_subparsers(dest="action", required=True)
    for name in ("inventory", "sbom", "scan", "sign", "verify", "verify-image"):
        cmd = commands.add_parser(name)
        if name != "verify-image":
            cmd.add_argument("--package", required=True, type=Path)
        else:
            cmd.add_argument("--tag", required=True)
            cmd.add_argument("--image-id", required=True)
        cmd.add_argument("--out", required=True, type=Path)
        if name in ("scan", "sbom"):
            cmd.add_argument("--trivy", default="trivy")
            cmd.add_argument("--offline-db", action="store_true")
        if name in ("sign", "verify", "verify-image"):
            cmd.add_argument("--key", required=True, type=Path)
            cmd.add_argument("--openssl", default=os.environ.get("OPENSSL", "openssl"))
    args = p.parse_args()
    if args.action == "inventory":
        write_json(args.out, inventory(args.package))
    elif args.action in ("scan", "sbom"):
        scan(args)
    else:
        signature(args)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, subprocess.CalledProcessError, tarfile.TarError) as err:
        sys.exit(str(err))
