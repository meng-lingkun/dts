import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest

import release


def archive(entries, mode="w"):
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode=mode) as out:
        for name, data in entries.items():
            m = tarfile.TarInfo(name)
            m.size = len(data)
            out.addfile(m, io.BytesIO(data))
    return buf.getvalue()


class OfflineSignatures(unittest.TestCase):
    def test_multitag_and_tampering(self):
        # A synthetic zero-finding scan exercises the gate. OpenSSL signs and
        # verifies actual bytes with ephemeral test keys.
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            out = base / "evidence"
            out.mkdir()
            scans = base / "archives"
            scans.mkdir()
            package = base / "sample.tar.gz"
            config = b'{"architecture":"amd64","os":"linux"}'
            img = hashlib.sha256(config).hexdigest()
            tags = ["example/app:v1", "example/app:pinned"]
            layer = archive({"data": b"sample"})
            row = {"Config": img, "RepoTags": tags, "Layers": ["layers/test.tar"]}
            inner = archive({"manifest.json": json.dumps([row]).encode(), img: config, "layers/test.tar": layer}, "w:gz")
            package.write_bytes(archive({"sample/images/all-images.tar.gz": inner}, "w:gz"))
            bom = release.inventory(package, scans)
            extracted = list(release.split_images(next(scans.iterdir()), base))
            self.assertEqual((img, tags), extracted[0][:2])
            release.write_json(out / "package.cdx.json", bom)
            scanpath, sbom = out / "image-000.trivy.json", out / "image-000.cdx.json"
            release.write_json(scanpath, {"SchemaVersion": 2, "ArtifactType": "container_image",
                                          "Metadata": {"ImageID": "sha256:" + img}, "Results": []})
            release.write_json(sbom, {"bomFormat": "CycloneDX", "specVersion":"1.5","version":1})
            manifest = {"schema": "dts-offline-release/v1", "trivy":"test fixture",
                               "package": package.name, "sha256": release.digest(package),
                               "inventory_sha256": release.digest(out / "package.cdx.json"),
                               "images": [{"config_sha256": img, "tags": tags, "high_critical": 0,
                                           "scan": scanpath.name, "scan_sha256": release.digest(scanpath),
                                           "sbom": sbom.name, "sbom_sha256": release.digest(sbom)}]}
            release.write_json(out / "scan-summary.json", release.scan_summary(out, manifest))
            manifest["summary_sha256"] = release.digest(out / "scan-summary.json")
            release.write_json(out / "release.json", manifest)
            openssl = os.environ.get("OPENSSL", "openssl")
            private, public = base / "test.key", base / "test.pub"
            subprocess.run([openssl, "genpkey", "-algorithm", "Ed25519", "-out", private], check=True)
            subprocess.run([openssl, "pkey", "-in", private, "-pubout", "-out", public], check=True)
            args = argparse.Namespace(out=out, package=package, action="sign", key=private, openssl=openssl)
            release.signature(args)
            args.action, args.key = "verify", public
            release.signature(args)
            args.action, args.tag, args.image_id = "verify-image", tags[0], "sha256:" + img
            release.signature(args)
            args.image_id = "sha256:" + "0"*64
            with self.assertRaises(ValueError):
                release.signature(args)
            args.action = "verify"
            for path in (package, scanpath, sbom, out / "scan-summary.json", out / "release.json", out / "release.ed25519"):
                original = path.read_bytes()
                path.write_bytes(original + b" ")
                with self.assertRaises((ValueError,subprocess.CalledProcessError)):
                    release.signature(args)
                path.write_bytes(original)
            with self.assertRaises(ValueError):
                release.compare_signing_inventory(package, out, {**manifest, "images": []})
            high = json.loads(scanpath.read_bytes())
            high["Results"] = [{"Vulnerabilities":[{"Severity":"HIGH"}]}]
            scanpath.write_text(json.dumps(high))
            self.assertEqual(1,release.inspect_scan(scanpath,img))


if __name__ == "__main__":
    unittest.main()
