"""Build the optional static linux/amd64 recovery tool with its dependency notices."""
import argparse
import gzip
import hashlib
from io import BytesIO
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile

SOURCE = Path(__file__).resolve().parent
ROOT = SOURCE.parent.parent


def sha(data):
    return hashlib.sha256(data).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, default=ROOT / "dist")
    out = parser.parse_args().out.resolve()
    out.mkdir(parents=True, exist_ok=True)
    name = "dts-s3-recovery-" + (ROOT / "VERSION").read_text().strip() + "-linux-amd64"
    toolchain = (ROOT / ".go-version").read_text().strip()
    env = dict(os.environ, GOTOOLCHAIN="go" + toolchain, GOOS="linux", GOARCH="amd64", CGO_ENABLED="0")

    def go(*args):
        return subprocess.check_output(["go", *args], cwd=SOURCE, env=env, text=True)

    with tempfile.TemporaryDirectory(dir=out) as tmp:
        binary = Path(tmp) / "dts-s3-recovery"
        go("build", "-mod=readonly", "-trimpath", "-buildvcs=false", "-ldflags=-s -w", "-o", str(binary), ".")
        files = {"dts-s3-recovery": binary.read_bytes(), "README.md": (SOURCE / "README.md").read_bytes()}
        # Only retain modules linked into this executable and their original notices.
        template = "{{with .Module}}{{.Path}}|{{.Dir}}{{end}}"
        modules = set(go("list", "-mod=readonly", "-deps", "-f", template, ".").splitlines())
        for record in sorted(modules):
            if not record:
                continue
            module, directory = record.split("|", 1)
            if module.startswith("dts/"):
                continue
            notices = [p for p in Path(directory).iterdir() if p.is_file() and
                       p.name.upper().startswith(("LICENSE", "NOTICE", "COPYING"))]
            if not notices:
                raise RuntimeError(f"missing license for {module}")
            for notice in notices:
                files[f"licenses/{module}/{notice.name}"] = notice.read_bytes()
        files["licenses/go/LICENSE"] = (Path(go("env", "GOROOT").strip()) / "LICENSE").read_bytes()
        files["SHA256SUMS"] = "".join(f"{sha(data)}  {key}\n" for key, data in sorted(files.items())).encode()
        result = out / (name + ".tar.gz")
        staging = Path(tmp) / result.name
        with staging.open("wb") as raw, gzip.GzipFile(fileobj=raw, filename="", mode="wb", mtime=0) as gz:
            with tarfile.open(fileobj=gz, mode="w|", format=tarfile.GNU_FORMAT) as tar:
                for key, data in sorted(files.items()):
                    item = tarfile.TarInfo(name + "/" + key)
                    item.mode = 0o755 if key == "dts-s3-recovery" else 0o644
                    item.size = len(data)
                    tar.addfile(item, BytesIO(data))
        os.replace(staging, result)
        checksum = sha(result.read_bytes())
        result.with_suffix(".gz.sha256").write_text(f"{checksum}  {result.name}\n", encoding="ascii", newline="\n")
        print(f"{result} {result.stat().st_size} bytes sha256={checksum}")


if __name__ == "__main__":
    main()
