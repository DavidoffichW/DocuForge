from __future__ import annotations

import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Tuple

from core.ordering import sort_dict
from core.capability_registry import build_registry


@dataclass(frozen=True)
class CanonicalizeResult:
    pdf_bytes: bytes
    manifest: Dict[str, object]


def _cap_available(name: str) -> bool:
    reg = build_registry()
    try:
        cap = reg.get(name)
    except KeyError:
        return False
    return getattr(cap, "status").name == "AVAILABLE"


def _write_temp_pdf(dir_path: Path, name: str, data: bytes) -> Path:
    p = dir_path / name
    p.write_bytes(data)
    return p


def _normalize_with_pikepdf(input_bytes: bytes) -> Tuple[bytes, Dict[str, object]]:
    import pikepdf

    with pikepdf.Pdf.open(input_bytes) as pdf:
        try:
            di = pdf.docinfo
            for k in list(di.keys()):
                del di[k]
        except Exception:
            pass

        try:
            pdf.remove_unreferenced_resources()
        except Exception:
            pass

        out = pikepdf.BytesIO()
        try:
            pdf.save(
                out,
                linearize=False,
                object_stream_mode=pikepdf.ObjectStreamMode.generate,
                compress_streams=True,
            )
        except Exception:
            pdf.save(out)
        return out.getvalue(), sort_dict({"stage": "pikepdf", "metadata_normalized": True})


def _canonicalize_with_qpdf(tmp_dir: Path, input_pdf: Path) -> bytes:
    output_pdf = tmp_dir / "out_qpdf.pdf"
    cmd = [
        "qpdf",
        "--deterministic-id",
        "--object-streams=generate",
        "--normalize-content=y",
        "--newline-before-endstream",
        str(input_pdf),
        str(output_pdf),
    ]
    subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return output_pdf.read_bytes()


def _canonicalize_with_mutool(tmp_dir: Path, input_pdf: Path) -> bytes:
    output_pdf = tmp_dir / "out_mutool.pdf"
    cmd = ["mutool", "clean", "-ggg", str(input_pdf), str(output_pdf)]
    subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return output_pdf.read_bytes()


def canonicalize_pdf(pdf_bytes: bytes) -> CanonicalizeResult:
    if not isinstance(pdf_bytes, (bytes, bytearray)) or len(pdf_bytes) == 0:
        raise ValueError("pdf_bytes must be non-empty bytes")

    qpdf_ok = _cap_available("canonicalizer.qpdf")
    mutool_ok = _cap_available("canonicalizer.mutool")
    pikepdf_ok = _cap_available("canonicalizer.pikepdf")

    if not (qpdf_ok or mutool_ok or pikepdf_ok):
        return CanonicalizeResult(
            pdf_bytes=bytes(pdf_bytes),
            manifest=sort_dict(
                {
                    "canonicalized": False,
                    "degraded": True,
                    "degradation": {"canonicalization": "skipped", "reason": "no_canonicalizer_capability"},
                }
            ),
        )

    stage1_bytes = bytes(pdf_bytes)
    stage1_manifest: Dict[str, object] = sort_dict({"stage": "none", "metadata_normalized": False})

    if pikepdf_ok:
        try:
            stage1_bytes, stage1_manifest = _normalize_with_pikepdf(stage1_bytes)
        except Exception:
            stage1_bytes = bytes(pdf_bytes)
            stage1_manifest = sort_dict({"stage": "pikepdf", "metadata_normalized": False, "error": "metadata_normalization_failed"})

    with tempfile.TemporaryDirectory() as td:
        tmp_dir = Path(td)
        in_path = _write_temp_pdf(tmp_dir, "in.pdf", stage1_bytes)

        provider = ""
        provider_version = ""
        out_bytes = stage1_bytes

        if qpdf_ok:
            provider = "qpdf"
            provider_version = ""
            try:
                out_bytes = _canonicalize_with_qpdf(tmp_dir, in_path)
            except Exception:
                out_bytes = stage1_bytes
        elif mutool_ok:
            provider = "mutool"
            provider_version = ""
            try:
                out_bytes = _canonicalize_with_mutool(tmp_dir, in_path)
            except Exception:
                out_bytes = stage1_bytes
        elif pikepdf_ok:
            provider = "pikepdf"
            provider_version = ""
            out_bytes = stage1_bytes

    canonicalized = out_bytes != bytes(pdf_bytes) or stage1_manifest.get("metadata_normalized") is True
    degraded = False
    degradation: Dict[str, object] = {}

    if (qpdf_ok or mutool_ok) and out_bytes == stage1_bytes and (qpdf_ok or mutool_ok):
        degradation = {"canonicalization": "partial", "reason": "cli_canonicalizer_failed"}
        degraded = True

    if pikepdf_ok and stage1_manifest.get("metadata_normalized") is False:
        degradation = {"canonicalization": "partial", "reason": "metadata_normalization_failed"}
        degraded = True

    manifest = {
        "canonicalized": bool(canonicalized),
        "degraded": bool(degraded),
        "canonicalizer": sort_dict({"provider": provider, "provider_version": provider_version}),
        "stage1": stage1_manifest,
    }
    if degraded:
        manifest["degradation"] = degradation

    return CanonicalizeResult(pdf_bytes=out_bytes, manifest=sort_dict(manifest))