from __future__ import annotations

import copy
import hashlib
import io
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pikepdf
from pypdf import PdfReader
from pypdf.generic import ArrayObject, Destination, IndirectObject, NullObject

from docuforge.errors import FidelityError, InvalidDocumentError
from docuforge.page_commands import validate_page_state


@dataclass(frozen=True)
class PdfInfo:
    page_count: int
    pdf_version: str
    has_forms: bool
    has_outlines: bool
    has_named_destinations: bool
    has_optional_content: bool


@dataclass(frozen=True)
class MaterializationResult:
    pdf_bytes: bytes
    byte_sha256: str
    semantic_sha256: str
    page_count: int
    byte_reproducible: bool


def _page_reference_key(value: Any) -> tuple[int, int] | None:
    if isinstance(value, IndirectObject):
        return int(value.idnum), int(value.generation)
    reference = getattr(value, "indirect_reference", None)
    if isinstance(reference, IndirectObject):
        return int(reference.idnum), int(reference.generation)
    return None


def _box(page: Any, key: str) -> list[float]:
    box = page.get(key)
    if box is None:
        box = page.mediabox
    return [round(float(item), 5) for item in box]


def _content_sha256(page: Any) -> str:
    try:
        contents = page.get_contents()
        data = b"" if contents is None else contents.get_data()
    except Exception:
        data = b""
    return hashlib.sha256(data).hexdigest()


RESOURCE_DIGEST_BUDGET = 1 * 1024 * 1024


def _stream_digest(
    value: Any,
    budget: list[int],
    *,
    expected_decoded_bytes: int | None = None,
) -> str | None:
    try:
        resolved = value.get_object() if hasattr(value, "get_object") else value
        # Only decode resources whose expanded size is known and fits the
        # per-document inspection budget.  This keeps image-heavy PDFs from
        # turning a fidelity check into an unbounded decompression pass.
        if expected_decoded_bytes is None or expected_decoded_bytes > budget[0]:
            return None
        data = resolved.get_data()
    except Exception:
        return None
    if len(data) > budget[0]:
        return None
    budget[0] -= len(data)
    return hashlib.sha256(data).hexdigest()


def _image_decoded_size(value: Any) -> int | None:
    width = int(value.get("/Width", 0) or 0)
    height = int(value.get("/Height", 0) or 0)
    bits = int(value.get("/BitsPerComponent", 1 if value.get("/ImageMask") else 0) or 0)
    color_space = str(value.get("/ColorSpace", ""))
    components = {
        "/DeviceGray": 1,
        "/DeviceRGB": 3,
        "/DeviceCMYK": 4,
    }.get(color_space)
    if value.get("/ImageMask"):
        components = 1
    if not width or not height or not bits or components is None:
        return None
    return (width * height * bits * components + 7) // 8


def _color_space_signature(value: Any) -> Any:
    try:
        resolved = value.get_object() if hasattr(value, "get_object") else value
    except Exception:
        return str(value)
    if not isinstance(resolved, (list, ArrayObject)):
        return str(resolved)
    if not resolved:
        return []
    kind = str(resolved[0])
    if kind == "/ICCBased" and len(resolved) > 1:
        try:
            profile = resolved[1].get_object()
            return {
                "kind": kind,
                "components": int(profile.get("/N", 0) or 0),
                "alternate": str(profile.get("/Alternate", "")),
            }
        except Exception:
            return {"kind": kind}
    if kind == "/Indexed" and len(resolved) > 2:
        return {
            "kind": kind,
            "base": _color_space_signature(resolved[1]),
            "high_value": int(resolved[2]),
        }
    if kind == "/Separation" and len(resolved) > 2:
        return {
            "kind": kind,
            "name": str(resolved[1]),
            "alternate": _color_space_signature(resolved[2]),
        }
    if kind == "/DeviceN" and len(resolved) > 2:
        return {
            "kind": kind,
            "names": [str(item) for item in resolved[1]],
            "alternate": _color_space_signature(resolved[2]),
        }
    return {"kind": kind}


def _resource_signature(page: Any, budget: list[int]) -> dict[str, Any]:
    resources = page.get("/Resources") or {}
    try:
        resources = resources.get_object()
    except Exception:
        pass
    xobjects: list[dict[str, Any]] = []
    xobject_resources = resources.get("/XObject") or {}
    try:
        xobject_resources = xobject_resources.get_object()
    except Exception:
        pass
    for name, reference in sorted(xobject_resources.items()):
        try:
            value = reference.get_object()
        except Exception:
            continue
        xobjects.append(
            {
                "name": str(name),
                "subtype": str(value.get("/Subtype", "")),
                "width": int(value.get("/Width", 0) or 0),
                "height": int(value.get("/Height", 0) or 0),
                "color_space": _color_space_signature(value.get("/ColorSpace", "")),
                "bits_per_component": int(value.get("/BitsPerComponent", 0) or 0),
                "data_sha256": _stream_digest(
                    value,
                    budget,
                    expected_decoded_bytes=_image_decoded_size(value),
                ),
            }
        )
    fonts: list[dict[str, Any]] = []
    font_resources = resources.get("/Font") or {}
    try:
        font_resources = font_resources.get_object()
    except Exception:
        pass
    for name, reference in sorted(font_resources.items()):
        try:
            value = reference.get_object()
        except Exception:
            continue
        fonts.append(
            {
                "name": str(name),
                "subtype": str(value.get("/Subtype", "")),
                "base_font": str(value.get("/BaseFont", "")),
                "has_to_unicode": value.get("/ToUnicode") is not None,
            }
        )
    return {"xobjects": xobjects, "fonts": fonts}


def _resources_equivalent(expected: dict[str, Any], actual: dict[str, Any]) -> bool:
    if expected["fonts"] != actual["fonts"]:
        return False
    expected_xobjects = expected["xobjects"]
    actual_xobjects = actual["xobjects"]
    if len(expected_xobjects) != len(actual_xobjects):
        return False
    for left, right in zip(expected_xobjects, actual_xobjects, strict=True):
        left_structure = {
            key: value for key, value in left.items() if key != "data_sha256"
        }
        right_structure = {
            key: value for key, value in right.items() if key != "data_sha256"
        }
        if left_structure != right_structure:
            return False
        if (
            left["data_sha256"] is not None
            and right["data_sha256"] is not None
            and left["data_sha256"] != right["data_sha256"]
        ):
            return False
    return True


DESTINATION_PARAMETER_COUNTS = {
    "/XYZ": 3,
    "/Fit": 0,
    "/FitH": 1,
    "/FitV": 1,
    "/FitR": 4,
    "/FitB": 0,
    "/FitBH": 1,
    "/FitBV": 1,
}


def _resolve_required_object(value: Any, context: str) -> Any:
    if hasattr(value, "get_object"):
        try:
            return value.get_object()
        except Exception as exc:
            raise FidelityError(f"unable to inspect {context}") from exc
    return value


def _destination_parameter(value: Any, context: str) -> float | None:
    value = _resolve_required_object(value, context)
    if value is None or isinstance(value, NullObject):
        return None
    if isinstance(value, bool):
        raise FidelityError(f"invalid boolean parameter in {context}")
    try:
        return round(float(value), 8)
    except (TypeError, ValueError) as exc:
        raise FidelityError(f"unsupported parameter in {context}") from exc


def _destination_signature(
    reader: PdfReader,
    destination: Any,
    page_references: dict[tuple[int, int], int],
    *,
    context: str,
) -> dict[str, Any] | None:
    if destination is None:
        return None
    name: str | None = None
    if isinstance(destination, (str, bytes)):
        name = str(destination)
        destination = reader.named_destinations.get(name)
        if destination is None:
            return {
                "name": name,
                "target": None,
                "type": None,
                "parameters": [],
            }
    destination = _resolve_required_object(destination, context)
    if isinstance(destination, Destination):
        values = list(destination.dest_array)
    elif isinstance(destination, (ArrayObject, list)):
        values = list(destination)
    else:
        raise FidelityError(f"unsupported destination object in {context}")
    if len(values) < 2:
        raise FidelityError(f"incomplete destination in {context}")
    reference = _page_reference_key(values[0])
    target = page_references.get(reference) if reference is not None else None
    if reference is not None and target is None:
        raise FidelityError(f"destination references an unavailable page in {context}")
    destination_type = str(values[1])
    parameter_count = DESTINATION_PARAMETER_COUNTS.get(destination_type)
    if parameter_count is None:
        raise FidelityError(f"unsupported destination type {destination_type} in {context}")
    if len(values) != parameter_count + 2:
        raise FidelityError(f"invalid {destination_type} destination in {context}")
    parameters = [
        _destination_parameter(value, context)
        for value in values[2:]
    ]
    return {
        "name": name,
        "target": target,
        "type": destination_type,
        "parameters": parameters,
    }


def _action_signature(
    reader: PdfReader,
    action: Any,
    page_references: dict[tuple[int, int], int],
    *,
    context: str,
    unsupported_actions: list[dict[str, Any]],
) -> dict[str, Any] | None:
    if action is None:
        return None
    action = _resolve_required_object(action, context)
    if not hasattr(action, "get") or not hasattr(action, "keys"):
        raise FidelityError(f"invalid action dictionary in {context}")
    action_type = str(action.get("/S", ""))
    allowed_keys = {
        "/URI": {"/Type", "/S", "/URI", "/IsMap"},
        "/GoTo": {"/Type", "/S", "/D"},
        "/Named": {"/Type", "/S", "/N"},
    }.get(action_type)
    extra_keys = sorted(
        str(key) for key in action.keys() if str(key) not in (allowed_keys or set())
    )
    if allowed_keys is None or extra_keys:
        unsupported_actions.append(
            {"context": context, "type": action_type or "missing", "extra_keys": extra_keys}
        )
        return {
            "type": action_type or "missing",
            "supported": False,
            "extra_keys": extra_keys,
        }
    if action_type == "/URI":
        uri = action.get("/URI")
        if uri is None:
            raise FidelityError(f"URI action has no URI in {context}")
        return {
            "type": action_type,
            "supported": True,
            "uri": str(uri),
            "is_map": bool(action.get("/IsMap", False)),
        }
    if action_type == "/GoTo":
        destination = _destination_signature(
            reader,
            action.get("/D"),
            page_references,
            context=f"{context} destination",
        )
        if destination is None:
            raise FidelityError(f"GoTo action has no destination in {context}")
        return {
            "type": action_type,
            "supported": True,
            "destination": destination,
        }
    name = action.get("/N")
    if name is None:
        raise FidelityError(f"Named action has no name in {context}")
    return {"type": action_type, "supported": True, "name": str(name)}


def _annotation_signature(
    annotation: Any,
    reader: PdfReader,
    page_references: dict[tuple[int, int], int],
    *,
    context: str,
    unsupported_actions: list[dict[str, Any]],
) -> dict[str, Any]:
    appearance = annotation.get("/AP") or {}
    try:
        appearance = appearance.get_object()
    except Exception:
        pass
    appearance_signatures: list[str] = []
    if hasattr(appearance, "items"):
        for _, value in sorted(appearance.items(), key=lambda item: str(item[0])):
            try:
                states = value.get_object()
            except Exception:
                continue
            if hasattr(states, "items"):
                appearance_signatures.extend(str(state) for state in sorted(states))
            else:
                appearance_signatures.append(str(states.get("/Subtype", "/Stream")))
    field = annotation
    if "/T" not in field and field.get("/Parent") is not None:
        try:
            field = field["/Parent"].get_object()
        except Exception:
            pass
    rect = annotation.get("/Rect") or []
    direct_destination = _destination_signature(
        reader,
        annotation.get("/Dest"),
        page_references,
        context=f"{context} direct destination",
    )
    action = _action_signature(
        reader,
        annotation.get("/A"),
        page_references,
        context=f"{context} action",
        unsupported_actions=unsupported_actions,
    )
    additional_actions: dict[str, Any] = {}
    raw_additional = annotation.get("/AA")
    if raw_additional is not None:
        raw_additional = _resolve_required_object(raw_additional, f"{context} additional actions")
        if not hasattr(raw_additional, "items"):
            raise FidelityError(f"invalid additional-action dictionary in {context}")
        for trigger, value in sorted(raw_additional.items(), key=lambda item: str(item[0])):
            additional_actions[str(trigger)] = _action_signature(
                reader,
                value,
                page_references,
                context=f"{context} additional action {trigger}",
                unsupported_actions=unsupported_actions,
            )
    return {
        "subtype": str(annotation.get("/Subtype", "")),
        "rect": [round(float(item), 5) for item in rect],
        "contents": str(annotation.get("/Contents", "")),
        "flags": int(annotation.get("/F", 0) or 0),
        "color": [float(item) for item in (annotation.get("/C") or [])],
        "field_name": str(field.get("/T", "")),
        "field_type": str(field.get("/FT", "")),
        "field_value": str(field.get("/V", "")),
        "appearance_structure": sorted(appearance_signatures),
        "destination": direct_destination,
        "action": action,
        "additional_actions": additional_actions,
    }


def _outline_items(reader: PdfReader) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []

    def walk(values: Iterable[Any], level: int = 0) -> None:
        for value in values:
            if isinstance(value, list):
                walk(value, level + 1)
                continue
            title = str(getattr(value, "title", ""))
            try:
                page_index = int(reader.get_destination_page_number(value))
            except Exception:
                page_index = None
            items.append({"title": title, "target": page_index, "level": level})

    try:
        walk(reader.outline)
    except Exception:
        return []
    return items


def _resolve_destination(
    reader: PdfReader,
    destination: Any,
    page_references: dict[tuple[int, int], int],
) -> tuple[int | None, str | None]:
    signature = _destination_signature(
        reader,
        destination,
        page_references,
        context="link destination",
    )
    if signature is None:
        return None, None
    return signature["target"], signature["name"]


def inspect_semantics(source: Path | bytes) -> dict[str, Any]:
    stream: Any = io.BytesIO(source) if isinstance(source, bytes) else source
    reader = PdfReader(stream, strict=False)
    if reader.is_encrypted:
        raise InvalidDocumentError("encrypted PDFs are not supported in this milestone")

    page_references: dict[tuple[int, int], int] = {}
    for index, page in enumerate(reader.pages):
        reference = _page_reference_key(page)
        if reference is not None:
            page_references[reference] = index

    pages: list[dict[str, Any]] = []
    resource_digest_budget = [RESOURCE_DIGEST_BUDGET]
    required_named_destinations: set[str] = set()
    unsupported_actions: list[dict[str, Any]] = []
    for page_index, page in enumerate(reader.pages):
        annotations: Counter[str] = Counter()
        external_links: list[str] = []
        internal_links: list[dict[str, Any]] = []
        widget_fields: list[str] = []
        annotation_details: list[dict[str, Any]] = []
        for annotation_index, annotation_reference in enumerate(page.get("/Annots", []) or []):
            try:
                annotation = annotation_reference.get_object()
            except Exception as exc:
                raise FidelityError(
                    f"unable to inspect annotation {annotation_index + 1} on page {page_index + 1}"
                ) from exc
            subtype = str(annotation.get("/Subtype", "unknown"))
            annotations[subtype] += 1
            annotation_details.append(
                _annotation_signature(
                    annotation,
                    reader,
                    page_references,
                    context=f"page {page_index + 1} annotation {annotation_index + 1}",
                    unsupported_actions=unsupported_actions,
                )
            )
            if subtype == "/Widget":
                field = annotation
                if "/T" not in field and field.get("/Parent") is not None:
                    try:
                        field = field["/Parent"].get_object()
                    except Exception:
                        pass
                if field.get("/T") is not None:
                    widget_fields.append(str(field.get("/T")))
            if subtype != "/Link":
                continue
            action = annotation.get("/A")
            if action is not None:
                try:
                    action = action.get_object()
                except Exception:
                    pass
            if action is not None and str(action.get("/S", "")) == "/URI":
                external_links.append(str(action.get("/URI", "")))
                continue
            destination = annotation.get("/Dest")
            if destination is None and action is not None and str(action.get("/S", "")) == "/GoTo":
                destination = action.get("/D")
            target, name = _resolve_destination(reader, destination, page_references)
            if name and target is not None:
                required_named_destinations.add(name)
            internal_links.append({"target": target, "name": name})

        pages.append(
            {
                "source_index": page_index,
                "rotation": int(page.get("/Rotate", 0) or 0) % 360,
                "media_box": _box(page, "/MediaBox"),
                "crop_box": _box(page, "/CropBox"),
                "content_sha256": _content_sha256(page),
                "resources": _resource_signature(page, resource_digest_budget),
                "annotations": dict(sorted(annotations.items())),
                "annotation_details": sorted(
                    annotation_details,
                    key=lambda item: (
                        item["subtype"],
                        item["rect"],
                        item["field_name"],
                        item["contents"],
                    ),
                ),
                "external_links": sorted(external_links),
                "internal_links": internal_links,
                "widget_fields": sorted(widget_fields),
            }
        )

    fields = reader.get_fields() or {}
    named_destinations: dict[str, dict[str, Any]] = {}
    for name, destination in reader.named_destinations.items():
        signature = _destination_signature(
            reader,
            destination,
            page_references,
            context=f"named destination {name}",
        )
        if signature is None:
            raise FidelityError(f"named destination {name} is empty")
        signature["name"] = str(name)
        named_destinations[str(name)] = signature

    result = {
        "page_count": len(reader.pages),
        "pages": pages,
        "fields": sorted(str(name) for name in fields),
        "outlines": _outline_items(reader),
        "named_destinations": dict(sorted(named_destinations.items())),
        "required_named_destinations": sorted(required_named_destinations),
        "unsupported_actions": unsupported_actions,
        "has_optional_content": bool(reader.trailer["/Root"].get("/OCProperties")),
    }
    return result


def _semantic_hash(snapshot: dict[str, Any]) -> str:
    import json

    encoded = json.dumps(snapshot, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _remap_destination(
    destination: dict[str, Any] | None,
    page_map: dict[int, int],
) -> dict[str, Any] | None:
    if destination is None:
        return None
    remapped = copy.deepcopy(destination)
    target = remapped.get("target")
    if target is not None:
        if target not in page_map:
            raise FidelityError("destination target is not present in the output page map")
        remapped["target"] = page_map[target]
    return remapped


def _remap_action(
    action: dict[str, Any] | None,
    page_map: dict[int, int],
) -> dict[str, Any] | None:
    if action is None:
        return None
    remapped = copy.deepcopy(action)
    if action.get("type") == "/GoTo" and action.get("supported"):
        remapped["destination"] = _remap_destination(action["destination"], page_map)
    return remapped


def _remap_annotation(
    annotation: dict[str, Any],
    page_map: dict[int, int],
) -> dict[str, Any]:
    remapped = copy.deepcopy(annotation)
    remapped["destination"] = _remap_destination(annotation.get("destination"), page_map)
    remapped["action"] = _remap_action(annotation.get("action"), page_map)
    remapped["additional_actions"] = {
        trigger: _remap_action(action, page_map)
        for trigger, action in annotation.get("additional_actions", {}).items()
    }
    return remapped


def _expected_named_destinations(
    snapshot: dict[str, Any],
    page_map: dict[int, int],
) -> dict[str, dict[str, Any]]:
    expected: dict[str, dict[str, Any]] = {}
    for name, destination in snapshot["named_destinations"].items():
        target = destination.get("target")
        if target is None or target not in page_map:
            continue
        expected[name] = _remap_destination(destination, page_map)
    return dict(sorted(expected.items()))


def _pikepdf_destination_array(
    target: Any,
    destination: dict[str, Any],
) -> pikepdf.Array:
    destination_type = destination["type"]
    if destination_type not in DESTINATION_PARAMETER_COUNTS:
        raise FidelityError(f"unsupported destination type {destination_type}")
    values: list[Any] = [target, pikepdf.Name(destination_type)]
    values.extend(destination["parameters"])
    return pikepdf.Array(values)


class PdfEngine:
    name = "pikepdf"

    def validate(self, path: Path) -> PdfInfo:
        try:
            with pikepdf.Pdf.open(path) as pdf:
                if pdf.is_encrypted:
                    raise InvalidDocumentError(
                        "encrypted PDFs are not supported in this milestone"
                    )
                if not pdf.pages:
                    raise InvalidDocumentError("PDF contains no pages")
                warnings = pdf.check_pdf_syntax()
                if warnings:
                    raise InvalidDocumentError("PDF structural validation failed")
                root = pdf.Root
                return PdfInfo(
                    page_count=len(pdf.pages),
                    pdf_version=str(pdf.pdf_version),
                    has_forms=bool(root.get("/AcroForm")),
                    has_outlines=bool(root.get("/Outlines")),
                    has_named_destinations=bool(root.get("/Dests") or root.get("/Names")),
                    has_optional_content=bool(root.get("/OCProperties")),
                )
        except InvalidDocumentError:
            raise
        except pikepdf.PasswordError as exc:
            raise InvalidDocumentError("encrypted PDFs are not supported") from exc
        except pikepdf.PdfError as exc:
            raise InvalidDocumentError("unable to parse PDF structure") from exc

    def _preflight(
        self,
        source_snapshot: dict[str, Any],
        page_states: list[dict[str, Any]],
    ) -> None:
        if source_snapshot["has_optional_content"]:
            raise FidelityError(
                "PDF layers are not transformed in this milestone; save was blocked"
            )
        if source_snapshot["unsupported_actions"]:
            action = source_snapshot["unsupported_actions"][0]
            raise FidelityError(
                f"unsupported PDF action {action['type']} in {action['context']}; save was blocked"
            )
        selected = {int(page["source_index"]) for page in page_states}
        for page in source_snapshot["pages"]:
            if page["source_index"] not in selected:
                continue
            for link in page["internal_links"]:
                target = link["target"]
                if target is not None and target not in selected:
                    raise FidelityError(
                        "a retained page links to a removed page; save was blocked"
                    )

    def _verify_output(
        self,
        source: dict[str, Any],
        output: dict[str, Any],
        page_states: list[dict[str, Any]],
    ) -> None:
        if output["page_count"] != len(page_states):
            raise FidelityError("saved PDF page count does not match the page map")
        target_map = {
            int(page["source_index"]): output_index
            for output_index, page in enumerate(page_states)
        }
        for output_index, state in enumerate(page_states):
            source_page = source["pages"][state["source_index"]]
            output_page = output["pages"][output_index]
            expected_rotation = (source_page["rotation"] + state["rotation"]) % 360
            for key in (
                "media_box",
                "crop_box",
                "content_sha256",
                "annotations",
            ):
                if output_page[key] != source_page[key]:
                    raise FidelityError(f"saved PDF changed page {output_index + 1} {key}")
            expected_annotations = sorted(
                (
                    _remap_annotation(annotation, target_map)
                    for annotation in source_page["annotation_details"]
                ),
                key=lambda item: (
                    item["subtype"],
                    item["rect"],
                    item["field_name"],
                    item["contents"],
                ),
            )
            if output_page["annotation_details"] != expected_annotations:
                raise FidelityError(
                    f"saved PDF changed page {output_index + 1} annotation actions"
                )
            if not _resources_equivalent(source_page["resources"], output_page["resources"]):
                raise FidelityError(f"saved PDF changed page {output_index + 1} resources")
            if output_page["rotation"] != expected_rotation:
                raise FidelityError("saved PDF rotation does not match the page map")
            if output_page["external_links"] != source_page["external_links"]:
                raise FidelityError("saved PDF changed external links")
            expected_internal = [
                {
                    "target": (
                        target_map[link["target"]]
                        if link["target"] is not None
                        else None
                    ),
                    "name": link["name"],
                }
                for link in source_page["internal_links"]
            ]
            if output_page["internal_links"] != expected_internal:
                raise FidelityError("saved PDF changed internal link destinations")
            if output_page["widget_fields"] != source_page["widget_fields"]:
                raise FidelityError("saved PDF changed page form widgets")

        expected_fields = sorted(
            {
                field
                for state in page_states
                for field in source["pages"][state["source_index"]]["widget_fields"]
            }
        )
        if output["fields"] != expected_fields:
            raise FidelityError("saved PDF changed interactive form registration")

        expected_names = _expected_named_destinations(source, target_map)
        if output["named_destinations"] != expected_names:
            raise FidelityError("saved PDF changed named destination semantics")
        if source["has_optional_content"] and not output["has_optional_content"]:
            raise FidelityError("saved PDF lost optional-content configuration")

        expected_outlines = [
            {
                "title": item["title"],
                "target": target_map[item["target"]],
                "level": item["level"],
            }
            for item in source["outlines"]
            if item["target"] in target_map
        ]
        if output["outlines"] != expected_outlines:
            raise FidelityError("saved PDF changed retained bookmarks")

    def _restore_navigation(
        self,
        destination: pikepdf.Pdf,
        snapshot: dict[str, Any],
        page_map: dict[int, int],
        *,
        outline_offset: int = 0,
        outline_root: list[pikepdf.OutlineItem] | None = None,
    ) -> None:
        names = destination.Root.get("/Names")
        if not isinstance(names, pikepdf.Dictionary):
            names = destination.make_indirect(pikepdf.Dictionary())
            destination.Root["/Names"] = names
        dests = names.get("/Dests")
        if dests is None:
            dests = pikepdf.NameTree.new(destination).obj
            names["/Dests"] = dests
        name_tree = pikepdf.NameTree(dests)
        for name, source_destination in snapshot["named_destinations"].items():
            source_target = source_destination.get("target")
            if source_target is None or source_target not in page_map or name in name_tree:
                continue
            target = destination.pages[page_map[source_target]].obj
            name_tree[name] = destination.make_indirect(
                pikepdf.Dictionary(
                    D=_pikepdf_destination_array(target, source_destination)
                )
            )

        target_root = outline_root
        own_context = None
        if target_root is None:
            own_context = destination.open_outline()
            outline = own_context.__enter__()
            target_root = outline.root
        try:
            parents: dict[int, pikepdf.OutlineItem] = {}
            for item in snapshot["outlines"]:
                if item["target"] not in page_map:
                    continue
                level = int(item["level"])
                node = pikepdf.OutlineItem(
                    item["title"], page_map[item["target"]] + outline_offset
                )
                if level > 0 and (level - 1) in parents:
                    parents[level - 1].children.append(node)
                else:
                    target_root.append(node)
                parents[level] = node
                parents = {key: value for key, value in parents.items() if key <= level}
        finally:
            if own_context is not None:
                own_context.__exit__(None, None, None)

    def materialize(
        self,
        source_path: Path,
        page_states: list[dict[str, Any]],
    ) -> MaterializationResult:
        validate_page_state(page_states)
        info = self.validate(source_path)
        if any(page["source_index"] >= info.page_count for page in page_states):
            raise ValueError("page map references a page outside the base version")
        source_snapshot = inspect_semantics(source_path)
        self._preflight(source_snapshot, page_states)

        with pikepdf.Pdf.open(source_path) as source:
            destination = pikepdf.Pdf.new()
            try:
                copy_result = destination.add_pages_from(
                    source,
                    pages=[page["source_index"] for page in page_states],
                    forms="preserve",
                )
                if getattr(copy_result, "partial_fields", None):
                    raise FidelityError("page selection would create a partial form field")
                target_map = {
                    int(page["source_index"]): output_index
                    for output_index, page in enumerate(page_states)
                }
                self._restore_navigation(destination, source_snapshot, target_map)
                for destination_page, state in zip(destination.pages, page_states, strict=True):
                    current = int(destination_page.obj.get("/Rotate", 0) or 0)
                    rotation = (current + int(state["rotation"])) % 360
                    if rotation:
                        destination_page.obj["/Rotate"] = rotation
                    elif "/Rotate" in destination_page.obj:
                        del destination_page.obj["/Rotate"]
                output = io.BytesIO()
                destination.save(
                    output,
                    min_version=source.pdf_version,
                    deterministic_id=True,
                    object_stream_mode=pikepdf.ObjectStreamMode.generate,
                    compress_streams=True,
                )
                first = output.getvalue()
            finally:
                destination.close()

        output_snapshot = inspect_semantics(first)
        self._verify_output(source_snapshot, output_snapshot, page_states)

        second = self._serialize_again(source_path, page_states, source_snapshot)
        return MaterializationResult(
            pdf_bytes=first,
            byte_sha256=hashlib.sha256(first).hexdigest(),
            semantic_sha256=_semantic_hash(output_snapshot),
            page_count=len(page_states),
            byte_reproducible=first == second,
        )

    def _serialize_again(
        self,
        source_path: Path,
        page_states: list[dict[str, Any]],
        snapshot: dict[str, Any],
    ) -> bytes:
        with pikepdf.Pdf.open(source_path) as source:
            destination = pikepdf.Pdf.new()
            try:
                destination.add_pages_from(
                    source,
                    pages=[page["source_index"] for page in page_states],
                    forms="preserve",
                )
                page_map = {
                    int(page["source_index"]): output_index
                    for output_index, page in enumerate(page_states)
                }
                self._restore_navigation(destination, snapshot, page_map)
                for destination_page, state in zip(destination.pages, page_states, strict=True):
                    current = int(destination_page.obj.get("/Rotate", 0) or 0)
                    rotation = (current + int(state["rotation"])) % 360
                    if rotation:
                        destination_page.obj["/Rotate"] = rotation
                    elif "/Rotate" in destination_page.obj:
                        del destination_page.obj["/Rotate"]
                output = io.BytesIO()
                destination.save(
                    output,
                    min_version=source.pdf_version,
                    deterministic_id=True,
                    object_stream_mode=pikepdf.ObjectStreamMode.generate,
                    compress_streams=True,
                )
                return output.getvalue()
            finally:
                destination.close()

    def merge(self, sources: list[Path]) -> MaterializationResult:
        if not sources:
            raise ValueError("at least one source PDF is required")
        source_snapshots = [inspect_semantics(path) for path in sources]
        if any(snapshot["has_optional_content"] for snapshot in source_snapshots):
            raise FidelityError("PDF layers are not merged in this milestone")
        for snapshot in source_snapshots:
            if snapshot["unsupported_actions"]:
                action = snapshot["unsupported_actions"][0]
                raise FidelityError(
                    f"unsupported PDF action {action['type']} in "
                    f"{action['context']}; merge was blocked"
                )
        all_fields = [field for snapshot in source_snapshots for field in snapshot["fields"]]
        if len(all_fields) != len(set(all_fields)):
            raise FidelityError("merge contains conflicting form-field names")
        all_names = [
            name for snapshot in source_snapshots for name in snapshot["named_destinations"]
        ]
        if len(all_names) != len(set(all_names)):
            raise FidelityError("merge contains conflicting named destinations")
        destination = pikepdf.Pdf.new()
        try:
            with destination.open_outline() as outline:
                offset = 0
                for source_path, snapshot in zip(sources, source_snapshots, strict=True):
                    with pikepdf.Pdf.open(source_path) as source:
                        result = destination.add_pages_from(source, forms="preserve")
                        if getattr(result, "partial_fields", None):
                            raise FidelityError("merge would create a partial form field")
                    page_map = {
                        source_index: offset + source_index
                        for source_index in range(snapshot["page_count"])
                    }
                    self._restore_navigation(
                        destination,
                        snapshot,
                        page_map,
                        outline_root=outline.root,
                    )
                    offset += snapshot["page_count"]
            output = io.BytesIO()
            destination.save(
                output,
                deterministic_id=True,
                object_stream_mode=pikepdf.ObjectStreamMode.generate,
                compress_streams=True,
            )
            data = output.getvalue()
        finally:
            destination.close()
        snapshot = inspect_semantics(data)
        if snapshot["page_count"] != sum(item["page_count"] for item in source_snapshots):
            raise FidelityError("merged PDF page count is incorrect")
        self._verify_merge_output(source_snapshots, snapshot)
        second = self._merge_again(sources, source_snapshots)
        return MaterializationResult(
            pdf_bytes=data,
            byte_sha256=hashlib.sha256(data).hexdigest(),
            semantic_sha256=_semantic_hash(snapshot),
            page_count=snapshot["page_count"],
            byte_reproducible=data == second,
        )

    def _verify_merge_output(
        self,
        sources: list[dict[str, Any]],
        output: dict[str, Any],
    ) -> None:
        offset = 0
        expected_fields: set[str] = set()
        expected_outlines: list[dict[str, Any]] = []
        expected_named_destinations: dict[str, dict[str, Any]] = {}
        requires_optional_content = False
        for source in sources:
            page_map = {
                source_index: offset + source_index
                for source_index in range(source["page_count"])
            }
            for source_index, source_page in enumerate(source["pages"]):
                output_page = output["pages"][offset + source_index]
                for key in (
                    "rotation",
                    "media_box",
                    "crop_box",
                    "content_sha256",
                    "annotations",
                    "external_links",
                    "widget_fields",
                ):
                    if output_page[key] != source_page[key]:
                        raise FidelityError(f"merge changed page {offset + source_index + 1} {key}")
                expected_annotations = sorted(
                    (
                        _remap_annotation(annotation, page_map)
                        for annotation in source_page["annotation_details"]
                    ),
                    key=lambda item: (
                        item["subtype"],
                        item["rect"],
                        item["field_name"],
                        item["contents"],
                    ),
                )
                if output_page["annotation_details"] != expected_annotations:
                    raise FidelityError(
                        f"merge changed page {offset + source_index + 1} annotation actions"
                    )
                if not _resources_equivalent(
                    source_page["resources"], output_page["resources"]
                ):
                    raise FidelityError(
                        f"merge changed page {offset + source_index + 1} resources"
                    )
                expected_internal = [
                    {
                        "target": offset + link["target"] if link["target"] is not None else None,
                        "name": link["name"],
                    }
                    for link in source_page["internal_links"]
                ]
                if output_page["internal_links"] != expected_internal:
                    raise FidelityError("merge changed internal link destinations")
                expected_fields.update(source_page["widget_fields"])
            expected_outlines.extend(
                {
                    "title": item["title"],
                    "target": offset + item["target"] if item["target"] is not None else None,
                    "level": item["level"],
                }
                for item in source["outlines"]
            )
            expected_named_destinations.update(
                _expected_named_destinations(source, page_map)
            )
            requires_optional_content = requires_optional_content or source["has_optional_content"]
            offset += source["page_count"]
        if output["fields"] != sorted(expected_fields):
            raise FidelityError("merge changed interactive form registration")
        if output["outlines"] != expected_outlines:
            raise FidelityError("merge changed bookmarks")
        if output["named_destinations"] != dict(sorted(expected_named_destinations.items())):
            raise FidelityError("merge changed named destination semantics")
        if requires_optional_content and not output["has_optional_content"]:
            raise FidelityError("merge lost optional-content configuration")

    def _merge_again(
        self,
        sources: list[Path],
        snapshots: list[dict[str, Any]],
    ) -> bytes:
        destination = pikepdf.Pdf.new()
        try:
            with destination.open_outline() as outline:
                offset = 0
                for path, snapshot in zip(sources, snapshots, strict=True):
                    with pikepdf.Pdf.open(path) as source:
                        destination.add_pages_from(source, forms="preserve")
                    page_map = {
                        source_index: offset + source_index
                        for source_index in range(snapshot["page_count"])
                    }
                    self._restore_navigation(
                        destination,
                        snapshot,
                        page_map,
                        outline_root=outline.root,
                    )
                    offset += snapshot["page_count"]
            output = io.BytesIO()
            destination.save(
                output,
                deterministic_id=True,
                object_stream_mode=pikepdf.ObjectStreamMode.generate,
                compress_streams=True,
            )
            return output.getvalue()
        finally:
            destination.close()
