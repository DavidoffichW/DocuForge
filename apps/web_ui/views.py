from django.conf import settings
from django.http import HttpRequest, HttpResponse
from django.shortcuts import render


def _api_base() -> str:
    v = getattr(settings, "DOCUFORGE_API_BASE", "")
    if not isinstance(v, str):
        return ""
    return v.strip().rstrip("/")


def index(request: HttpRequest) -> HttpResponse:
    return render(
        request,
        "web_ui/index.html",
        {
            "api_base": _api_base(),
        },
    )


def document_view(request: HttpRequest, document_id: str) -> HttpResponse:
    api_base = _api_base()
    content_url = f"{api_base}/documents/{document_id}/content" if api_base else f"/documents/{document_id}/content"
    return render(
        request,
        "web_ui/document.html",
        {
            "api_base": api_base,
            "document_id": document_id,
            "content_url": content_url,
        },
    )


def views_page(request: HttpRequest) -> HttpResponse:
    return render(
        request,
        "web_ui/views.html",
        {
            "api_base": _api_base(),
        },
    )