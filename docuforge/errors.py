from __future__ import annotations


class DocuForgeError(Exception):
    code = "docuforge_error"
    status_code = 400


class InvalidDocumentError(DocuForgeError):
    code = "invalid_document"


class DocumentTooLargeError(DocuForgeError):
    code = "document_too_large"
    status_code = 413


class NotFoundError(DocuForgeError):
    code = "not_found"
    status_code = 404


class RevisionConflictError(DocuForgeError):
    code = "revision_conflict"
    status_code = 409


class FidelityError(DocuForgeError):
    code = "fidelity_check_failed"
    status_code = 409


class StorageError(DocuForgeError):
    code = "storage_error"
    status_code = 500
