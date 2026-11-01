import * as pdfjsLib from "/assets/vendor/pdfjs/build/pdf.mjs";
import {
  EventBus,
  PDFFindController,
  PDFLinkService,
  PDFViewer,
} from "/assets/vendor/pdfjs/web/pdf_viewer.mjs";

pdfjsLib.GlobalWorkerOptions.workerSrc = "/assets/vendor/pdfjs/build/pdf.worker.mjs";

const $ = (selector) => document.querySelector(selector);
const all = (selector) => [...document.querySelectorAll(selector)];
const state = {
  workspaceId: "default",
  document: null,
  version: null,
  session: null,
  pdf: null,
  virtualPdf: null,
  loadingTask: null,
  selected: new Set(),
  thumbnailGeneration: 0,
  thumbnailQueue: [],
  thumbnailWorkers: 0,
  thumbnailCache: new Map(),
  thumbnailTasks: new Set(),
  viewerGeneration: 0,
  mutationQueue: Promise.resolve(),
  draggedPageId: null,
  tableCandidates: [],
  documents: [],
  mergeSources: [],
};

const eventBus = new EventBus();
const linkService = new PDFLinkService({ eventBus });
const findController = new PDFFindController({ eventBus, linkService });
const viewer = new PDFViewer({
  container: $("#viewerContainer"),
  viewer: $("#viewer"),
  eventBus,
  linkService,
  findController,
  textLayerMode: 1,
  annotationMode: pdfjsLib.AnnotationMode.ENABLE,
  enableHWA: true,
  removePageBorders: true,
});
linkService.setViewer(viewer);

function status(message) { $("#status-message").textContent = message; }
function busy(active, message = "Working…") {
  $("#progress").hidden = !active;
  $("#progress output").textContent = message;
  document.body.setAttribute("aria-busy", String(active));
}
function toast(message, kind = "info") {
  const element = $("#toast");
  element.querySelector(".toast-message").textContent = message;
  element.querySelector(".toast-icon use").setAttribute("href", kind === "error" ? "#icon-error" : "#icon-success");
  element.classList.remove("success", "error");
  if (kind !== "info") element.classList.add(kind);
  element.hidden = false;
  clearTimeout(toast.timer);
  toast.timer = setTimeout(() => { element.hidden = true; }, 5000);
}

async function api(path, options = {}) {
  const headers = new Headers(options.headers || {});
  if (options.body && !(options.body instanceof FormData)) headers.set("Content-Type", "application/json");
  if (options.method && options.method.toUpperCase() !== "GET") {
    headers.set("X-DocuForge-Client", "browser");
  }
  const response = await fetch(path, { ...options, headers });
  if (!response.ok) {
    let detail = { message: `Request failed (${response.status})` };
    try { detail = await response.json(); } catch (_) { /* controlled fallback */ }
    throw new Error(detail.message || detail.detail || "Request failed");
  }
  const type = response.headers.get("content-type") || "";
  return type.includes("application/json") ? response.json() : response;
}

async function importManagedFile(file) {
  const form = new FormData();
  form.append("file", file, file.name);
  form.append("title", file.name);
  return api(`/api/workspaces/${state.workspaceId}/documents`, { method: "POST", body: form });
}

async function openFile(file) {
  if (!file) return;
  busy(true, "Validating and importing PDF…");
  try {
    const imported = await importManagedFile(file);
    await openDocument(imported.document.id, imported.version.id);
    status(`Imported ${imported.pdf.page_count} pages`);
    await refreshDocuments();
  } catch (error) { toast(error.message, "error"); status("Import failed · choose a valid PDF to try again"); }
  finally { busy(false); $("#open-file").value = ""; }
}

async function openDocument(documentId, versionId = null) {
  busy(true, "Opening document…");
  try {
    const document = await api(`/api/documents/${documentId}`);
    const version = versionId ? document.versions.find((item) => item.id === versionId) : document.versions.at(-1);
    const session = await api(`/api/documents/${documentId}/sessions`, {
      method: "POST", body: JSON.stringify({ version_id: version.id }),
    });
    state.document = document;
    state.version = version;
    state.session = session;
    state.selected.clear();
    resetTableTools();
    await loadPdf(`/api/versions/${version.id}/content`);
    await renderSession({ focusPageId: session.pages[0]?.id, resetScale: true });
    status(`Opened ${document.title}`);
  } catch (error) { toast(error.message, "error"); status("Open failed · the previous workspace state is unchanged"); }
  finally { busy(false); }
}

async function loadPdf(url) {
  state.thumbnailGeneration += 1;
  state.thumbnailQueue = [];
  state.thumbnailCache.clear();
  cancelThumbnailTasks();
  if (state.loadingTask) await state.loadingTask.destroy().catch(() => {});
  state.loadingTask = pdfjsLib.getDocument({ url, disableAutoFetch: true, rangeChunkSize: 262144 });
  state.pdf = await state.loadingTask.promise;
  $("#empty-state").hidden = true;
  $("#viewerContainer").hidden = false;
}

function remapExplicitDestination(destination, pages) {
  if (!Array.isArray(destination) || !Number.isInteger(destination[0])) return destination;
  const virtualIndex = pages.findIndex((page) => page.source_index === destination[0]);
  if (virtualIndex < 0) return null;
  const mapped = [...destination];
  mapped[0] = virtualIndex;
  return mapped;
}

function createVirtualPage(sourcePage, pageState, virtualPageNumber) {
  return new Proxy(sourcePage, {
    get(target, property) {
      if (property === "pageNumber") return virtualPageNumber;
      if (property === "rotate") return (target.rotate + pageState.rotation) % 360;
      const value = Reflect.get(target, property, target);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
}

function createVirtualDocument(sourcePdf, sessionPages) {
  const pages = sessionPages.map((page) => ({ ...page }));
  const pageCache = new Map();
  const virtualPosition = (sourceIndex) => pages.findIndex((page) => page.source_index === sourceIndex);
  return new Proxy(sourcePdf, {
    get(target, property) {
      if (property === "numPages") return pages.length;
      if (property === "getPage") {
        return async (pageNumber) => {
          if (!Number.isInteger(pageNumber) || pageNumber < 1 || pageNumber > pages.length) {
            throw new Error("virtual page number is outside the editing session");
          }
          const pageState = pages[pageNumber - 1];
          const cacheKey = `${pageState.id}:${pageState.rotation}:${pageNumber}`;
          if (!pageCache.has(cacheKey)) {
            const sourcePage = await target.getPage(pageState.source_index + 1);
            pageCache.set(cacheKey, createVirtualPage(sourcePage, pageState, pageNumber));
          }
          return pageCache.get(cacheKey);
        };
      }
      if (property === "getPageIndex") {
        return async (reference) => {
          const sourceIndex = await target.getPageIndex(reference);
          const position = virtualPosition(sourceIndex);
          if (position < 0) throw new Error("destination page was removed from the editing session");
          return position;
        };
      }
      if (property === "cachedPageNumber") {
        return (reference) => {
          const sourcePageNumber = target.cachedPageNumber(reference);
          if (!sourcePageNumber) return null;
          const position = virtualPosition(sourcePageNumber - 1);
          return position < 0 ? null : position + 1;
        };
      }
      if (property === "getDestination") {
        return async (name) => remapExplicitDestination(await target.getDestination(name), pages);
      }
      if (property === "getDestinations") {
        return async () => {
          const destinations = await target.getDestinations();
          return Object.fromEntries(Object.entries(destinations).map(
            ([name, destination]) => [name, remapExplicitDestination(destination, pages)],
          ));
        };
      }
      const value = Reflect.get(target, property, target);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
}

function waitForViewerInitialization(generation) {
  return new Promise((resolve) => {
    const initialized = () => {
      eventBus.off("pagesinit", initialized);
      resolve(generation === state.viewerGeneration);
    };
    eventBus.on("pagesinit", initialized);
  });
}

async function applySessionToViewer({ focusPageId = null, fallbackIndex = 0, resetScale = false } = {}) {
  if (!state.pdf || !state.session?.pages.length) return;
  const generation = ++state.viewerGeneration;
  const previousScale = resetScale ? "page-width" : viewer.currentScaleValue || "page-width";
  const initialized = waitForViewerInitialization(generation);
  state.virtualPdf = createVirtualDocument(state.pdf, state.session.pages);
  linkService.setDocument(state.virtualPdf);
  viewer.setDocument(state.virtualPdf);
  const current = await initialized;
  if (!current) return;
  const mappedIndex = focusPageId
    ? state.session.pages.findIndex((page) => page.id === focusPageId)
    : -1;
  viewer.currentScaleValue = previousScale;
  viewer.currentPageNumber = (mappedIndex >= 0
    ? mappedIndex
    : Math.min(fallbackIndex, state.session.pages.length - 1)) + 1;
}

function visiblePageContext() {
  const index = Math.max(0, (viewer.currentPageNumber || 1) - 1);
  return { pageId: state.session?.pages[index]?.id || null, index };
}

async function renderSession(viewOptions = {}) {
  const session = state.session;
  $("#document-title").textContent = state.document.title;
  $("#dirty-marker").hidden = !session.dirty;
  $("#page-count").textContent = session.pages.length;
  $("#page-number").max = String(session.pages.length);
  const downloadLabel = session.dirty ? "Save and download" : "Download PDF";
  $("#save-as-button").setAttribute("aria-label", downloadLabel);
  $("#save-as-button").dataset.tooltip = session.dirty
    ? "Save a new immutable version, then download it"
    : "Download the current saved version";
  const enabled = Boolean(state.pdf);
  all("#save-button, #save-as-button, #undo-button, #redo-button, #previous-page, #next-page, #page-number, #zoom-out, #zoom-in, #zoom-select, #search-field, #search-button").forEach((element) => { element.disabled = !enabled; });
  $("#save-button").disabled = !session.dirty;
  $("#undo-button").disabled = !session.history.length;
  $("#redo-button").disabled = !session.redo.length;
  renderThumbnails();
  updateSelectionControls();
  updateTableControls();
  await applySessionToViewer(viewOptions);
}

function renderThumbnails() {
  const generation = ++state.thumbnailGeneration;
  state.thumbnailQueue = [];
  cancelThumbnailTasks();
  const list = $("#thumbnail-list");
  list.replaceChildren();
  const observer = new IntersectionObserver((entries) => {
    for (const entry of entries) {
      if (entry.isIntersecting) {
        observer.unobserve(entry.target);
        queueThumbnail(entry.target, generation);
      }
    }
  }, { root: list.closest(".left-panel"), rootMargin: "180px" });

  state.session.pages.forEach((page, index) => {
    const item = document.createElement("div");
    item.className = `thumbnail${state.selected.has(page.id) ? " selected" : ""}`;
    item.draggable = true;
    item.tabIndex = 0;
    item.setAttribute("aria-label", `Page ${index + 1}${page.rotation ? `, rotated ${page.rotation} degrees` : ""}`);
    item.dataset.pageId = page.id;
    item.dataset.sourceIndex = String(page.source_index);
    item.dataset.position = String(index);
    item.innerHTML = `
      <input type="checkbox" aria-label="Select page ${index + 1}" ${state.selected.has(page.id) ? "checked" : ""}>
      <div><div class="thumbnail-placeholder">Page ${index + 1}</div>
      <div class="thumbnail-meta"><span>${index + 1}</span></div></div>`;
    item.querySelector("input").addEventListener("change", (event) => {
      event.stopPropagation();
      event.target.checked ? state.selected.add(page.id) : state.selected.delete(page.id);
      item.classList.toggle("selected", event.target.checked);
      updateSelectionControls();
    });
    item.addEventListener("click", (event) => {
      if (event.target.matches("input")) return;
      viewer.currentPageNumber = index + 1;
    });
    item.addEventListener("keydown", (event) => {
      if (event.key !== "Enter" && event.key !== " ") return;
      event.preventDefault();
      viewer.currentPageNumber = index + 1;
    });
    item.addEventListener("dragstart", (event) => {
      state.draggedPageId = page.id;
      item.classList.add("dragging");
      event.dataTransfer.effectAllowed = "move";
      event.dataTransfer.setData("text/plain", page.id);
    });
    item.addEventListener("dragend", () => {
      state.draggedPageId = null;
      item.classList.remove("dragging");
      all(".drop-target").forEach((el) => el.classList.remove("drop-target"));
    });
    item.addEventListener("dragover", (event) => { event.preventDefault(); item.classList.add("drop-target"); });
    item.addEventListener("dragleave", () => item.classList.remove("drop-target"));
    item.addEventListener("drop", async (event) => {
      event.preventDefault();
      const pageId = event.dataTransfer.getData("text/plain") || state.draggedPageId;
      if (!pageId || pageId === item.dataset.pageId) return;
      await command({ kind: "move", page_id: pageId, to_index: Number(item.dataset.position) });
    });
    list.append(item);
    observer.observe(item);
  });
  markCurrentThumbnail(viewer.currentPageNumber || 1);
}

function queueThumbnail(element, generation) {
  state.thumbnailQueue.push({ element, generation });
  pumpThumbnails();
}

function pumpThumbnails() {
  while (state.thumbnailWorkers < 4 && state.thumbnailQueue.length) {
    const job = state.thumbnailQueue.shift();
    state.thumbnailWorkers += 1;
    renderThumbnail(job).finally(() => { state.thumbnailWorkers -= 1; pumpThumbnails(); });
  }
}

async function renderThumbnail({ element, generation }) {
  if (generation !== state.thumbnailGeneration || !element.isConnected) return;
  const sourceIndex = Number(element.dataset.sourceIndex);
  const cacheKey = `${state.version.id}:${sourceIndex}`;
  let page = state.thumbnailCache.get(cacheKey);
  if (!page) {
    page = await state.pdf.getPage(sourceIndex + 1);
    state.thumbnailCache.set(cacheKey, page);
    if (state.thumbnailCache.size > 40) state.thumbnailCache.delete(state.thumbnailCache.keys().next().value);
  }
  if (generation !== state.thumbnailGeneration || !element.isConnected) return;
  const pageState = state.session.pages.find((item) => item.id === element.dataset.pageId);
  if (!pageState) return;
  const viewport = page.getViewport({
    scale: 0.23,
    rotation: (page.rotate + pageState.rotation) % 360,
  });
  const canvas = document.createElement("canvas");
  canvas.width = Math.ceil(viewport.width);
  canvas.height = Math.ceil(viewport.height);
  const renderTask = page.render({ canvasContext: canvas.getContext("2d"), viewport });
  state.thumbnailTasks.add(renderTask);
  try { await renderTask.promise; } catch (error) { if (error?.name !== "RenderingCancelledException") throw error; }
  finally { state.thumbnailTasks.delete(renderTask); }
  if (generation !== state.thumbnailGeneration || !element.isConnected) { renderTask.cancel(); return; }
  element.querySelector(".thumbnail-placeholder").replaceWith(canvas);
}

function cancelThumbnailTasks() {
  for (const task of state.thumbnailTasks) task.cancel();
  state.thumbnailTasks.clear();
}

function updateSelectionControls() {
  const selected = state.selected.size > 0;
  all("#rotate-left, #rotate-right, #delete-pages, #extract-pages").forEach((element) => { element.disabled = !selected; });
  $("#delete-pages").disabled = !selected || state.selected.size >= (state.session?.pages.length || 0);
}

function markCurrentThumbnail(pageNumber) {
  all(".thumbnail").forEach((element, index) => {
    const current = index + 1 === pageNumber;
    element.classList.toggle("current", current);
    if (current) element.setAttribute("aria-current", "page");
    else element.removeAttribute("aria-current");
  });
}

async function command(payload) {
  if (!state.session) return;
  return queueMutation(async () => {
    const focus = visiblePageContext();
    busy(true, "Updating page plan…");
    try {
      state.session = await api(`/api/sessions/${state.session.id}/commands`, {
        method: "POST",
        body: JSON.stringify({ expected_revision: state.session.revision, command: payload }),
      });
      for (const id of [...state.selected]) if (!state.session.pages.some((page) => page.id === id)) state.selected.delete(id);
      await renderSession({ focusPageId: focus.pageId, fallbackIndex: focus.index });
      status("Page plan updated; save to materialize the version");
    } catch (error) { toast(error.message, "error"); status("Page command failed · no changes were applied"); }
    finally { busy(false); }
  });
}

async function runRevisionAction(action) {
  return queueMutation(async () => {
    const focus = visiblePageContext();
    busy(true, `${action === "undo" ? "Undoing" : "Redoing"}…`);
    try {
      state.session = await api(`/api/sessions/${state.session.id}/${action}`, {
        method: "POST", body: JSON.stringify({ expected_revision: state.session.revision }),
      });
      await renderSession({ focusPageId: focus.pageId, fallbackIndex: focus.index });
      status(action === "undo" ? "Last page command undone" : "Page command restored");
    } catch (error) { toast(error.message, "error"); status(`${action === "undo" ? "Undo" : "Redo"} failed · document state is unchanged`); }
    finally { busy(false); }
  });
}

function queueMutation(operation) {
  const queued = state.mutationQueue.then(operation, operation);
  state.mutationQueue = queued.catch(() => {});
  return queued;
}

async function save(downloadAfter = false) {
  if (!state.session) return;
  return queueMutation(async () => {
    busy(true, "Validating and saving version…");
    try {
      const result = await api(`/api/sessions/${state.session.id}/save`, {
        method: "POST", body: JSON.stringify({ expected_revision: state.session.revision }),
      });
      state.version = result.version;
      state.session = result.session;
      resetTableTools();
      await loadPdf(`/api/versions/${state.version.id}/content`);
      await renderSession({ focusPageId: state.session.pages[0]?.id, resetScale: true });
      status(`Saved immutable version ${state.version.version_number}`);
      if (downloadAfter) download(`/api/versions/${state.version.id}/content`, state.document.title);
      const saveButton = $("#save-button");
      saveButton.classList.remove("save-complete");
      requestAnimationFrame(() => saveButton.classList.add("save-complete"));
      toast(downloadAfter ? "Version saved and download started" : `Version ${state.version.version_number} saved`, "success");
    } catch (error) { toast(error.message, "error"); status("Save blocked · the previous version remains available"); }
    finally { busy(false); }
  });
}

async function extractSelected() {
  return queueMutation(async () => {
    busy(true, "Extracting selected pages…");
    try {
      const result = await api(`/api/sessions/${state.session.id}/extract`, {
        method: "POST",
        body: JSON.stringify({ expected_revision: state.session.revision, page_ids: [...state.selected] }),
      });
      download(`/api/artifacts/${result.artifact.id}/content`, "extracted-pages.pdf");
      status("Extracted pages without changing the document");
    } catch (error) { toast(error.message, "error"); status("Extraction failed · the document was not changed"); }
    finally { busy(false); }
  });
}

function download(url, filename) {
  const anchor = document.createElement("a");
  anchor.href = url; anchor.download = filename; document.body.append(anchor); anchor.click(); anchor.remove();
}

function downloadCurrent() {
  if (!state.version) return;
  if (state.session?.dirty) save(true);
  else {
    download(`/api/versions/${state.version.id}/content`, state.document.title);
    status(`Downloading saved version ${state.version.version_number}`);
    toast("Download started", "success");
  }
}

function confirmDelete() {
  const count = state.selected.size;
  if (!count) return Promise.resolve(false);
  const dialog = $("#delete-dialog");
  $("#delete-dialog-title").textContent = `Remove ${count} ${count === 1 ? "page" : "pages"}?`;
  $("#delete-dialog-message").textContent = "The pending page plan will change immediately. You can undo this command before or after saving a version.";
  return new Promise((resolve) => {
    dialog.addEventListener("close", () => resolve(dialog.returnValue === "confirm"), { once: true });
    dialog.showModal();
  });
}

async function refreshDocuments() {
  try {
    const documents = await api(`/api/workspaces/${state.workspaceId}/documents`);
    state.documents = documents;
    const list = $("#document-list");
    list.replaceChildren();
    for (const document of documents) {
      const card = window.document.createElement("button");
      card.className = "document-card";
      card.innerHTML = `<span><strong></strong><small></small></span>`;
      card.querySelector("strong").textContent = document.title;
      card.querySelector("small").textContent = `${document.page_count} pages · latest version`;
      card.addEventListener("click", () => openDocument(document.id, document.latest_version_id));
      list.append(card);
    }
    refreshMergePicker();
  } catch (error) { toast(error.message, "error"); }
}

function refreshMergePicker() {
  const picker = $("#merge-document-select");
  picker.replaceChildren();
  for (const document of state.documents) {
    const option = window.document.createElement("option");
    option.value = document.latest_version_id;
    option.textContent = `${document.title} · ${document.page_count} pages`;
    picker.append(option);
  }
  $("#merge-add-document").disabled = !state.documents.length;
  renderMergeQueue();
}

function renderMergeQueue() {
  const list = $("#merge-queue");
  list.replaceChildren();
  state.mergeSources.forEach((source, index) => {
    const item = window.document.createElement("li");
    const label = window.document.createElement("span");
    label.textContent = `${source.title} · ${source.pageCount} pages`;
    const controls = window.document.createElement("span");
    controls.className = "merge-order-controls";
    for (const [text, delta, action] of [["↑", -1, "Move up"], ["↓", 1, "Move down"]]) {
      const button = window.document.createElement("button");
      button.type = "button";
      button.textContent = text;
      button.setAttribute("aria-label", `${action} ${source.title}`);
      button.disabled = index + delta < 0 || index + delta >= state.mergeSources.length;
      button.addEventListener("click", () => {
        const next = index + delta;
        [state.mergeSources[index], state.mergeSources[next]] = [state.mergeSources[next], state.mergeSources[index]];
        renderMergeQueue();
      });
      controls.append(button);
    }
    const remove = window.document.createElement("button");
    remove.type = "button";
    remove.textContent = "×";
    remove.setAttribute("aria-label", `Remove ${source.title} from merge`);
    remove.addEventListener("click", () => {
      state.mergeSources.splice(index, 1);
      renderMergeQueue();
    });
    controls.append(remove);
    item.append(label, controls);
    list.append(item);
  });
  $("#merge-documents").disabled = state.mergeSources.length < 2;
}

function addMergeSource(source) {
  state.mergeSources.push(source);
  renderMergeQueue();
}

function addSelectedMergeDocument() {
  const versionId = $("#merge-document-select").value;
  const document = state.documents.find((item) => item.latest_version_id === versionId);
  if (!document) return;
  addMergeSource({
    versionId,
    title: document.title,
    pageCount: document.page_count,
  });
}

async function importMergeFiles(files) {
  if (!files.length) return;
  busy(true, `Importing ${files.length} PDF${files.length === 1 ? "" : "s"}…`);
  try {
    for (const file of files) {
      const imported = await importManagedFile(file);
      addMergeSource({
        versionId: imported.version.id,
        title: imported.document.title,
        pageCount: imported.version.page_count,
      });
    }
    await refreshDocuments();
    status(`Imported ${files.length} merge source${files.length === 1 ? "" : "s"}`);
  } catch (error) {
    toast(error.message, "error");
    status("Merge-source import failed");
  } finally {
    busy(false);
    $("#merge-files").value = "";
  }
}

async function mergeDocuments() {
  if (state.mergeSources.length < 2) return;
  busy(true, `Merging ${state.mergeSources.length} PDFs…`);
  try {
    const result = await api(`/api/workspaces/${state.workspaceId}/documents/merge`, {
      method: "POST",
      body: JSON.stringify({
        title: $("#merge-title").value || "Merged document.pdf",
        version_ids: state.mergeSources.map((source) => source.versionId),
      }),
    });
    state.mergeSources = [];
    renderMergeQueue();
    await refreshDocuments();
    await openDocument(result.document.id, result.version.id);
    status(`Merged ${result.source_version_ids.length} PDFs into ${result.version.page_count} pages`);
    toast("Merged document created", "success");
  } catch (error) {
    toast(error.message, "error");
    status("Merge failed · source documents were not changed");
  } finally { busy(false); }
}

function resetTableTools() {
  state.tableCandidates = [];
  const list = $("#table-candidates");
  if (list) list.replaceChildren();
  updateTableControls();
}

function updateTableControls() {
  const hasSavedVersion = Boolean(state.version);
  const dirty = Boolean(state.session?.dirty);
  const detect = $("#detect-tables");
  if (!detect) return;
  detect.disabled = !hasSavedVersion || dirty;
  const help = $("#etl-document-help");
  if (!hasSavedVersion) help.textContent = "Open a PDF, then detect tables in its saved version.";
  else if (dirty) help.textContent = "Save pending page changes before extracting tables.";
  else if (state.tableCandidates.length) help.textContent = `${state.tableCandidates.length} table candidate${state.tableCandidates.length === 1 ? "" : "s"} found.`;
  else help.textContent = "Detection checks each page with pdfplumber, then uses a text-grid fallback.";
  const selected = all(".table-candidate input:checked").length > 0;
  all("[data-table-export]").forEach((button) => { button.disabled = !selected; });
}

function tablePreview(candidate) {
  const wrapper = document.createElement("div");
  wrapper.className = "table-preview";
  const table = document.createElement("table");
  const body = document.createElement("tbody");
  candidate.grid.slice(0, 5).forEach((values) => {
    const row = document.createElement("tr");
    values.slice(0, 6).forEach((value) => {
      const cell = document.createElement("td");
      cell.textContent = value;
      cell.title = value;
      row.append(cell);
    });
    body.append(row);
  });
  table.append(body);
  wrapper.append(table);
  return wrapper;
}

function renderTableCandidates() {
  const list = $("#table-candidates");
  list.replaceChildren();
  state.tableCandidates.forEach((candidate) => {
    const label = document.createElement("label");
    label.className = "table-candidate";
    const checkbox = document.createElement("input");
    checkbox.type = "checkbox";
    checkbox.value = candidate.id;
    checkbox.setAttribute("aria-label", `Select table ${candidate.table_index + 1} on page ${candidate.page_number}`);
    checkbox.addEventListener("change", updateTableControls);
    const copy = document.createElement("span");
    const title = document.createElement("strong");
    title.textContent = `Page ${candidate.page_number} · table ${candidate.table_index + 1}`;
    const detail = document.createElement("small");
    detail.textContent = `${candidate.row_count} × ${candidate.column_count} · ${candidate.provider}`;
    copy.append(title, detail);
    label.append(checkbox, copy, tablePreview(candidate));
    list.append(label);
  });
  updateTableControls();
}

async function detectCurrentTables() {
  if (!state.version || state.session?.dirty) return;
  busy(true, "Detecting tables…");
  try {
    const result = await api(`/api/versions/${state.version.id}/tables/detect`, { method: "POST" });
    state.tableCandidates = result.candidates;
    renderTableCandidates();
    const message = result.status === "NO_TABLE"
      ? "No text-based tables were found"
      : `Found ${result.candidates.length} table candidate${result.candidates.length === 1 ? "" : "s"}`;
    status(message);
    toast(message, result.status === "FAILED" ? "error" : "success");
  } catch (error) {
    resetTableTools();
    toast(error.message, "error");
    status("Table detection failed");
  } finally { busy(false); }
}

function selectedTablePayload() {
  const mode = $("#table-slice-mode").value;
  const row = Number($("#table-slice-row").value);
  const column = Number($("#table-slice-column").value);
  return all(".table-candidate input:checked").map((input) => ({
    candidate_id: input.value,
    mode,
    row: mode === "row" || mode === "cell" ? row : null,
    column: mode === "column" || mode === "cell" ? column : null,
  }));
}

function responseFilename(response, fallback) {
  const disposition = response.headers.get("content-disposition") || "";
  const match = disposition.match(/filename="([^"]+)"/i);
  return match?.[1] || fallback;
}

async function downloadResponse(response, fallback) {
  const blob = await response.blob();
  const url = URL.createObjectURL(blob);
  download(url, responseFilename(response, fallback));
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

async function exportSelectedTables(format) {
  const selections = selectedTablePayload();
  if (!state.version || !selections.length) return;
  busy(true, `Building ${format.toUpperCase()} export…`);
  try {
    const response = await api(`/api/versions/${state.version.id}/tables/export`, {
      method: "POST",
      body: JSON.stringify({ format, selections }),
    });
    await downloadResponse(response, `docuforge-tables.${format}`);
    status(`Downloaded ${format.toUpperCase()} table export`);
    toast("Table export ready", "success");
  } catch (error) { toast(error.message, "error"); status("Table export failed"); }
  finally { busy(false); }
}

function updateSliceFields() {
  const mode = $("#table-slice-mode").value;
  $("#slice-row-field").hidden = mode !== "row" && mode !== "cell";
  $("#slice-column-field").hidden = mode !== "column" && mode !== "cell";
}

async function exportBatchTables() {
  const files = [...$("#batch-files").files, ...$("#batch-folder").files];
  if (!files.length) {
    toast("Choose one or more PDFs for the batch", "error");
    return;
  }
  const form = new FormData();
  files.forEach((file) => form.append("files", file, file.name));
  form.append("page", $("#batch-page").value);
  form.append("table", $("#batch-table").value);
  form.append("mode", "table");
  form.append("include_xlsx", "true");
  busy(true, `Processing ${files.length} PDFs…`);
  try {
    const response = await api("/api/tables/batch-export", { method: "POST", body: form });
    const success = response.headers.get("x-docuforge-batch-success") || "0";
    const failure = response.headers.get("x-docuforge-batch-failure") || "0";
    await downloadResponse(response, "docuforge-batch-tables.zip");
    status(`Batch complete · ${success} succeeded · ${failure} failed`);
    toast(`Batch ready · ${success} succeeded · ${failure} failed`, failure === "0" ? "success" : "info");
    $("#batch-files").value = "";
    $("#batch-folder").value = "";
  } catch (error) { toast(error.message, "error"); status("Batch export failed"); }
  finally { busy(false); }
}

eventBus.on("pagesinit", () => { viewer.currentScaleValue = "page-width"; });
eventBus.on("pagechanging", ({ pageNumber }) => {
  $("#page-number").value = pageNumber;
  markCurrentThumbnail(pageNumber);
});
eventBus.on("pagesloaded", ({ pagesCount }) => { $("#page-count").textContent = state.session?.pages.length ?? pagesCount; });

$("#open-file").addEventListener("change", (event) => openFile(event.target.files[0]));
$("#open-button").addEventListener("click", () => $("#open-file").click());
$("#empty-open-button").addEventListener("click", () => $("#open-file").click());
$("#save-button").addEventListener("click", () => save(false));
$("#save-as-button").addEventListener("click", downloadCurrent);
$("#undo-button").addEventListener("click", () => runRevisionAction("undo"));
$("#redo-button").addEventListener("click", () => runRevisionAction("redo"));
$("#previous-page").addEventListener("click", () => viewer.previousPage());
$("#next-page").addEventListener("click", () => viewer.nextPage());
$("#page-number").addEventListener("change", (event) => { viewer.currentPageNumber = Number(event.target.value); });
$("#zoom-select").addEventListener("change", (event) => { viewer.currentScaleValue = event.target.value; });
$("#zoom-in").addEventListener("click", () => viewer.increaseScale({ steps: 1 }));
$("#zoom-out").addEventListener("click", () => viewer.decreaseScale({ steps: 1 }));
function closeSearch() {
  const tools = $("#search-tools");
  const field = $("#search-field");
  tools.classList.remove("expanded");
  $("#search-button").setAttribute("aria-expanded", "false");
  field.value = "";
  field.hidden = true;
  eventBus.dispatch("find", { source: window, type: "", query: "", phraseSearch: true, highlightAll: true, findPrevious: false });
}

$("#search-button").addEventListener("click", () => {
  const tools = $("#search-tools");
  const field = $("#search-field");
  if (!tools.classList.contains("expanded")) {
    tools.classList.add("expanded");
    field.hidden = false;
    $("#search-button").setAttribute("aria-expanded", "true");
    requestAnimationFrame(() => field.focus());
    return;
  }
  eventBus.dispatch("find", { source: window, type: "", query: field.value, phraseSearch: true, highlightAll: true, findPrevious: false });
});
$("#search-field").addEventListener("keydown", (event) => {
  if (event.key === "Enter") $("#search-button").click();
  if (event.key === "Escape") closeSearch();
});
$("#rotate-left").addEventListener("click", () => command({ kind: "rotate", page_ids: [...state.selected], delta: -90 }));
$("#rotate-right").addEventListener("click", () => command({ kind: "rotate", page_ids: [...state.selected], delta: 90 }));
$("#delete-pages").addEventListener("click", async () => {
  if (await confirmDelete()) await command({ kind: "delete", page_ids: [...state.selected] });
});
$("#extract-pages").addEventListener("click", extractSelected);
$("#refresh-documents").addEventListener("click", refreshDocuments);
$("#merge-files").addEventListener("change", (event) => importMergeFiles([...event.target.files]));
$("#merge-add-document").addEventListener("click", addSelectedMergeDocument);
$("#merge-documents").addEventListener("click", mergeDocuments);
function setTablePanel(open) {
  $("#etl-panel").hidden = !open;
  $(".workspace-shell").classList.toggle("etl-open", open);
  $("#tables-button").setAttribute("aria-expanded", String(open));
}
$("#tables-button").addEventListener("click", () => setTablePanel($("#etl-panel").hidden));
$("#close-etl-panel").addEventListener("click", () => setTablePanel(false));
$("#detect-tables").addEventListener("click", detectCurrentTables);
$("#table-slice-mode").addEventListener("change", updateSliceFields);
all("[data-table-export]").forEach((button) => button.addEventListener(
  "click", () => exportSelectedTables(button.dataset.tableExport),
));
$("#batch-export").addEventListener("click", exportBatchTables);
$("#theme-button").addEventListener("click", () => {
  const root = document.documentElement;
  setTheme(root.dataset.theme === "dark" ? "light" : "dark");
});
all(".tab").forEach((tab) => tab.addEventListener("click", () => {
  all(".tab").forEach((item) => {
    const active = item === tab;
    item.classList.toggle("active", active);
    item.setAttribute("aria-selected", String(active));
  });
  all(".panel-body").forEach((panel) => {
    const active = panel.id === tab.dataset.panel;
    panel.classList.toggle("active", active);
    panel.hidden = !active;
  });
}));
window.addEventListener("keydown", (event) => {
  if (!(event.ctrlKey || event.metaKey)) return;
  if (event.key.toLowerCase() === "s") {
    event.preventDefault();
    if (event.shiftKey) downloadCurrent();
    else if (state.session?.dirty) save(false);
    else status("No unsaved page changes");
  }
  if (event.key.toLowerCase() === "z") { event.preventDefault(); runRevisionAction(event.shiftKey ? "redo" : "undo"); }
});

function setTheme(theme) {
  const value = theme === "light" ? "light" : "dark";
  document.documentElement.dataset.theme = value;
  const label = value === "dark" ? "Use light theme" : "Use dark theme";
  $("#theme-button").setAttribute("aria-label", label);
  $("#theme-button").dataset.tooltip = label;
  try { localStorage.setItem("docuforge-theme", value); } catch (_) { /* local preference is optional */ }
}

$(".toast-close").addEventListener("click", () => { $("#toast").hidden = true; });
try { setTheme(localStorage.getItem("docuforge-theme") || "dark"); } catch (_) { setTheme("dark"); }

updateSliceFields();
updateTableControls();
refreshDocuments();
status("Ready · local workspace");
