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
  loadingTask: null,
  selected: new Set(),
  thumbnailGeneration: 0,
  thumbnailQueue: [],
  thumbnailWorkers: 0,
  thumbnailCache: new Map(),
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
}
function toast(message) {
  const element = $("#toast");
  element.textContent = message;
  element.hidden = false;
  clearTimeout(toast.timer);
  toast.timer = setTimeout(() => { element.hidden = true; }, 5000);
}

async function api(path, options = {}) {
  const headers = new Headers(options.headers || {});
  if (options.body && !(options.body instanceof FormData)) headers.set("Content-Type", "application/json");
  const response = await fetch(path, { ...options, headers });
  if (!response.ok) {
    let detail = { message: `Request failed (${response.status})` };
    try { detail = await response.json(); } catch (_) { /* controlled fallback */ }
    throw new Error(detail.message || detail.detail || "Request failed");
  }
  const type = response.headers.get("content-type") || "";
  return type.includes("application/json") ? response.json() : response;
}

async function openFile(file) {
  if (!file) return;
  busy(true, "Validating and importing PDF…");
  try {
    const form = new FormData();
    form.append("file", file, file.name);
    form.append("title", file.name);
    const imported = await api(`/api/workspaces/${state.workspaceId}/documents`, { method: "POST", body: form });
    await openDocument(imported.document.id, imported.version.id);
    status(`Imported ${imported.pdf.page_count} pages`);
    await refreshDocuments();
  } catch (error) { toast(error.message); status("Import failed"); }
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
    await loadPdf(`/api/versions/${version.id}/content`);
    renderSession();
    status(`Opened ${document.title}`);
  } catch (error) { toast(error.message); status("Open failed"); }
  finally { busy(false); }
}

async function loadPdf(url) {
  state.thumbnailGeneration += 1;
  state.thumbnailQueue = [];
  state.thumbnailCache.clear();
  if (state.loadingTask) await state.loadingTask.destroy().catch(() => {});
  state.loadingTask = pdfjsLib.getDocument({ url, disableAutoFetch: true, rangeChunkSize: 262144 });
  state.pdf = await state.loadingTask.promise;
  viewer.setDocument(state.pdf);
  linkService.setDocument(state.pdf);
  $("#empty-state").hidden = true;
  $("#viewerContainer").hidden = false;
  await eventBus.dispatch("pagesinit", { source: window });
  viewer.currentScaleValue = "page-width";
}

function renderSession() {
  const session = state.session;
  $("#document-title").textContent = state.document.title;
  $("#dirty-marker").hidden = !session.dirty;
  $("#page-count").textContent = session.pages.length;
  $("#version-label").textContent = `Version ${state.version.version_number} · rev ${session.revision}`;
  const enabled = Boolean(state.pdf);
  all("#save-button, #save-as-button, #undo-button, #redo-button, #previous-page, #next-page, #page-number, #zoom-out, #zoom-in, #zoom-select, #search-field, #search-button").forEach((element) => { element.disabled = !enabled; });
  $("#save-button").disabled = !session.dirty;
  $("#undo-button").disabled = !session.history.length;
  $("#redo-button").disabled = !session.redo.length;
  renderThumbnails();
  updateSelectionControls();
}

function renderThumbnails() {
  const generation = ++state.thumbnailGeneration;
  state.thumbnailQueue = [];
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
    item.dataset.pageId = page.id;
    item.dataset.sourceIndex = String(page.source_index);
    item.dataset.position = String(index);
    item.innerHTML = `
      <input type="checkbox" aria-label="Select page ${index + 1}" ${state.selected.has(page.id) ? "checked" : ""}>
      <div><div class="thumbnail-placeholder">Page ${index + 1}</div>
      <div class="thumbnail-meta"><span>${index + 1}</span><span>${page.rotation}°</span></div></div>`;
    item.querySelector("input").addEventListener("change", (event) => {
      event.stopPropagation();
      event.target.checked ? state.selected.add(page.id) : state.selected.delete(page.id);
      item.classList.toggle("selected", event.target.checked);
      updateSelectionControls();
    });
    item.addEventListener("click", (event) => {
      if (event.target.matches("input")) return;
      viewer.currentPageNumber = page.source_index + 1;
      updateProperties(index);
    });
    item.addEventListener("dragstart", () => { item.classList.add("dragging"); item.dataset.dragging = "true"; });
    item.addEventListener("dragend", () => { item.classList.remove("dragging"); delete item.dataset.dragging; all(".drop-target").forEach((el) => el.classList.remove("drop-target")); });
    item.addEventListener("dragover", (event) => { event.preventDefault(); item.classList.add("drop-target"); });
    item.addEventListener("dragleave", () => item.classList.remove("drop-target"));
    item.addEventListener("drop", async (event) => {
      event.preventDefault();
      const source = $(".thumbnail[data-dragging='true']");
      if (!source || source === item) return;
      await command({ kind: "move", page_id: source.dataset.pageId, to_index: Number(item.dataset.position) });
    });
    list.append(item);
    observer.observe(item);
  });
  updateProperties(0);
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
  const viewport = page.getViewport({ scale: 0.23 });
  const canvas = document.createElement("canvas");
  canvas.width = Math.ceil(viewport.width);
  canvas.height = Math.ceil(viewport.height);
  const renderTask = page.render({ canvasContext: canvas.getContext("2d"), viewport });
  try { await renderTask.promise; } catch (error) { if (error?.name !== "RenderingCancelledException") throw error; }
  if (generation !== state.thumbnailGeneration || !element.isConnected) { renderTask.cancel(); return; }
  element.querySelector(".thumbnail-placeholder").replaceWith(canvas);
}

function updateSelectionControls() {
  const selected = state.selected.size > 0;
  all("#rotate-left, #rotate-right, #delete-pages, #extract-pages").forEach((element) => { element.disabled = !selected; });
  $("#delete-pages").disabled = !selected || state.selected.size >= (state.session?.pages.length || 0);
}

function updateProperties(index) {
  const page = state.session?.pages[index];
  $("#property-page").textContent = page ? String(index + 1) : "—";
  $("#property-source").textContent = page ? String(page.source_index + 1) : "—";
  $("#property-rotation").textContent = page ? `${page.rotation}°` : "—";
  $("#property-id").textContent = page ? page.id : "—";
}

async function command(payload) {
  if (!state.session) return;
  busy(true, "Updating page plan…");
  try {
    state.session = await api(`/api/sessions/${state.session.id}/commands`, {
      method: "POST",
      body: JSON.stringify({ expected_revision: state.session.revision, command: payload }),
    });
    for (const id of [...state.selected]) if (!state.session.pages.some((page) => page.id === id)) state.selected.delete(id);
    renderSession();
    status("Page plan updated; save to materialize the version");
  } catch (error) { toast(error.message); }
  finally { busy(false); }
}

async function runRevisionAction(action) {
  busy(true, `${action === "undo" ? "Undoing" : "Redoing"}…`);
  try {
    state.session = await api(`/api/sessions/${state.session.id}/${action}`, {
      method: "POST", body: JSON.stringify({ expected_revision: state.session.revision }),
    });
    renderSession();
    status(action === "undo" ? "Last page command undone" : "Page command restored");
  } catch (error) { toast(error.message); }
  finally { busy(false); }
}

async function save(downloadAfter = false) {
  if (!state.session) return;
  busy(true, "Validating and saving version…");
  try {
    const result = await api(`/api/sessions/${state.session.id}/save`, {
      method: "POST", body: JSON.stringify({ expected_revision: state.session.revision }),
    });
    state.version = result.version;
    state.session = result.session;
    await loadPdf(`/api/versions/${state.version.id}/content`);
    renderSession();
    status(`Saved immutable version ${state.version.version_number}`);
    if (downloadAfter) download(`/api/versions/${state.version.id}/content`, state.document.title);
  } catch (error) { toast(error.message); status("Save blocked"); }
  finally { busy(false); }
}

async function extractSelected() {
  busy(true, "Extracting selected pages…");
  try {
    const result = await api(`/api/sessions/${state.session.id}/extract`, {
      method: "POST",
      body: JSON.stringify({ expected_revision: state.session.revision, page_ids: [...state.selected] }),
    });
    download(`/api/artifacts/${result.artifact.id}/content`, "extracted-pages.pdf");
    status("Extracted pages without changing the document");
  } catch (error) { toast(error.message); }
  finally { busy(false); }
}

function download(url, filename) {
  const anchor = document.createElement("a");
  anchor.href = url; anchor.download = filename; document.body.append(anchor); anchor.click(); anchor.remove();
}

async function refreshDocuments() {
  try {
    const documents = await api(`/api/workspaces/${state.workspaceId}/documents`);
    const list = $("#document-list");
    list.replaceChildren();
    for (const document of documents) {
      const card = window.document.createElement("button");
      card.className = "document-card";
      card.innerHTML = `<strong></strong><small></small>`;
      card.querySelector("strong").textContent = document.title;
      card.querySelector("small").textContent = `${document.page_count} pages · latest version`;
      card.addEventListener("click", () => openDocument(document.id, document.latest_version_id));
      list.append(card);
    }
  } catch (error) { toast(error.message); }
}

eventBus.on("pagesinit", () => { viewer.currentScaleValue = "page-width"; });
eventBus.on("pagechanging", ({ pageNumber }) => {
  $("#page-number").value = pageNumber;
  const index = state.session?.pages.findIndex((page) => page.source_index === pageNumber - 1);
  if (index >= 0) updateProperties(index);
});
eventBus.on("pagesloaded", ({ pagesCount }) => { $("#page-count").textContent = state.session?.pages.length ?? pagesCount; });

$("#open-file").addEventListener("change", (event) => openFile(event.target.files[0]));
$("#save-button").addEventListener("click", () => save(false));
$("#save-as-button").addEventListener("click", () => state.session?.dirty ? save(true) : download(`/api/versions/${state.version.id}/content`, state.document.title));
$("#undo-button").addEventListener("click", () => runRevisionAction("undo"));
$("#redo-button").addEventListener("click", () => runRevisionAction("redo"));
$("#previous-page").addEventListener("click", () => viewer.previousPage());
$("#next-page").addEventListener("click", () => viewer.nextPage());
$("#page-number").addEventListener("change", (event) => { viewer.currentPageNumber = Number(event.target.value); });
$("#zoom-select").addEventListener("change", (event) => { viewer.currentScaleValue = event.target.value; });
$("#zoom-in").addEventListener("click", () => viewer.increaseScale({ steps: 1 }));
$("#zoom-out").addEventListener("click", () => viewer.decreaseScale({ steps: 1 }));
$("#search-button").addEventListener("click", () => eventBus.dispatch("find", { source: window, type: "", query: $("#search-field").value, phraseSearch: true, highlightAll: true, findPrevious: false }));
$("#search-field").addEventListener("keydown", (event) => { if (event.key === "Enter") $("#search-button").click(); });
$("#rotate-left").addEventListener("click", () => command({ kind: "rotate", page_ids: [...state.selected], delta: -90 }));
$("#rotate-right").addEventListener("click", () => command({ kind: "rotate", page_ids: [...state.selected], delta: 90 }));
$("#delete-pages").addEventListener("click", async () => command({ kind: "delete", page_ids: [...state.selected] }));
$("#extract-pages").addEventListener("click", extractSelected);
$("#refresh-documents").addEventListener("click", refreshDocuments);
$("#theme-button").addEventListener("click", () => {
  const root = document.documentElement;
  root.dataset.theme = root.dataset.theme === "dark" ? "light" : "dark";
});
all(".tab").forEach((tab) => tab.addEventListener("click", () => {
  all(".tab").forEach((item) => item.classList.toggle("active", item === tab));
  all(".panel-body").forEach((panel) => panel.classList.toggle("active", panel.id === tab.dataset.panel));
}));
window.addEventListener("keydown", (event) => {
  if (!(event.ctrlKey || event.metaKey)) return;
  if (event.key.toLowerCase() === "s") { event.preventDefault(); save(event.shiftKey); }
  if (event.key.toLowerCase() === "z") { event.preventDefault(); runRevisionAction(event.shiftKey ? "redo" : "undo"); }
});

refreshDocuments();
status("Ready · local workspace");
