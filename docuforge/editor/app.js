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
  $("#version-label").textContent = `Version ${state.version.version_number} · rev ${session.revision}`;
  const downloadLabel = $("#save-as-button .button-label");
  downloadLabel.textContent = session.dirty ? "Save & download" : "Download PDF";
  $("#save-as-button").title = session.dirty
    ? "Save a new immutable version, then download it"
    : "Download the current saved version";
  const enabled = Boolean(state.pdf);
  all("#save-button, #save-as-button, #undo-button, #redo-button, #previous-page, #next-page, #page-number, #zoom-out, #zoom-in, #zoom-select, #search-field, #search-button").forEach((element) => { element.disabled = !enabled; });
  $("#save-button").disabled = !session.dirty;
  $("#undo-button").disabled = !session.history.length;
  $("#redo-button").disabled = !session.redo.length;
  renderThumbnails();
  updateSelectionControls();
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
    item.setAttribute("aria-label", `Page ${index + 1}, source page ${page.source_index + 1}, rotation ${page.rotation} degrees`);
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
      viewer.currentPageNumber = index + 1;
      updateProperties(index);
    });
    item.addEventListener("keydown", (event) => {
      if (event.key !== "Enter" && event.key !== " ") return;
      event.preventDefault();
      viewer.currentPageNumber = index + 1;
      updateProperties(index);
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
  updateProperties(0);
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
  const count = $("#selection-count");
  count.textContent = selected ? `${state.selected.size} selected` : "No selection";
  count.classList.toggle("active", selected);
}

function markCurrentThumbnail(pageNumber) {
  all(".thumbnail").forEach((element, index) => {
    const current = index + 1 === pageNumber;
    element.classList.toggle("current", current);
    if (current) element.setAttribute("aria-current", "page");
    else element.removeAttribute("aria-current");
  });
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
  } catch (error) { toast(error.message, "error"); }
}

eventBus.on("pagesinit", () => { viewer.currentScaleValue = "page-width"; });
eventBus.on("pagechanging", ({ pageNumber }) => {
  $("#page-number").value = pageNumber;
  markCurrentThumbnail(pageNumber);
  if (pageNumber >= 1 && pageNumber <= (state.session?.pages.length || 0)) {
    updateProperties(pageNumber - 1);
  }
});
eventBus.on("pagesloaded", ({ pagesCount }) => { $("#page-count").textContent = state.session?.pages.length ?? pagesCount; });

$("#open-file").addEventListener("change", (event) => openFile(event.target.files[0]));
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
$("#search-button").addEventListener("click", () => eventBus.dispatch("find", { source: window, type: "", query: $("#search-field").value, phraseSearch: true, highlightAll: true, findPrevious: false }));
$("#search-field").addEventListener("keydown", (event) => { if (event.key === "Enter") $("#search-button").click(); });
$("#rotate-left").addEventListener("click", () => command({ kind: "rotate", page_ids: [...state.selected], delta: -90 }));
$("#rotate-right").addEventListener("click", () => command({ kind: "rotate", page_ids: [...state.selected], delta: 90 }));
$("#delete-pages").addEventListener("click", async () => {
  if (await confirmDelete()) await command({ kind: "delete", page_ids: [...state.selected] });
});
$("#extract-pages").addEventListener("click", extractSelected);
$("#refresh-documents").addEventListener("click", refreshDocuments);
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
  $("#theme-button").setAttribute("aria-label", value === "dark" ? "Use light theme" : "Use dark theme");
  try { localStorage.setItem("docuforge-theme", value); } catch (_) { /* local preference is optional */ }
}

$(".toast-close").addEventListener("click", () => { $("#toast").hidden = true; });
try { setTheme(localStorage.getItem("docuforge-theme") || "dark"); } catch (_) { setTheme("dark"); }

refreshDocuments();
status("Ready · local workspace");
