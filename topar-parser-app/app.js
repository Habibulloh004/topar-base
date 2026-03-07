const state = {
  run: null,
  parsedFields: [],
  sample: [],
  schema: { eksmo: [], main: [] },
  mappingRules: {},
  latestMapping: null,
};

const refs = {
  parseForm: document.getElementById("parse-form"),
  clearBtn: document.getElementById("clear-btn"),
  parseStatus: document.getElementById("parse-status"),
  parsedSchema: document.getElementById("parsed-schema"),
  parsedCount: document.getElementById("parsed-count"),
  mappingTable: document.getElementById("mapping-table"),
  mappingName: document.getElementById("mapping-name"),
  saveMapping: document.getElementById("save-mapping"),
  syncBtn: document.getElementById("sync-btn"),
  syncStatus: document.getElementById("sync-status"),
  sampleWrap: document.getElementById("sample-table-wrap"),
  sampleCount: document.getElementById("sample-count"),
};

const aliases = {
  barcode: ["barcode", "gtin", "gtin13", "gtin14", "sku", "isbn"],
  nomcode: ["nomcode", "barcode", "gtin", "sku"],
  isbn: ["isbn", "gtin", "gtin13"],
  name: ["name", "title"],
  annotation: ["annotation", "description"],
  coverUrl: ["cover", "image", "coverurl"],
  price: ["price", "offers.price"],
};

const MAX_TABLE_CELL_SYMBOLS = 100;
const STORAGE_KEY = "topar_parser_app_state_v1";
let persistTimer = null;

init().catch((err) => {
  setStatus(refs.parseStatus, `Ошибка инициализации: ${err.message}`, true);
});

async function init() {
  assertRequiredRefs();
  restorePersistentState();

  const initTasks = [loadSchema()];
  if (!hasMappingRules(state.mappingRules)) {
    initTasks.push(loadLatestMapping());
  }
  await Promise.all(initTasks);

  if (state.run?.id) {
    setStatus(refs.parseStatus, "Восстановление последнего запуска...");
    await restoreRunFromBackend(state.run.id);
  }

  renderParsedSchema();
  renderMappingTable();
  renderSampleTable();
  bindEvents();
  persistState();
}

function assertRequiredRefs() {
  const required = [
    ["parseForm", refs.parseForm],
    ["parseStatus", refs.parseStatus],
    ["mappingTable", refs.mappingTable],
    ["mappingName", refs.mappingName],
    ["saveMapping", refs.saveMapping],
    ["syncBtn", refs.syncBtn],
    ["syncStatus", refs.syncStatus],
    ["sampleWrap", refs.sampleWrap],
    ["sampleCount", refs.sampleCount],
  ];

  const missing = required.filter(([, el]) => !el).map(([name]) => name);
  if (missing.length) {
    throw new Error(`DOM элементы не найдены: ${missing.join(", ")}`);
  }
}

function bindEvents() {
  refs.parseForm?.addEventListener("submit", onParseSubmit);
  if (refs.clearBtn) {
    refs.clearBtn.addEventListener("click", onClearParsed);
  }
  refs.saveMapping?.addEventListener("click", onSaveMapping);
  refs.syncBtn?.addEventListener("click", onSync);
  refs.mappingName?.addEventListener("input", schedulePersist);
}

async function loadSchema() {
  const response = await api("/parser-app/schema");
  state.schema = response.target || { eksmo: [], main: [] };
}

async function loadLatestMapping() {
  const response = await api("/parser-app/mappings?limit=1");
  const latest = Array.isArray(response.items) ? response.items[0] : null;
  state.latestMapping = latest || null;
  if (latest?.rules) {
    state.mappingRules = structuredClone(latest.rules);
    refs.mappingName.value = latest.name || "";
  }
}

async function onParseSubmit(event) {
  event.preventDefault();
  setStatus(refs.parseStatus, "Парсинг запущен...");

  const formData = new FormData(refs.parseForm);
  const rawLimit = String(formData.get("limit") || "").trim();
  const parsedLimit = Number(rawLimit);
  const payload = {
    sourceUrl: String(formData.get("sourceUrl") || "").trim(),
    limit: rawLimit === "" ? 0 : parsedLimit,
    workers: Number(formData.get("workers") || 1),
    requestsPerSec: Number(formData.get("requestsPerSec") || 3),
  };

  if (!payload.sourceUrl) {
    setStatus(refs.parseStatus, "Необходимо указать URL источника", true);
    return;
  }
  if (!Number.isFinite(payload.limit) || payload.limit < 0) {
    setStatus(refs.parseStatus, "Лимит должен быть пустым или положительным числом.", true);
    return;
  }
  if (!Number.isFinite(payload.requestsPerSec) || payload.requestsPerSec < 1 || payload.requestsPerSec > 20) {
    setStatus(refs.parseStatus, "Запросов/сек должно быть в диапазоне 1.0-20.0.", true);
    return;
  }

  try {
    const response = await api("/parser-app/parse", {
      method: "POST",
      body: JSON.stringify(payload),
    });

    state.run = response.run;
    state.sample = Array.isArray(response.sample) ? response.sample : [];
    state.parsedFields = Array.isArray(response.run?.detectedFields) ? response.run.detectedFields : inferFieldsFromSample(state.sample);

    autoFillMappings();
    renderParsedSchema();
    renderMappingTable();
    renderSampleTable();
    persistState();

    setStatus(
      refs.parseStatus,
      `Запуск ${state.run.id}: распарсено ${state.run.parsedProducts} товаров из ${state.run.discoveredUrls} URL. Повторы из-за лимитов: ${state.run.rateLimitRetries}.`
    );
  } catch (err) {
    setStatus(refs.parseStatus, err.message, true);
  }
}

function autoFillMappings() {
  const allTargets = collectTargetKeys();
  const normalizedSource = new Map(state.parsedFields.map((f) => [normalizeKey(f), f]));

  for (const target of allTargets) {
    if (state.mappingRules[target]?.source || state.mappingRules[target]?.constant) {
      continue;
    }

    const key = target.split(".")[1] || "";
    const options = aliases[key] || [key];
    let match = "";

    for (const candidate of options) {
      const normalized = normalizeKey(candidate);
      if (normalizedSource.has(normalized)) {
        match = normalizedSource.get(normalized);
        break;
      }
      for (const sourceField of state.parsedFields) {
        if (normalizeKey(sourceField).endsWith(normalized)) {
          match = sourceField;
          break;
        }
      }
      if (match) break;
    }

    if (match) {
      state.mappingRules[target] = { source: match, constant: "" };
    }
  }
}

function renderParsedSchema() {
  if (!refs.parsedCount || !refs.parsedSchema) {
    return;
  }
  refs.parsedCount.textContent = `${state.parsedFields.length} полей`;
  refs.parsedSchema.innerHTML = "";

  if (!state.parsedFields.length) {
    refs.parsedSchema.innerHTML = "<li>Пока нет распарсенных полей.</li>";
    return;
  }

  for (const field of state.parsedFields) {
    const li = document.createElement("li");
    li.textContent = field;
    refs.parsedSchema.appendChild(li);
  }
}

function renderMappingTable() {
  refs.mappingTable.innerHTML = "";
  const groups = getTargetGroups();
  const sourceUsage = collectSourceUsage();
  const columns = document.createElement("div");
  columns.className = "mapping-columns";

  for (const group of groups) {
    const column = document.createElement("section");
    column.className = "mapping-column";

    const sectionTitle = document.createElement("div");
    sectionTitle.className = "mapping-section-title";
    sectionTitle.textContent = group.name;
    column.appendChild(sectionTitle);

    for (const item of group.fields || []) {
      const target = `${group.prefix}.${item.key}`;
      const row = document.createElement("div");
      row.className = "mapping-row";

      const targetLabel = document.createElement("div");
      targetLabel.className = "target";
      targetLabel.textContent = item.key;

      const sourceSelect = document.createElement("select");
      sourceSelect.dataset.target = target;
      sourceSelect.innerHTML = `<option value="">(не сопоставлено) [пусто]</option>`;
      const existing = state.mappingRules[target] || { source: "", constant: "" };
      for (const field of state.parsedFields) {
        const option = document.createElement("option");
        option.value = field;
        const usedCount = sourceUsage.get(field) || 0;
        if (existing.source === field) {
          option.textContent = usedCount > 1 ? `${field} [выбрано x${usedCount}]` : `${field} [выбрано]`;
        } else if (usedCount > 0) {
          option.textContent = `${field} [используется x${usedCount}]`;
        } else {
          option.textContent = `${field} [свободно]`;
        }
        sourceSelect.appendChild(option);
      }

      const constantInput = document.createElement("input");
      constantInput.type = "text";
      constantInput.placeholder = "константа (необязательно)";
      constantInput.dataset.target = target;

      sourceSelect.value = existing.source || "";
      constantInput.value = existing.constant || "";

      sourceSelect.addEventListener("change", () => {
        const source = sourceSelect.value || "";
        const constant = String(state.mappingRules[target]?.constant || "").trim();
        if (!source && !constant) {
          delete state.mappingRules[target];
        } else {
          ensureRule(target);
          state.mappingRules[target].source = source;
        }
        renderMappingTable();
        schedulePersist();
      });

      constantInput.addEventListener("input", () => {
        const source = String(state.mappingRules[target]?.source || "").trim();
        const constant = constantInput.value;
        if (!source && !String(constant).trim()) {
          delete state.mappingRules[target];
          schedulePersist();
          return;
        }
        ensureRule(target);
        state.mappingRules[target].constant = constant;
        schedulePersist();
      });

      const controls = document.createElement("div");
      controls.className = "mapping-controls";
      controls.appendChild(sourceSelect);
      controls.appendChild(constantInput);

      row.appendChild(targetLabel);
      row.appendChild(controls);
      column.appendChild(row);
    }

    columns.appendChild(column);
  }
  refs.mappingTable.appendChild(columns);
}

function onClearParsed() {
  if (persistTimer) {
    clearTimeout(persistTimer);
    persistTimer = null;
  }

  state.run = null;
  state.sample = [];
  state.parsedFields = [];
  state.mappingRules = {};
  state.latestMapping = null;
  refs.mappingName.value = "";

  renderParsedSchema();
  renderMappingTable();
  renderSampleTable();
  clearPersistentState();
  setStatus(refs.parseStatus, "Результаты парсинга очищены.");
  setStatus(refs.syncStatus, "");
}

function ensureRule(target) {
  if (!state.mappingRules[target]) {
    state.mappingRules[target] = { source: "", constant: "" };
  }
}

function renderSampleTable() {
  refs.sampleCount.textContent = `${state.sample.length} записей`;
  refs.sampleWrap.innerHTML = "";

  if (!state.sample.length) {
    refs.sampleWrap.innerHTML = '<div class="status">Пока нет распарсенных записей.</div>';
    return;
  }

  const fields = state.parsedFields.length ? [...state.parsedFields] : inferFieldsFromSample(state.sample);
  const table = document.createElement("table");
  const head = document.createElement("thead");
  const body = document.createElement("tbody");

  head.innerHTML = `<tr>${fields.map((f) => `<th>${escapeHTML(f)}</th>`).join("")}</tr>`;
  for (const record of state.sample) {
    const row = document.createElement("tr");
    for (const field of fields) {
      const fullValue = toDisplay(record.data?.[field] ?? (field === "source_url" ? record.sourceUrl || record.source_url : null));
      const { text, truncated } = truncateText(fullValue, MAX_TABLE_CELL_SYMBOLS);
      const cell = document.createElement("td");
      cell.textContent = text;
      if (truncated) {
        cell.title = fullValue;
      }
      row.appendChild(cell);
    }
    body.appendChild(row);
  }

  table.appendChild(head);
  table.appendChild(body);
  refs.sampleWrap.appendChild(table);
}

async function onSaveMapping() {
  const rules = collectMappingRules();
  if (!Object.keys(rules).length) {
    setStatus(refs.syncStatus, "Нет правил сопоставления для сохранения.", true);
    return;
  }

  try {
    const response = await api("/parser-app/mappings", {
      method: "POST",
      body: JSON.stringify({
        name: refs.mappingName.value.trim(),
        rules,
      }),
    });
    persistState();
    setStatus(refs.syncStatus, `Маппинг сохранен: ${response.name} (${response.id}).`);
  } catch (err) {
    setStatus(refs.syncStatus, err.message, true);
  }
}

async function onSync() {
  if (!state.run?.id) {
    setStatus(refs.syncStatus, "Сначала запустите парсинг.", true);
    return;
  }

  const rules = collectMappingRules();
  if (!Object.keys(rules).length) {
    setStatus(refs.syncStatus, "Укажите хотя бы одно правило сопоставления.", true);
    return;
  }

  const payload = {
    rules,
    saveMapping: !!refs.mappingName.value.trim(),
    mappingName: refs.mappingName.value.trim(),
    syncEksmo: true,
    syncMain: true,
  };

  try {
    setStatus(refs.syncStatus, "Синхронизация запущена...");
    const response = await api(`/parser-app/runs/${state.run.id}/sync`, {
      method: "POST",
      body: JSON.stringify(payload),
    });

    const lines = [
      `Обработано: ${response.totalRecords}`,
      `Все -> добавлено/обновлено ${response.eksmoUpserted}, изменено ${response.eksmoModified}, пропущено ${response.eksmoSkipped}`,
      `Основные -> добавлено ${response.mainInserted}, изменено ${response.mainModified}, пропущено ${response.mainSkipped}`,
    ];
    if (response.mappingProfile) {
      lines.push(`Маппинг: ${response.mappingProfile}`);
    }

    setStatus(refs.syncStatus, lines.join("\n"));
    persistState();
  } catch (err) {
    setStatus(refs.syncStatus, err.message, true);
  }
}

function collectMappingRules() {
  const cleaned = {};
  for (const [target, rule] of Object.entries(state.mappingRules)) {
    const source = String(rule?.source || "").trim();
    const constant = String(rule?.constant || "").trim();
    if (!source && !constant) {
      continue;
    }
    cleaned[target] = { source, constant };
  }
  return cleaned;
}

function collectTargetKeys() {
  const keys = [];
  for (const group of getTargetGroups()) {
    for (const field of group.fields || []) {
      keys.push(`${group.prefix}.${field.key}`);
    }
  }
  return keys;
}

function hasMappingRules(rules) {
  if (!rules || typeof rules !== "object") {
    return false;
  }
  return Object.keys(collectMappingRules()).length > 0;
}

function clearSourceMappings() {
  const nextRules = {};
  for (const [target, rule] of Object.entries(state.mappingRules || {})) {
    const constant = String(rule?.constant || "").trim();
    if (!constant) {
      continue;
    }
    nextRules[target] = { source: "", constant };
  }
  state.mappingRules = nextRules;
}

function collectSourceUsage() {
  const usage = new Map();
  for (const rule of Object.values(state.mappingRules || {})) {
    const source = String(rule?.source || "").trim();
    if (!source) {
      continue;
    }
    usage.set(source, (usage.get(source) || 0) + 1);
  }
  return usage;
}

async function restoreRunFromBackend(runID) {
  try {
    const response = await api(`/parser-app/runs/${runID}?limit=20`);
    if (response?.run) {
      state.run = response.run;
    }
    state.sample = Array.isArray(response?.records) ? response.records : [];
    state.parsedFields = Array.isArray(state.run?.detectedFields) && state.run.detectedFields.length
      ? state.run.detectedFields
      : inferFieldsFromSample(state.sample);

    setStatus(
      refs.parseStatus,
      `Запуск ${state.run.id}: распарсено ${state.run.parsedProducts} товаров из ${state.run.discoveredUrls} URL.`
    );
    persistState();
  } catch (err) {
    state.run = null;
    state.sample = [];
    state.parsedFields = [];
    persistState();
    setStatus(refs.parseStatus, `Не удалось восстановить предыдущий запуск: ${err.message}`, true);
  }
}

function schedulePersist() {
  if (persistTimer) {
    clearTimeout(persistTimer);
  }
  persistTimer = setTimeout(() => {
    persistTimer = null;
    persistState();
  }, 160);
}

function persistState() {
  try {
    const payload = {
      run: state.run ? { id: state.run.id } : null,
      parsedFields: Array.isArray(state.parsedFields) ? state.parsedFields : [],
      mappingRules: state.mappingRules || {},
      mappingName: refs.mappingName?.value || "",
    };
    localStorage.setItem(STORAGE_KEY, JSON.stringify(payload));
  } catch (err) {
    console.warn("persist state failed", err);
  }
}

function clearPersistentState() {
  try {
    localStorage.removeItem(STORAGE_KEY);
  } catch (err) {
    console.warn("clear state failed", err);
  }
}

function restorePersistentState() {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) {
      return;
    }
    const payload = JSON.parse(raw);
    if (!payload || typeof payload !== "object") {
      return;
    }

    if (payload.run && typeof payload.run.id === "string" && payload.run.id.trim() !== "") {
      state.run = { id: payload.run.id.trim() };
    }
    if (Array.isArray(payload.parsedFields)) {
      state.parsedFields = payload.parsedFields.map((item) => String(item)).filter((item) => item.trim() !== "");
    }
    if (payload.mappingRules && typeof payload.mappingRules === "object") {
      state.mappingRules = payload.mappingRules;
    }
    if (typeof payload.mappingName === "string") {
      refs.mappingName.value = payload.mappingName;
    }
  } catch (err) {
    console.warn("restore state failed", err);
  }
}

function getTargetGroups() {
  return [
    { name: "Все товары", fields: state.schema.eksmo || [], prefix: "eksmo" },
    { name: "Основные товары", fields: state.schema.main || [], prefix: "main" },
  ];
}

function inferFieldsFromSample(sample) {
  const set = new Set();
  for (const row of sample || []) {
    for (const key of Object.keys(row.data || {})) {
      set.add(key);
    }
  }
  return [...set].sort();
}

function normalizeKey(key) {
  return String(key || "")
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "");
}

function toDisplay(value) {
  if (value == null) return "";
  if (typeof value === "object") {
    const json = JSON.stringify(value);
    return json == null ? String(value) : json;
  }
  return String(value);
}

function truncateText(value, maxLen = 100) {
  const text = String(value || "");
  if (maxLen < 1 || text.length <= maxLen) {
    return { text, truncated: false };
  }
  return { text: `${text.slice(0, maxLen)}...`, truncated: true };
}

function escapeHTML(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function setStatus(target, text, isError = false) {
  target.textContent = text || "";
  target.style.color = isError ? "#a32a2a" : "#4d4779";
}

async function api(path, options = {}) {
  const headers = {
    "Content-Type": "application/json",
    ...(options.headers || {}),
  };

  const response = await fetch(path, {
    ...options,
    headers,
  });

  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(payload.error || `Ошибка запроса (${response.status})`);
  }
  return payload;
}
