const state = {
  files: [],
  rows: [],
  columns: [],
  selectedFile: null,
  sortColumn: null,
  sortDirection: 'asc',
  searchTerm: '',
  currentPage: 1,
  pageSize: 100,
  columnWidths: {},
  selectedRow: null,
};

const elements = {
  fileSelect: document.getElementById('fileSelect'),
  rowCount: document.getElementById('rowCount'),
  pageSizeSelect: document.getElementById('pageSizeSelect'),
  searchInput: document.getElementById('searchInput'),
  dataTable: document.getElementById('dataTable'),
  tableHead: document.querySelector('#dataTable thead'),
  tableBody: document.querySelector('#dataTable tbody'),
  tableStatus: document.getElementById('tableStatus'),
  prevPageButton: document.getElementById('prevPageButton'),
  nextPageButton: document.getElementById('nextPageButton'),
  pageInfo: document.getElementById('pageInfo'),
  rowModal: document.getElementById('rowModal'),
  rowModalMeta: document.getElementById('rowModalMeta'),
  rowDetailGrid: document.getElementById('rowDetailGrid'),
  closeRowModalButton: document.getElementById('closeRowModalButton'),
  tabButtons: [...document.querySelectorAll('.tab-button')],
  tableTab: document.getElementById('tableTab'),
  readmeTab: document.getElementById('readmeTab'),
  readmeContent: document.getElementById('readmeContent'),
  themeToggle: document.getElementById('themeToggle'),
};

async function fetchJson(url) {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Request failed: ${response.status}`);
  }
  return response.json();
}

function formatCell(value) {
  if (value === null || value === undefined || value === '') return '—';
  return String(value);
}

function compareValues(left, right) {
  const leftNumber = Number(left);
  const rightNumber = Number(right);
  const leftNumeric = Number.isFinite(leftNumber);
  const rightNumeric = Number.isFinite(rightNumber);

  if (leftNumeric && rightNumeric) return leftNumber - rightNumber;
  return String(left ?? '').localeCompare(String(right ?? ''), undefined, { numeric: true, sensitivity: 'base' });
}

function getFilteredRows() {
  const term = state.searchTerm.trim().toLowerCase();
  let rows = state.rows.slice();

  if (term) {
    rows = rows.filter((row) =>
      Object.values(row).some((value) => String(value ?? '').toLowerCase().includes(term))
    );
  }

  if (state.sortColumn) {
    rows.sort((a, b) => {
      const delta = compareValues(a[state.sortColumn], b[state.sortColumn]);
      return state.sortDirection === 'asc' ? delta : -delta;
    });
  }

  return rows;
}

function getPagedRows(rows) {
  const totalPages = Math.max(1, Math.ceil(rows.length / state.pageSize));
  state.currentPage = Math.min(state.currentPage, totalPages);
  const start = (state.currentPage - 1) * state.pageSize;
  return {
    rows: rows.slice(start, start + state.pageSize),
    totalPages,
  };
}

function ensureColumnGroup() {
  let colgroup = elements.dataTable.querySelector('colgroup');
  if (!colgroup) {
    colgroup = document.createElement('colgroup');
    elements.dataTable.insertBefore(colgroup, elements.tableHead);
  }
  colgroup.innerHTML = '';
  state.columns.forEach((column) => {
    const col = document.createElement('col');
    const width = state.columnWidths[column.name];
    if (width) {
      col.style.width = `${width}px`;
    }
    colgroup.appendChild(col);
  });
}

function startColumnResize(event, columnName) {
  event.preventDefault();
  event.stopPropagation();
  const th = event.target.closest('th');
  const startX = event.clientX;
  const startWidth = th.getBoundingClientRect().width;

  const onMove = (moveEvent) => {
    const nextWidth = Math.max(96, startWidth + (moveEvent.clientX - startX));
    state.columnWidths[columnName] = nextWidth;
    ensureColumnGroup();
  };

  const onUp = () => {
    window.removeEventListener('pointermove', onMove);
    window.removeEventListener('pointerup', onUp);
    document.body.classList.remove('is-resizing');
  };

  document.body.classList.add('is-resizing');
  window.addEventListener('pointermove', onMove);
  window.addEventListener('pointerup', onUp);
}

function renderTable() {
  const filteredRows = getFilteredRows();
  const { rows, totalPages } = getPagedRows(filteredRows);
  elements.rowCount.textContent = filteredRows.length.toLocaleString();

  ensureColumnGroup();
  elements.tableHead.innerHTML = '';
  const headerRow = document.createElement('tr');
  state.columns.forEach((column) => {
    const th = document.createElement('th');
    const headerInner = document.createElement('div');
    headerInner.className = 'header-cell';
    const button = document.createElement('button');
    const sortMark = state.sortColumn === column.name ? (state.sortDirection === 'asc' ? ' ↑' : ' ↓') : '';
    button.innerHTML = `<span class="tooltip-label" title="${escapeHtml(column.description)}">${escapeHtml(column.name)}</span>${sortMark}`;
    button.addEventListener('click', () => {
      if (state.sortColumn === column.name) {
        state.sortDirection = state.sortDirection === 'asc' ? 'desc' : 'asc';
      } else {
        state.sortColumn = column.name;
        state.sortDirection = 'asc';
      }
      renderTable();
    });
    const resizer = document.createElement('span');
    resizer.className = 'column-resizer';
    resizer.title = `Resize ${column.name}`;
    resizer.addEventListener('pointerdown', (event) => startColumnResize(event, column.name));
    headerInner.appendChild(button);
    headerInner.appendChild(resizer);
    th.appendChild(headerInner);
    headerRow.appendChild(th);
  });
  elements.tableHead.appendChild(headerRow);

  elements.tableBody.innerHTML = '';
  rows.forEach((row) => {
    const tr = document.createElement('tr');
    tr.className = 'data-row';
    tr.tabIndex = 0;
    tr.addEventListener('click', () => openRowModal(row));
    tr.addEventListener('keydown', (event) => {
      if (event.key === 'Enter' || event.key === ' ') {
        event.preventDefault();
        openRowModal(row);
      }
    });
    state.columns.forEach((column) => {
      const td = document.createElement('td');
      td.textContent = formatCell(row[column.name]);
      tr.appendChild(td);
    });
    elements.tableBody.appendChild(tr);
  });

  const pageStart = filteredRows.length === 0 ? 0 : ((state.currentPage - 1) * state.pageSize) + 1;
  const pageEnd = Math.min(state.currentPage * state.pageSize, filteredRows.length);
  elements.tableStatus.textContent =
    `${state.selectedFile} · showing ${pageStart.toLocaleString()}-${pageEnd.toLocaleString()} of ${filteredRows.length.toLocaleString()} filtered rows`;
  elements.pageInfo.textContent = `Page ${state.currentPage} of ${totalPages}`;
  elements.prevPageButton.disabled = state.currentPage <= 1;
  elements.nextPageButton.disabled = state.currentPage >= totalPages;
}

function openRowModal(row) {
  state.selectedRow = row;
  const identityParts = [
    row.underlying_symbol,
    row.option_type,
    row.expiration_date,
    row.strike !== undefined && row.strike !== null ? `strike ${row.strike}` : null,
  ].filter(Boolean);
  elements.rowModalMeta.textContent = identityParts.join(' · ');
  elements.rowDetailGrid.innerHTML = '';

  state.columns.forEach((column) => {
    const item = document.createElement('article');
    item.className = 'row-detail-item';

    const label = document.createElement('div');
    label.className = 'row-detail-label';
    label.textContent = column.name;
    label.title = column.description;

    const value = document.createElement('div');
    value.className = 'row-detail-value';
    value.textContent = formatCell(row[column.name]);

    const description = document.createElement('div');
    description.className = 'row-detail-description';
    description.textContent = column.description;

    item.appendChild(label);
    item.appendChild(value);
    item.appendChild(description);
    elements.rowDetailGrid.appendChild(item);
  });

  elements.rowModal.classList.add('open');
  elements.rowModal.setAttribute('aria-hidden', 'false');
  document.body.classList.add('modal-open');
}

function closeRowModal() {
  state.selectedRow = null;
  elements.rowModal.classList.remove('open');
  elements.rowModal.setAttribute('aria-hidden', 'true');
  document.body.classList.remove('modal-open');
}

function escapeHtml(text) {
  return String(text)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;');
}

function renderMarkdown(markdown) {
  const lines = markdown.split('\n');
  let html = '';
  let inList = false;
  let inCode = false;

  const closeList = () => {
    if (inList) {
      html += '</ul>';
      inList = false;
    }
  };

  for (const line of lines) {
    if (line.startsWith('```')) {
      closeList();
      html += inCode ? '</code></pre>' : '<pre><code>';
      inCode = !inCode;
      continue;
    }

    if (inCode) {
      html += `${escapeHtml(line)}\n`;
      continue;
    }

    if (!line.trim()) {
      closeList();
      continue;
    }

    if (line.startsWith('### ')) {
      closeList();
      html += `<h3>${inlineMarkdown(line.slice(4))}</h3>`;
      continue;
    }
    if (line.startsWith('## ')) {
      closeList();
      html += `<h2>${inlineMarkdown(line.slice(3))}</h2>`;
      continue;
    }
    if (line.startsWith('# ')) {
      closeList();
      html += `<h1>${inlineMarkdown(line.slice(2))}</h1>`;
      continue;
    }
    if (line.startsWith('- ')) {
      if (!inList) {
        html += '<ul>';
        inList = true;
      }
      html += `<li>${inlineMarkdown(line.slice(2))}</li>`;
      continue;
    }

    closeList();
    html += `<p>${inlineMarkdown(line)}</p>`;
  }

  closeList();
  if (inCode) html += '</code></pre>';
  return html;
}

function inlineMarkdown(text) {
  return escapeHtml(text)
    .replace(/`([^`]+)`/g, '<code>$1</code>');
}

async function loadFiles() {
  const payload = await fetchJson('/api/files');
  state.files = payload.files;
  elements.fileSelect.innerHTML = '';

  state.files.forEach((file) => {
    const option = document.createElement('option');
    option.value = file.name;
    option.textContent = file.name;
    elements.fileSelect.appendChild(option);
  });
}

async function loadData(fileName) {
  const payload = await fetchJson(`/api/data?file=${encodeURIComponent(fileName)}`);
  state.selectedFile = payload.selected_file;
  state.rows = payload.rows;
  state.columns = payload.columns;
  state.currentPage = 1;
  state.columnWidths = {};
  elements.fileSelect.value = state.selectedFile;
  renderTable();
}

async function loadReadme() {
  const payload = await fetchJson('/api/readme');
  elements.readmeContent.innerHTML = renderMarkdown(payload.markdown);
}

function activateTab(tabName) {
  elements.tabButtons.forEach((button) => {
    button.classList.toggle('active', button.dataset.tab === tabName);
  });
  elements.tableTab.classList.toggle('active', tabName === 'table');
  elements.readmeTab.classList.toggle('active', tabName === 'readme');
}

function setTheme(theme) {
  document.body.dataset.theme = theme;
  localStorage.setItem('options-fetcher-theme', theme);
}

function initializeTheme() {
  const savedTheme = localStorage.getItem('options-fetcher-theme');
  setTheme(savedTheme || 'light');
}

async function initialize() {
  initializeTheme();
  await Promise.all([loadFiles(), loadReadme()]);
  if (state.files.length > 0) {
    await loadData(state.files[0].name);
  } else {
    elements.tableStatus.textContent = 'No CSV files found in the project root.';
  }

  elements.fileSelect.addEventListener('change', async (event) => {
    await loadData(event.target.value);
  });

  elements.searchInput.addEventListener('input', (event) => {
    state.searchTerm = event.target.value;
    state.currentPage = 1;
    renderTable();
  });

  elements.pageSizeSelect.addEventListener('change', (event) => {
    state.pageSize = Number(event.target.value);
    state.currentPage = 1;
    renderTable();
  });

  elements.prevPageButton.addEventListener('click', () => {
    if (state.currentPage > 1) {
      state.currentPage -= 1;
      renderTable();
    }
  });

  elements.nextPageButton.addEventListener('click', () => {
    state.currentPage += 1;
    renderTable();
  });

  elements.closeRowModalButton.addEventListener('click', closeRowModal);
  elements.rowModal.addEventListener('click', (event) => {
    if (event.target.dataset.closeModal === 'true') {
      closeRowModal();
    }
  });
  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape' && state.selectedRow) {
      closeRowModal();
    }
  });

  elements.tabButtons.forEach((button) => {
    button.addEventListener('click', () => activateTab(button.dataset.tab));
  });

  elements.themeToggle.addEventListener('click', () => {
    setTheme(document.body.dataset.theme === 'dark' ? 'light' : 'dark');
  });
}

initialize().catch((error) => {
  elements.tableStatus.textContent = error.message;
});
