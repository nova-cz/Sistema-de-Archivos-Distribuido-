// Animaciones GSAP
gsap.from("header", { duration: 0.8, y: -50, opacity: 0, ease: "power3.out" });
gsap.from(".actions", { duration: 0.6, y: 30, opacity: 0, delay: 0.3, ease: "power3.out" });
gsap.from(".panel", { duration: 0.6, y: 40, opacity: 0, stagger: 0.15, delay: 0.5, ease: "power3.out" });
gsap.from(".stat-card", { duration: 0.5, scale: 0.8, opacity: 0, stagger: 0.1, delay: 0.4, ease: "back.out(1.7)" });

// Animación de orbes
gsap.to(".orb-1", { duration: 15, x: 50, y: 30, repeat: -1, yoyo: true, ease: "sine.inOut" });
gsap.to(".orb-2", { duration: 12, x: -40, y: -20, repeat: -1, yoyo: true, ease: "sine.inOut" });
gsap.to(".orb-3", { duration: 10, x: 30, y: 40, repeat: -1, yoyo: true, ease: "sine.inOut" });

let selectedFile = null;
let files = [];
let nodeStatus = {};

document.addEventListener('DOMContentLoaded', () => {
    loadAll();
    setInterval(loadAll, 3000);

    document.getElementById('file-input').addEventListener('change', uploadFile);
    document.getElementById('btn-download').addEventListener('click', downloadFile);
    document.getElementById('btn-attributes').addEventListener('click', showAttributes);
    document.getElementById('btn-delete').addEventListener('click', deleteFile);
    document.getElementById('btn-table').addEventListener('click', showTable);
});

function loadAll() {
    fetch('/api/distributed_files').then(r => r.json()).then(d => {
        if (d.status === 'ok') { files = d.files || []; renderFiles(); document.getElementById('total-files').textContent = files.length; }
    });
    fetch('/api/status').then(r => r.json()).then(d => {
        nodeStatus = d;
        let on = 0;
        for (let n in d) {
            const el = document.getElementById(`status-${n}`);
            if (el) { el.className = `status-dot ${d[n] ? 'online' : 'offline'}`; if (d[n]) on++; }
        }
        document.getElementById('nodes-online').textContent = on;
    });
    fetch('/api/system_stats').then(r => r.json()).then(d => {
        if (d.status === 'ok') {
            document.getElementById('total-blocks').textContent = d.stats.total_blocks || 0;
            let total = 0;
            for (let n in d.stats.node_usage) {
                const u = d.stats.node_usage[n] || 0;
                const c = d.stats.node_capacity[n] || 50;
                const p = Math.min((u / c) * 100, 100);
                total += u;
                const bar = document.getElementById(`cap-${n}`);
                const txt = document.getElementById(`cap-text-${n}`);
                if (bar) { bar.style.width = `${p}%`; bar.className = 'capacity-fill' + (p > 80 ? ' danger' : p > 60 ? ' warning' : ''); }
                if (txt) txt.textContent = `${u} / ${c} MB`;
            }
            document.getElementById('total-space').textContent = total;
        }
    });
}

function renderFiles() {
    const list = document.getElementById('file-list');
    if (files.length === 0) {
        list.innerHTML = '<li class="empty-state"><div class="empty-icon">📂</div><div>No hay archivos</div><div style="font-size:0.9em;margin-top:8px;">Sube uno con el botón verde</div></li>';
        return;
    }
    list.innerHTML = files.map(f => `
        <li class="file-item ${selectedFile?.file_id === f.file_id ? 'selected' : ''}" onclick="selectFile('${f.file_id}')">
            <div class="file-info">
                <div class="file-icon">📄</div>
                <div>
                    <div class="file-name">${f.filename}</div>
                    <div class="file-meta">${formatSize(f.size)} • ${f.total_blocks} bloque(s)</div>
                </div>
            </div>
        </li>
    `).join('');

    // Animar nuevos items
    gsap.from(".file-item", { duration: 0.3, x: -20, opacity: 0, stagger: 0.05 });
}

function selectFile(id) {
    selectedFile = files.find(f => f.file_id === id);
    renderFiles();
    const has = !!selectedFile;
    document.getElementById('btn-download').disabled = !has;
    document.getElementById('btn-attributes').disabled = !has;
    document.getElementById('btn-delete').disabled = !has;
}

function uploadFile(e) {
    const file = e.target.files[0];
    if (!file) return;

    const form = new FormData();
    form.append('file', file);

    const overlay = document.getElementById('progress-overlay');
    const fill = document.getElementById('progress-fill');
    const text = document.getElementById('progress-text');

    overlay.style.display = 'flex';
    gsap.fromTo(overlay, { opacity: 0 }, { opacity: 1, duration: 0.3 });

    let prog = 0;
    const int = setInterval(() => { prog += Math.random() * 15; if (prog > 90) prog = 90; fill.style.width = `${prog}%`; }, 200);

    fetch('/api/upload', { method: 'POST', body: form })
        .then(r => r.json())
        .then(d => {
            clearInterval(int);
            fill.style.width = '100%';
            setTimeout(() => {
                gsap.to(overlay, { opacity: 0, duration: 0.3, onComplete: () => overlay.style.display = 'none' });
                if (d.status === 'ok') {
                    toast(`✅ "${d.filename}" subido (${d.total_blocks} bloques)`, 'success');
                    loadAll();
                } else {
                    toast(`❌ Error: ${d.message}`, 'error');
                }
            }, 500);
        })
        .catch(e => {
            clearInterval(int);
            overlay.style.display = 'none';
            toast('❌ Error al subir', 'error');
        });

    e.target.value = '';
}

function downloadFile() {
    if (!selectedFile) return;
    toast(`📥 Descargando ${selectedFile.filename}...`, 'info');
    const a = document.createElement('a');
    a.href = `/api/download/${selectedFile.file_id}`;
    a.download = selectedFile.filename;
    a.click();
}

function deleteFile() {
    if (!selectedFile) return;
    if (!confirm(`¿Eliminar "${selectedFile.filename}"?`)) return;

    fetch(`/api/delete_distributed/${selectedFile.file_id}`, { method: 'DELETE' })
        .then(r => r.json())
        .then(d => {
            if (d.status === 'ok') {
                toast('✅ Archivo eliminado', 'success');
                selectedFile = null;
                document.getElementById('btn-download').disabled = true;
                document.getElementById('btn-attributes').disabled = true;
                document.getElementById('btn-delete').disabled = true;
                loadAll();
            } else {
                toast(`❌ ${d.message}`, 'error');
            }
        });
}

function showAttributes() {
    if (!selectedFile) return;

    fetch(`/api/file_attributes/${selectedFile.file_id}`)
        .then(r => r.json())
        .then(d => {
            if (d.status === 'ok') {
                const a = d.attributes;
                document.getElementById('attr-grid').innerHTML = `
                    <div class="attr-card">
                        <div class="attr-label">Nombre</div>
                        <div class="attr-value">${a.original_filename}</div>
                    </div>
                    <div class="attr-card">
                        <div class="attr-label">Tamaño</div>
                        <div class="attr-value">${formatSize(a.size)}</div>
                    </div>
                    <div class="attr-card">
                        <div class="attr-label">Bloques</div>
                        <div class="attr-value">${a.total_blocks}</div>
                    </div>
                    <div class="attr-card">
                        <div class="attr-label">ID</div>
                        <div class="attr-value" style="font-size:0.8em;">${selectedFile.file_id}</div>
                    </div>
                    <div class="attr-card">
                        <div class="attr-label">Creado</div>
                        <div class="attr-value" style="font-size:0.9em;">${formatDate(a.created_at)}</div>
                    </div>
                    <div class="attr-card">
                        <div class="attr-label">Replicación</div>
                        <div class="attr-value">✅ Completa</div>
                    </div>
                `;

                document.getElementById('blocks-tbody').innerHTML = (a.blocks_detail || []).map(b => `
                    <tr>
                        <td>${b.block_num}</td>
                        <td>${formatSize(b.size)}</td>
                        <td><span class="badge badge-primary">${b.primary_node}</span></td>
                        <td><span class="badge badge-replica">${b.replica_node}</span></td>
                        <td><code style="color:var(--gray);font-size:0.8em;">${(b.hash || '').substring(0, 8)}...</code></td>
                    </tr>
                `).join('');

                openModal('modal-attr');
            }
        });
}

function showTable() {
    Promise.all([
        fetch('/api/block_table').then(r => r.json()),
        fetch('/api/system_stats').then(r => r.json())
    ]).then(([table, stats]) => {
        if (table.status === 'ok' && stats.status === 'ok') {
            const s = stats.stats;
            document.getElementById('stats-grid').innerHTML = `
                <div class="attr-card">
                    <div class="attr-label">Archivos</div>
                    <div class="attr-value">${s.total_files}</div>
                </div>
                <div class="attr-card">
                    <div class="attr-label">Bloques</div>
                    <div class="attr-value">${s.total_blocks}</div>
                </div>
                ${Object.keys(s.node_capacity).map(n => `
                    <div class="attr-card">
                        <div class="attr-label">${n}</div>
                        <div class="attr-value">
                            ${s.node_usage[n] || 0} / ${s.node_capacity[n]} MB
                            <span class="badge ${nodeStatus[n] ? 'badge-online' : 'badge-offline'}" style="margin-left:8px;font-size:0.75em;">
                                ${nodeStatus[n] ? 'Online' : 'Offline'}
                            </span>
                        </div>
                    </div>
                `).join('')}
            `;

            const blocks = Object.values(table.block_table.blocks || {});
            if (blocks.length === 0) {
                document.getElementById('table-tbody').innerHTML = '<tr><td colspan="6" style="text-align:center;color:var(--gray);">No hay bloques</td></tr>';
            } else {
                document.getElementById('table-tbody').innerHTML = blocks.map(b => `
                    <tr>
                        <td><code style="font-size:0.75em;">${b.block_id.substring(0, 12)}...</code></td>
                        <td>${b.original_filename || 'N/A'}</td>
                        <td>${b.block_num}</td>
                        <td>${formatSize(b.size)}</td>
                        <td><span class="badge badge-primary">${b.primary_node}</span></td>
                        <td><span class="badge badge-replica">${b.replica_node}</span></td>
                    </tr>
                `).join('');
            }

            openModal('modal-table');
        }
    });
}

function openModal(id) {
    const m = document.getElementById(id);
    m.style.display = 'flex';
    gsap.fromTo(m, { opacity: 0 }, { opacity: 1, duration: 0.3 });
    gsap.fromTo(m.querySelector('.modal-content'), { scale: 0.9, y: 50 }, { scale: 1, y: 0, duration: 0.4, ease: "back.out(1.7)" });
}

function closeModal(id) {
    const m = document.getElementById(id);
    gsap.to(m, { opacity: 0, duration: 0.2, onComplete: () => m.style.display = 'none' });
}

function toast(msg, type = 'info') {
    const container = document.getElementById('toast-container');
    const t = document.createElement('div');
    t.className = `toast ${type}`;
    t.innerHTML = `<span class="toast-icon">${type === 'success' ? '✅' : type === 'error' ? '❌' : 'ℹ️'}</span><div>${msg}</div>`;
    container.appendChild(t);

    gsap.fromTo(t, { x: 50, opacity: 0 }, { x: 0, opacity: 1, duration: 0.4, ease: "back.out(1.7)" });

    setTimeout(() => {
        gsap.to(t, { x: 50, opacity: 0, duration: 0.3, onComplete: () => t.remove() });
    }, 4000);
}

function formatSize(b) {
    if (!b) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(b) / Math.log(k));
    return parseFloat((b / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function formatDate(ts) {
    if (!ts) return 'N/A';
    return new Date(ts * 1000).toLocaleString('es-MX');
}

window.onclick = e => { if (e.target.classList.contains('modal')) closeModal(e.target.id); }
