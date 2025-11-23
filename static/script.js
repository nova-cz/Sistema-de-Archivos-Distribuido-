/* Lógica del cliente para el Sistema de Archivos Distribuido */

let selectedFile = null;
let distributedFiles = [];
let nodeStatus = {};

document.addEventListener('DOMContentLoaded', function() {
    loadAll();
    setInterval(loadAll, 3000);
    
    document.getElementById('file-input').addEventListener('change', handleFileUpload);
    document.getElementById('btn-download').addEventListener('click', downloadFile);
    document.getElementById('btn-attributes').addEventListener('click', showAttributes);
    document.getElementById('btn-delete').addEventListener('click', deleteFile);
    document.getElementById('btn-block-table').addEventListener('click', showBlockTable);
});

function loadAll() {
    loadDistributedFiles();
    loadNodeStatus();
    loadSystemStats();
}

function loadDistributedFiles() {
    fetch('/api/distributed_files')
        .then(r => r.json())
        .then(data => {
            if (data.status === 'ok') {
                distributedFiles = data.files || [];
                renderDistributedFiles();
                document.getElementById('total-files').textContent = distributedFiles.length;
            }
        })
        .catch(e => console.error('Error:', e));
}

function loadNodeStatus() {
    fetch('/api/status')
        .then(r => r.json())
        .then(data => {
            nodeStatus = data;
            let online = 0;
            for (let node in data) {
                const el = document.getElementById(`status-${node}`);
                if (el) {
                    el.className = `node-status ${data[node] ? 'online' : 'offline'}`;
                    if (data[node]) online++;
                }
            }
            document.getElementById('nodes-online').textContent = online;
        })
        .catch(e => console.error('Error:', e));
}

function loadSystemStats() {
    fetch('/api/system_stats')
        .then(r => r.json())
        .then(data => {
            if (data.status === 'ok') {
                const stats = data.stats;
                document.getElementById('total-blocks').textContent = stats.total_blocks || 0;
                
                let totalUsed = 0;
                for (let node in stats.node_usage) {
                    const used = stats.node_usage[node] || 0;
                    const capacity = stats.node_capacity[node] || 50;
                    const percent = Math.min((used / capacity) * 100, 100);
                    totalUsed += used;
                    
                    const capEl = document.getElementById(`capacity-${node}`);
                    const txtEl = document.getElementById(`capacity-text-${node}`);
                    
                    if (capEl) {
                        capEl.style.width = `${percent}%`;
                        capEl.className = 'capacity-fill';
                        if (percent > 80) capEl.classList.add('danger');
                        else if (percent > 60) capEl.classList.add('warning');
                    }
                    if (txtEl) txtEl.textContent = `${used} / ${capacity} MB`;
                }
                document.getElementById('total-space').textContent = `${totalUsed} MB`;
            }
        })
        .catch(e => console.error('Error:', e));
}

function renderDistributedFiles() {
    const container = document.getElementById('distributed-files');
    
    if (distributedFiles.length === 0) {
        container.innerHTML = '<li class="empty-message">No hay archivos. ¡Sube uno con el botón verde!</li>';
        return;
    }
    
    container.innerHTML = distributedFiles.map(file => `
        <li class="file-item ${selectedFile && selectedFile.file_id === file.file_id ? 'selected' : ''}"
            onclick="selectFile('${file.file_id}')">
            <div class="file-name">
                <span>📄</span>
                <span>${file.filename}</span>
            </div>
            <span class="file-size">${formatSize(file.size)} • ${file.total_blocks} bloque(s)</span>
        </li>
    `).join('');
}

function selectFile(fileId) {
    selectedFile = distributedFiles.find(f => f.file_id === fileId);
    renderDistributedFiles();
    
    const hasSelection = selectedFile !== null;
    document.getElementById('btn-download').disabled = !hasSelection;
    document.getElementById('btn-attributes').disabled = !hasSelection;
    document.getElementById('btn-delete').disabled = !hasSelection;
}

function handleFileUpload(event) {
    const file = event.target.files[0];
    if (!file) return;
    
    const formData = new FormData();
    formData.append('file', file);
    
    const progContainer = document.getElementById('progress-container');
    const progFill = document.getElementById('progress-fill');
    const progText = document.getElementById('progress-text');
    
    progContainer.style.display = 'block';
    progFill.style.width = '0%';
    progText.textContent = `Subiendo y distribuyendo ${file.name}...`;
    
    let progress = 0;
    const interval = setInterval(() => {
        progress += Math.random() * 15;
        if (progress > 90) progress = 90;
        progFill.style.width = `${progress}%`;
    }, 200);
    
    fetch('/api/upload', { method: 'POST', body: formData })
        .then(r => r.json())
        .then(data => {
            clearInterval(interval);
            progFill.style.width = '100%';
            
            setTimeout(() => {
                progContainer.style.display = 'none';
                if (data.status === 'ok') {
                    showToast(`✅ "${data.filename}" subido (${data.total_blocks} bloques distribuidos)`, 'success');
                    loadAll();
                } else {
                    showToast(`❌ Error: ${data.message}`, 'error');
                }
            }, 500);
        })
        .catch(e => {
            clearInterval(interval);
            progContainer.style.display = 'none';
            showToast('❌ Error al subir archivo', 'error');
        });
    
    event.target.value = '';
}

function downloadFile() {
    if (!selectedFile) return;
    showToast(`📥 Reconstruyendo y descargando ${selectedFile.filename}...`, 'success');
    
    const link = document.createElement('a');
    link.href = `/api/download/${selectedFile.file_id}`;
    link.download = selectedFile.filename;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
}

function deleteFile() {
    if (!selectedFile) return;
    if (!confirm(`¿Eliminar "${selectedFile.filename}" y todos sus bloques de todos los nodos?`)) return;
    
    fetch(`/api/delete_distributed/${selectedFile.file_id}`, { method: 'DELETE' })
        .then(r => r.json())
        .then(data => {
            if (data.status === 'ok') {
                showToast('✅ Archivo eliminado de todos los nodos', 'success');
                selectedFile = null;
                document.getElementById('btn-download').disabled = true;
                document.getElementById('btn-attributes').disabled = true;
                document.getElementById('btn-delete').disabled = true;
                loadAll();
            } else {
                showToast(`❌ Error: ${data.message}`, 'error');
            }
        })
        .catch(e => showToast('❌ Error al eliminar', 'error'));
}

function showAttributes() {
    if (!selectedFile) return;
    
    fetch(`/api/file_attributes/${selectedFile.file_id}`)
        .then(r => r.json())
        .then(data => {
            if (data.status === 'ok') {
                const attrs = data.attributes;
                
                document.getElementById('attributes-grid').innerHTML = `
                    <div class="attr-item">
                        <div class="attr-label">Nombre</div>
                        <div class="attr-value">${attrs.original_filename}</div>
                    </div>
                    <div class="attr-item">
                        <div class="attr-label">ID</div>
                        <div class="attr-value" style="font-size:0.9em;">${selectedFile.file_id}</div>
                    </div>
                    <div class="attr-item">
                        <div class="attr-label">Tamaño Total</div>
                        <div class="attr-value">${formatSize(attrs.size)}</div>
                    </div>
                    <div class="attr-item">
                        <div class="attr-label">Bloques</div>
                        <div class="attr-value">${attrs.total_blocks}</div>
                    </div>
                    <div class="attr-item">
                        <div class="attr-label">Creado</div>
                        <div class="attr-value">${formatDate(attrs.created_at)}</div>
                    </div>
                    <div class="attr-item">
                        <div class="attr-label">Replicación</div>
                        <div class="attr-value">✅ Cada bloque tiene réplica</div>
                    </div>
                `;
                
                document.getElementById('blocks-detail').innerHTML = (attrs.blocks_detail || []).map(b => `
                    <tr>
                        <td>${b.block_num}</td>
                        <td>${formatSize(b.size)}</td>
                        <td><span class="block-badge primary">${b.primary_node}</span> ${!nodeStatus[b.primary_node] ? '⚠️' : ''}</td>
                        <td><span class="block-badge replica">${b.replica_node}</span> ${!nodeStatus[b.replica_node] ? '⚠️' : ''}</td>
                        <td><code style="color:#888;">${(b.hash || '').substring(0, 8)}...</code></td>
                    </tr>
                `).join('');
                
                openModal('modal-attributes');
            } else {
                showToast(`❌ ${data.message}`, 'error');
            }
        })
        .catch(e => showToast('❌ Error al obtener atributos', 'error'));
}

function showBlockTable() {
    Promise.all([
        fetch('/api/block_table').then(r => r.json()),
        fetch('/api/system_stats').then(r => r.json())
    ])
    .then(([tableData, statsData]) => {
        if (tableData.status === 'ok' && statsData.status === 'ok') {
            const blocks = tableData.block_table.blocks || {};
            const stats = statsData.stats;
            
            document.getElementById('system-stats').innerHTML = `
                <div class="attr-item">
                    <div class="attr-label">Total Archivos</div>
                    <div class="attr-value">${stats.total_files}</div>
                </div>
                <div class="attr-item">
                    <div class="attr-label">Total Bloques</div>
                    <div class="attr-value">${stats.total_blocks}</div>
                </div>
                ${Object.keys(stats.node_capacity).map(node => `
                    <div class="attr-item">
                        <div class="attr-label">${node}</div>
                        <div class="attr-value">
                            ${stats.node_usage[node] || 0} / ${stats.node_capacity[node]} MB
                            <span class="block-badge ${nodeStatus[node] ? 'primary' : 'replica'}">${nodeStatus[node] ? 'Online' : 'Offline'}</span>
                        </div>
                    </div>
                `).join('')}
            `;
            
            const blocksList = Object.values(blocks);
            if (blocksList.length === 0) {
                document.getElementById('all-blocks').innerHTML = '<tr><td colspan="7" style="text-align:center;color:#666;">No hay bloques</td></tr>';
            } else {
                document.getElementById('all-blocks').innerHTML = blocksList.map(b => `
                    <tr>
                        <td><code style="font-size:0.75em;">${b.block_id.substring(0, 12)}...</code></td>
                        <td>${b.original_filename || 'N/A'}</td>
                        <td>${b.block_num}</td>
                        <td>${formatSize(b.size)}</td>
                        <td><span class="block-badge primary">${b.primary_node}</span></td>
                        <td><span class="block-badge replica">${b.replica_node}</span></td>
                        <td><span class="block-badge primary">${b.status}</span></td>
                    </tr>
                `).join('');
            }
            
            openModal('modal-block-table');
        }
    })
    .catch(e => showToast('❌ Error al cargar tabla', 'error'));
}

function formatSize(bytes) {
    if (!bytes) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function formatDate(ts) {
    if (!ts) return 'N/A';
    return new Date(ts * 1000).toLocaleString('es-MX');
}

function openModal(id) { document.getElementById(id).style.display = 'block'; }
function closeModal(id) { document.getElementById(id).style.display = 'none'; }

function showToast(msg, type = 'info') {
    const container = document.getElementById('toast-container');
    const toast = document.createElement('div');
    toast.className = `toast ${type}`;
    toast.innerHTML = msg;
    container.appendChild(toast);
    setTimeout(() => {
        toast.style.opacity = '0';
        setTimeout(() => toast.remove(), 300);
    }, 4000);
}

window.onclick = function(e) {
    if (e.target.classList.contains('modal')) e.target.style.display = 'none';
}
