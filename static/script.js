// Variables globales
let selectedFile = null;
let allFiles = [];

// ==================== INICIALIZACIÓN ====================

document.addEventListener('DOMContentLoaded', () => {
    initEventListeners();
    loadDistributedFiles();
    loadSystemStats();
    loadNodeStatus();
    
    // Actualizar cada 3 segundos
    setInterval(() => {
        loadDistributedFiles();
        loadSystemStats();
        loadNodeStatus();
    }, 3000);
});

function initEventListeners() {
    document.getElementById('file-input').addEventListener('change', handleFileUpload);
    document.getElementById('btn-view').addEventListener('click', viewFile);
    document.getElementById('btn-download').addEventListener('click', downloadFile);
    document.getElementById('btn-attributes').addEventListener('click', showAttributes);
    document.getElementById('btn-delete').addEventListener('click', deleteFile);
    document.getElementById('btn-table').addEventListener('click', showBlockTable);
}

// ==================== CARGAR ARCHIVOS DISTRIBUIDOS ====================

async function loadDistributedFiles() {
    try {
        const response = await fetch('/api/distributed_files');
        const data = await response.json();
        
        if (data.status === 'ok') {
            allFiles = data.files || [];
            renderFileList(allFiles);
        }
    } catch (error) {
        console.error('Error al cargar archivos:', error);
    }
}

function renderFileList(files) {
    const fileList = document.getElementById('file-list');
    fileList.innerHTML = '';
    
    if (files.length === 0) {
        fileList.innerHTML = '<li class="empty-state">No hay archivos subidos</li>';
        return;
    }
    
    files.forEach(file => {
        const li = document.createElement('li');
        li.className = 'file-item';
        if (selectedFile && selectedFile.file_id === file.file_id) {
            li.classList.add('selected');
        }
        
        const size = formatFileSize(file.size);
        const date = new Date(file.created_at * 1000).toLocaleString('es-ES');
        
        li.innerHTML = `
            <div class="file-info">
                <div class="file-name">📄 ${file.filename}</div>
                <div class="file-meta">
                    <span>${size}</span>
                    <span>${file.total_blocks} bloques</span>
                    <span>${date}</span>
                </div>
            </div>
        `;
        
        li.addEventListener('click', () => selectFile(file, li));
        fileList.appendChild(li);
    });
}

function selectFile(file, element) {
    selectedFile = file;
    
    // Actualizar selección visual
    document.querySelectorAll('.file-item').forEach(item => {
        item.classList.remove('selected');
    });
    element.classList.add('selected');
    
    // Habilitar botones
    document.getElementById('btn-view').disabled = false;
    document.getElementById('btn-download').disabled = false;
    document.getElementById('btn-attributes').disabled = false;
    document.getElementById('btn-delete').disabled = false;
}

// ==================== SUBIR ARCHIVO ====================

async function handleFileUpload(event) {
    const file = event.target.files[0];
    if (!file) return;
    
    showProgress(`Subiendo ${file.name}...`);
    
    const formData = new FormData();
    formData.append('file', file);
    
    try {
        const response = await fetch('/api/upload', {
            method: 'POST',
            body: formData
        });
        
        const result = await response.json();
        
        if (result.status === 'ok') {
            hideProgress();
            showToast(`✅ Archivo subido: ${result.filename}`, 'success');
            loadDistributedFiles();
            loadSystemStats();
        } else {
            hideProgress();
            showToast(`❌ Error: ${result.message}`, 'error');
        }
    } catch (error) {
        hideProgress();
        showToast('❌ Error al subir archivo', 'error');
        console.error(error);
    }
    
    // Limpiar input
    event.target.value = '';
}

// ==================== VER ARCHIVO ====================

async function viewFile() {
    if (!selectedFile) return;
    
    const modal = document.getElementById('modal-view');
    const modalBody = document.getElementById('view-body');
    const modalTitle = document.getElementById('view-title');
    
    modalTitle.textContent = `👁️ ${selectedFile.filename}`;
    modalBody.innerHTML = '<div style="text-align:center; padding:40px;"><div style="font-size:3em;">⏳</div><div>Cargando...</div></div>';
    modal.style.display = 'flex';
    
    try {
        const response = await fetch(`/api/view_distributed/${selectedFile.file_id}`);
        const data = await response.json();
        
        if (data.status === 'ok') {
            if (data.file_type === 'text') {
                modalBody.innerHTML = `<pre class="file-content">${escapeHtml(data.content)}</pre>`;
            } else if (data.file_type === 'image') {
                modalBody.innerHTML = `<img src="data:${data.mime_type};base64,${data.content}" alt="${data.filename}" style="max-width:100%; border-radius:8px;">`;
            } else {
                modalBody.innerHTML = '<div class="error-message">⚠️ Tipo de archivo no soportado para visualización</div>';
            }
        } else {
            modalBody.innerHTML = `<div class="error-message">❌ ${data.message}</div>`;
        }
    } catch (error) {
        modalBody.innerHTML = '<div class="error-message">❌ Error al cargar archivo</div>';
    }
}

// ==================== DESCARGAR ARCHIVO ====================

function downloadFile() {
    if (!selectedFile) return;
    window.location.href = `/api/download/${selectedFile.file_id}`;
    showToast('⬇️ Descargando archivo...', 'info');
}

// ==================== ELIMINAR ARCHIVO ====================

async function deleteFile() {
    if (!selectedFile) return;
    
    if (!confirm(`¿Eliminar "${selectedFile.filename}"?\n\nEsto eliminará el archivo y todos sus bloques del sistema distribuido.`)) {
        return;
    }
    
    try {
        const response = await fetch(`/api/delete_distributed/${selectedFile.file_id}`, {
            method: 'DELETE'
        });
        
        const result = await response.json();
        
        if (result.status === 'ok') {
            showToast('🗑️ Archivo eliminado correctamente', 'success');
            selectedFile = null;
            loadDistributedFiles();
            loadSystemStats();
            
            // Deshabilitar botones
            document.getElementById('btn-view').disabled = true;
            document.getElementById('btn-download').disabled = true;
            document.getElementById('btn-attributes').disabled = true;
            document.getElementById('btn-delete').disabled = true;
        } else {
            showToast(`❌ Error: ${result.message}`, 'error');
        }
    } catch (error) {
        showToast('❌ Error al eliminar archivo', 'error');
        console.error(error);
    }
}

// ==================== ATRIBUTOS ====================

async function showAttributes() {
    if (!selectedFile) return;
    
    try {
        const response = await fetch(`/api/file_attributes/${selectedFile.file_id}`);
        const data = await response.json();
        
        if (data.status === 'ok') {
            const attrs = data.attributes;
            
            const attrGrid = document.getElementById('attr-grid');
            attrGrid.innerHTML = `
                <div class="attr-item">
                    <div class="attr-label">Nombre:</div>
                    <div class="attr-value">${attrs.original_filename}</div>
                </div>
                <div class="attr-item">
                    <div class="attr-label">Tamaño:</div>
                    <div class="attr-value">${formatFileSize(attrs.size)}</div>
                </div>
                <div class="attr-item">
                    <div class="attr-label">Bloques:</div>
                    <div class="attr-value">${attrs.total_blocks}</div>
                </div>
                <div class="attr-item">
                    <div class="attr-label">Creado:</div>
                    <div class="attr-value">${new Date(attrs.created_at * 1000).toLocaleString('es-ES')}</div>
                </div>
            `;
            
            const tbody = document.getElementById('blocks-tbody');
            tbody.innerHTML = '';
            
            attrs.blocks_detail.forEach(block => {
                const tr = document.createElement('tr');
                tr.innerHTML = `
                    <td>${block.block_num}</td>
                    <td>${formatFileSize(block.size)}</td>
                    <td><span class="node-badge-sm">${block.primary_node}</span></td>
                    <td><span class="node-badge-sm">${block.replica_node}</span></td>
                    <td><code>${block.hash.substring(0, 8)}...</code></td>
                `;
                tbody.appendChild(tr);
            });
            
            document.getElementById('modal-attr').style.display = 'flex';
        }
    } catch (error) {
        showToast('❌ Error al obtener atributos', 'error');
    }
}

// ==================== TABLA DE BLOQUES ====================

async function showBlockTable() {
    try {
        const [tableRes, statsRes] = await Promise.all([
            fetch('/api/block_table'),
            fetch('/api/system_stats')
        ]);
        
        const tableData = await tableRes.json();
        const statsData = await statsRes.json();
        
        if (tableData.status === 'ok' && statsData.status === 'ok') {
            const blocks = tableData.block_table.blocks || {};
            const stats = statsData.stats;
            
            // Mostrar estadísticas
            const statsGrid = document.getElementById('stats-grid');
            statsGrid.innerHTML = `
                <div class="attr-item">
                    <div class="attr-label">Archivos:</div>
                    <div class="attr-value">${stats.total_files}</div>
                </div>
                <div class="attr-item">
                    <div class="attr-label">Bloques:</div>
                    <div class="attr-value">${stats.total_blocks}</div>
                </div>
                <div class="attr-item">
                    <div class="attr-label">Espacio Usado:</div>
                    <div class="attr-value">${Object.values(stats.node_usage).reduce((a,b) => a+b, 0)} MB</div>
                </div>
                <div class="attr-item">
                    <div class="attr-label">Espacio Libre:</div>
                    <div class="attr-value">${Object.values(stats.node_free_space).reduce((a,b) => a+b, 0)} MB</div>
                </div>
            `;
            
            // Tabla de bloques con botón de eliminar
            const tbody = document.getElementById('table-tbody');
            tbody.innerHTML = '';
            
            if (Object.keys(blocks).length === 0) {
                tbody.innerHTML = '<tr><td colspan="7" style="text-align:center; padding:20px; color:var(--gray);">No hay bloques en el sistema</td></tr>';
            } else {
                Object.values(blocks).forEach(block => {
                    const tr = document.createElement('tr');
                    tr.innerHTML = `
                        <td><code>${block.block_id.substring(0, 12)}...</code></td>
                        <td>${block.original_filename}</td>
                        <td>${block.block_num}</td>
                        <td>${formatFileSize(block.size)}</td>
                        <td><span class="node-badge-sm">${block.primary_node}</span></td>
                        <td><span class="node-badge-sm">${block.replica_node}</span></td>
                        <td>
                            <button class="btn-delete-small" onclick="deleteFileFromTable('${block.file_id}', '${block.original_filename}')" title="Eliminar archivo">
                                🗑️
                            </button>
                        </td>
                    `;
                    tbody.appendChild(tr);
                });
            }
            
            document.getElementById('modal-table').style.display = 'flex';
        }
    } catch (error) {
        showToast('❌ Error al cargar tabla de bloques', 'error');
        console.error(error);
    }
}

// ==================== ELIMINAR DESDE TABLA ====================

async function deleteFileFromTable(fileId, filename) {
    if (!confirm(`¿Eliminar "${filename}"?\n\nEsto eliminará el archivo y todos sus bloques.`)) {
        return;
    }
    
    try {
        const response = await fetch(`/api/delete_distributed/${fileId}`, {
            method: 'DELETE'
        });
        
        const result = await response.json();
        
        if (result.status === 'ok') {
            showToast('🗑️ Archivo eliminado', 'success');
            showBlockTable(); // Recargar tabla
            loadDistributedFiles(); // Recargar lista
            loadSystemStats(); // Actualizar stats
        } else {
            showToast(`❌ Error: ${result.message}`, 'error');
        }
    } catch (error) {
        showToast('❌ Error al eliminar', 'error');
    }
}

// ==================== LIMPIAR TODO ====================

async function clearAllFiles() {
    if (!confirm('⚠️ ¿ELIMINAR TODOS LOS ARCHIVOS?\n\nEsta acción eliminará TODOS los archivos y bloques del sistema.\n\n¿Estás seguro?')) {
        return;
    }
    
    if (!confirm('⚠️ CONFIRMACIÓN FINAL\n\n¿Realmente quieres eliminar TODOS los archivos? Esta acción no se puede deshacer.')) {
        return;
    }
    
    showProgress('Eliminando todos los archivos...');
    
    try {
        // Obtener todos los archivos
        const response = await fetch('/api/distributed_files');
        const data = await response.json();
        
        if (data.status === 'ok' && data.files) {
            const files = data.files;
            let deleted = 0;
            let errors = 0;
            
            // Eliminar uno por uno
            for (const file of files) {
                try {
                    const delResponse = await fetch(`/api/delete_distributed/${file.file_id}`, {
                        method: 'DELETE'
                    });
                    const delResult = await delResponse.json();
                    
                    if (delResult.status === 'ok') {
                        deleted++;
                    } else {
                        errors++;
                    }
                } catch (err) {
                    errors++;
                }
            }
            
            hideProgress();
            
            if (errors === 0) {
                showToast(`✅ ${deleted} archivos eliminados correctamente`, 'success');
            } else {
                showToast(`⚠️ ${deleted} eliminados, ${errors} con errores`, 'warning');
            }
            
            loadDistributedFiles();
            loadSystemStats();
            
            // Cerrar modal si está abierto
            closeModal('modal-table');
        }
    } catch (error) {
        hideProgress();
        showToast('❌ Error al limpiar archivos', 'error');
        console.error(error);
    }
}

// ==================== ESTADÍSTICAS Y ESTADO ====================

async function loadSystemStats() {
    try {
        const response = await fetch('/api/system_stats');
        const data = await response.json();
        
        if (data.status === 'ok') {
            const stats = data.stats;
            
            document.getElementById('total-files').textContent = stats.total_files;
            document.getElementById('total-blocks').textContent = stats.total_blocks;
            
            const totalUsed = Object.values(stats.node_usage).reduce((a,b) => a+b, 0);
            document.getElementById('total-space').textContent = totalUsed;
            
            // Actualizar barras de capacidad
            for (const [node, used] of Object.entries(stats.node_usage)) {
                const capacity = stats.node_capacity[node] || 50;
                const percentage = (used / capacity) * 100;
                
                const capBar = document.getElementById(`cap-${node}`);
                const capText = document.getElementById(`cap-text-${node}`);
                
                if (capBar) {
                    capBar.style.width = `${Math.min(percentage, 100)}%`;
                    if (percentage > 80) {
                        capBar.style.background = 'var(--danger)';
                    } else if (percentage > 50) {
                        capBar.style.background = 'var(--warning)';
                    } else {
                        capBar.style.background = 'var(--primary)';
                    }
                }
                
                if (capText) {
                    capText.textContent = `${used} / ${capacity} MB`;
                }
            }
        }
    } catch (error) {
        console.error('Error al cargar stats:', error);
    }
}

async function loadNodeStatus() {
    try {
        const response = await fetch('/api/status');
        const status = await response.json();
        
        let onlineCount = 0;
        for (const [node, isOnline] of Object.entries(status)) {
            if (isOnline) onlineCount++;
            
            const statusDot = document.getElementById(`status-${node}`);
            if (statusDot) {
                statusDot.className = isOnline ? 'status-dot online' : 'status-dot offline';
            }
        }
        
        document.getElementById('nodes-online').textContent = onlineCount;
    } catch (error) {
        console.error('Error al cargar estado:', error);
    }
}

// ==================== UTILIDADES ====================

function formatFileSize(bytes) {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return Math.round((bytes / Math.pow(k, i)) * 100) / 100 + ' ' + sizes[i];
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function showProgress(message) {
    document.getElementById('progress-title').textContent = message;
    document.getElementById('progress-overlay').style.display = 'flex';
}

function hideProgress() {
    document.getElementById('progress-overlay').style.display = 'none';
}

function showToast(message, type = 'info') {
    const container = document.getElementById('toast-container');
    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;
    toast.textContent = message;
    
    container.appendChild(toast);
    
    setTimeout(() => toast.classList.add('show'), 10);
    
    setTimeout(() => {
        toast.classList.remove('show');
        setTimeout(() => container.removeChild(toast), 300);
    }, 3000);
}

function closeModal(modalId) {
    document.getElementById(modalId).style.display = 'none';
}

// Cerrar modales al hacer clic fuera
window.onclick = function(event) {
    if (event.target.classList.contains('modal')) {
        event.target.style.display = 'none';
    }
}