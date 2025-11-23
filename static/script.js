/* Lógica del cliente con Animaciones GSAP */

let selectedFile = null;
let distributedFiles = [];
let nodeStatus = {};

// Configuración GSAP
gsap.config({ nullTargetWarn: false });

document.addEventListener('DOMContentLoaded', function () {
    // Animación de entrada inicial
    initAnimations();

    loadAll();
    setInterval(loadAll, 3000);

    document.getElementById('file-input').addEventListener('change', handleFileUpload);
    document.getElementById('btn-download').addEventListener('click', downloadFile);
    document.getElementById('btn-attributes').addEventListener('click', showAttributes);
    document.getElementById('btn-delete').addEventListener('click', deleteFile);
    document.getElementById('btn-block-table').addEventListener('click', showBlockTable);
});

function initAnimations() {
    const tl = gsap.timeline();

    tl.from('header', {
        y: -50,
        opacity: 0,
        duration: 1,
        ease: 'power3.out'
    })
        .from('.stat-item', {
            y: 20,
            opacity: 0,
            duration: 0.6,
            stagger: 0.1,
            ease: 'back.out(1.7)'
        }, '-=0.5')
        .from('.actions', {
            scale: 0.95,
            opacity: 0,
            duration: 0.5
        }, '-=0.3')
        .from('.panel', {
            y: 30,
            opacity: 0,
            duration: 0.8,
            stagger: 0.2,
            ease: 'power2.out'
        }, '-=0.3');
}

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
                const newFiles = data.files || [];
                // Solo renderizar si hay cambios para evitar parpadeos innecesarios
                // (En una app real usaríamos diffing, aquí simplificamos)
                if (JSON.stringify(newFiles) !== JSON.stringify(distributedFiles)) {
                    distributedFiles = newFiles;
                    renderDistributedFiles();
                    animateValue('total-files', parseInt(document.getElementById('total-files').textContent), distributedFiles.length);
                }
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
                    const isOnline = data[node];
                    const currentClass = el.className;
                    const newClass = `node-status ${isOnline ? 'online' : 'offline'}`;

                    if (currentClass !== newClass) {
                        el.className = newClass;
                        // Animar cambio de estado
                        gsap.fromTo(el, { scale: 1.5 }, { scale: 1, duration: 0.5, ease: 'elastic.out(1, 0.3)' });
                    }

                    if (isOnline) online++;
                }
            }
            animateValue('nodes-online', parseInt(document.getElementById('nodes-online').textContent), online);
        })
        .catch(e => console.error('Error:', e));
}

function loadSystemStats() {
    fetch('/api/system_stats')
        .then(r => r.json())
        .then(data => {
            if (data.status === 'ok') {
                const stats = data.stats;
                animateValue('total-blocks', parseInt(document.getElementById('total-blocks').textContent), stats.total_blocks || 0);

                let totalUsed = 0;
                for (let node in stats.node_usage) {
                    const used = stats.node_usage[node] || 0;
                    const capacity = stats.node_capacity[node] || 50;
                    const percent = Math.min((used / capacity) * 100, 100);
                    totalUsed += used;

                    const capEl = document.getElementById(`capacity-${node}`);
                    const txtEl = document.getElementById(`capacity-text-${node}`);

                    if (capEl) {
                        // Animar barra de capacidad
                        gsap.to(capEl, { width: `${percent}%`, duration: 1, ease: 'power2.out' });

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

function animateValue(id, start, end) {
    if (start === end) return;
    const obj = { val: start };
    gsap.to(obj, {
        val: end,
        duration: 1,
        ease: 'power1.out',
        onUpdate: function () {
            document.getElementById(id).textContent = Math.floor(obj.val);
        }
    });
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
                <span class="file-icon">📄</span>
                <span>${file.filename}</span>
            </div>
            <span class="file-size">${formatSize(file.size)} • ${file.total_blocks} bloque(s)</span>
        </li>
    `).join('');

    // Animar entrada de items
    gsap.from('.file-item', {
        y: 10,
        opacity: 0,
        duration: 0.4,
        stagger: 0.05,
        ease: 'power1.out',
        clearProps: 'all' // Limpiar propiedades para no interferir con hover CSS
    });
}

function selectFile(fileId) {
    selectedFile = distributedFiles.find(f => f.file_id === fileId);

    // Actualizar clases visuales sin re-renderizar todo para mantener animaciones suaves
    document.querySelectorAll('.file-item').forEach(el => {
        el.classList.remove('selected');
        if (el.getAttribute('onclick').includes(fileId)) {
            el.classList.add('selected');
            // Pequeño pulso al seleccionar
            gsap.fromTo(el, { scale: 0.98 }, { scale: 1, duration: 0.3, ease: 'back.out(2)' });
        }
    });

    const hasSelection = selectedFile !== null;
    const buttons = ['btn-download', 'btn-attributes', 'btn-delete'];

    buttons.forEach(btnId => {
        const btn = document.getElementById(btnId);
        btn.disabled = !hasSelection;
        if (hasSelection) {
            gsap.fromTo(btn, { scale: 0.9 }, { scale: 1, duration: 0.4, ease: 'elastic.out(1, 0.5)' });
        }
    });
}

function handleFileUpload(event) {
    const file = event.target.files[0];
    if (!file) return;

    const formData = new FormData();
    formData.append('file', file);

    const progContainer = document.getElementById('progress-container');
    const progFill = document.getElementById('progress-fill');
    const progText = document.getElementById('progress-text');

    // Mostrar contenedor con animación
    progContainer.style.display = 'block';
    gsap.fromTo(progContainer, { height: 0, opacity: 0 }, { height: 'auto', opacity: 1, duration: 0.5 });

    progFill.style.width = '0%';
    progText.textContent = `Subiendo y distribuyendo ${file.name}...`;

    let progress = 0;
    const interval = setInterval(() => {
        progress += Math.random() * 15;
        if (progress > 90) progress = 90;
        gsap.to(progFill, { width: `${progress}%`, duration: 0.2 });
    }, 200);

    fetch('/api/upload', { method: 'POST', body: formData })
        .then(r => r.json())
        .then(data => {
            clearInterval(interval);
            gsap.to(progFill, { width: '100%', duration: 0.3 });

            setTimeout(() => {
                // Ocultar con animación
                gsap.to(progContainer, {
                    height: 0,
                    opacity: 0,
                    duration: 0.5,
                    onComplete: () => { progContainer.style.display = 'none'; }
                });

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
            gsap.to(progContainer, { height: 0, opacity: 0, duration: 0.5 });
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
                        <td><code style="color:var(--text-muted);">${(b.hash || '').substring(0, 8)}...</code></td>
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
                    document.getElementById('all-blocks').innerHTML = '<tr><td colspan="7" style="text-align:center;color:var(--text-muted);">No hay bloques</td></tr>';
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

function openModal(id) {
    const modal = document.getElementById(id);
    const content = modal.querySelector('.modal-content');

    modal.style.display = 'block';

    // Animación de entrada modal
    gsap.to(modal, { opacity: 1, duration: 0.3 });
    gsap.fromTo(content,
        { scale: 0.8, opacity: 0, y: 20 },
        { scale: 1, opacity: 1, y: 0, duration: 0.5, ease: 'back.out(1.2)' }
    );
}

function closeModal(id) {
    const modal = document.getElementById(id);
    const content = modal.querySelector('.modal-content');

    // Animación de salida modal
    gsap.to(content, { scale: 0.8, opacity: 0, y: 20, duration: 0.3, ease: 'power2.in' });
    gsap.to(modal, {
        opacity: 0,
        duration: 0.3,
        delay: 0.1,
        onComplete: () => { modal.style.display = 'none'; }
    });
}

function showToast(msg, type = 'info') {
    const container = document.getElementById('toast-container');
    const toast = document.createElement('div');
    toast.className = `toast ${type}`;
    toast.innerHTML = msg;
    container.appendChild(toast);

    // Animación de entrada toast
    gsap.to(toast, { x: 0, opacity: 1, duration: 0.5, ease: 'back.out(1.2)' });

    setTimeout(() => {
        // Animación de salida toast
        gsap.to(toast, {
            x: 100,
            opacity: 0,
            duration: 0.5,
            ease: 'power2.in',
            onComplete: () => toast.remove()
        });
    }, 4000);
}

window.onclick = function (e) {
    if (e.target.classList.contains('modal')) {
        closeModal(e.target.id);
    }
}
