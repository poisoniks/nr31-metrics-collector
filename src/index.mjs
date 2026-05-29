import http from 'node:http';
import { execSync } from 'node:child_process';
import fs from 'node:fs/promises';
import Docker from 'dockerode';

const PORT = process.env.PORT || 9100;
const HOST_PROC = process.env.HOST_PROC || '/host/proc';
const isDebug = process.argv.includes('--debug') || process.env.DEBUG === 'true';

function logDebug(message, data) {
    if (isDebug) {
        console.log(`[DEBUG] ${new Date().toISOString()} - ${message}`);
        if (data) {
            console.dir(data, { depth: null, colors: true });
        }
    }
}

const docker = new Docker({ socketPath: '/var/run/docker.sock' });

// Global State
const state = {
    host: {
        cpuCores: 1,
        cpuUsagePercent: 0,
        memoryTotalBytes: 0,
        memoryUsedBytes: 0,
        memoryAvailableBytes: 0,
        diskTotalBytes: 0,
        diskUsedBytes: 0,
        diskAvailableBytes: 0
    },
    containers: {}, // map of id -> container stats
    volumes: []
};

// History State
const history = []; // array of 10-second snapshots, max 60

// SSE Clients
const clients = new Set();

// --- Background Pollers ---

async function pollHostCpuAndMem() {
    // Read Mem
    try {
        const memData = await fs.readFile(`${HOST_PROC}/meminfo`, 'utf8');
        let memTotal = 0, memAvailable = 0;
        for (const line of memData.split('\n')) {
            if (line.startsWith('MemTotal:')) memTotal = parseInt(line.match(/\d+/)[0], 10) * 1024;
            else if (line.startsWith('MemAvailable:')) memAvailable = parseInt(line.match(/\d+/)[0], 10) * 1024;
        }
        state.host.memoryTotalBytes = memTotal;
        state.host.memoryAvailableBytes = memAvailable;
        state.host.memoryUsedBytes = memTotal - memAvailable;
    } catch (e) {
        logDebug('Error reading meminfo:', e.message);
    }

    // Read CPU
    try {
        const cpuData = await fs.readFile(`${HOST_PROC}/stat`, 'utf8');
        const cpuLine = cpuData.split('\n').find(line => line.startsWith('cpu '));
        if (cpuLine) {
            const parts = cpuLine.match(/\d+/g).map(Number);
            const idle = parts[3] + (parts[4] || 0);
            const total = parts.reduce((a, b) => a + b, 0);
            
            if (state._lastCpu) {
                const idleDelta = idle - state._lastCpu.idle;
                const totalDelta = total - state._lastCpu.total;
                if (totalDelta > 0) {
                    state.host.cpuUsagePercent = (1 - (idleDelta / totalDelta)) * 100;
                }
            }
            state._lastCpu = { idle, total };
        }
        
        const cpuinfo = await fs.readFile(`${HOST_PROC}/cpuinfo`, 'utf8');
        state.host.cpuCores = cpuinfo.split('\n').filter(line => line.startsWith('processor')).length || 1;
    } catch (e) {
        logDebug('Error reading cpu stats:', e.message);
    }
}

async function pollDisksAndVolumes() {
    try {
        const output = execSync('df -B1 /').toString();
        const lines = output.trim().split('\n');
        if (lines.length >= 2) {
            const parts = lines[1].trim().split(/\s+/);
            state.host.diskTotalBytes = parseInt(parts[1], 10) || 0;
            state.host.diskUsedBytes = parseInt(parts[2], 10) || 0;
            state.host.diskAvailableBytes = parseInt(parts[3], 10) || 0;
        }
    } catch (e) {
        logDebug('Error reading disk stats:', e.message);
    }

    try {
        const dfData = await docker.df();
        const volumes = [];
        for (const vol of dfData.Volumes || []) {
            volumes.push({
                name: vol.Name,
                sizeBytes: vol.UsageData?.Size || 0
            });
        }
        state.volumes = volumes;
    } catch (e) {
        logDebug('Error reading docker df:', e.message);
    }
}

// Container Streams
const activeStreams = new Map(); // id -> stream

function calculateContainerCpuPercent(stats) {
    if (!stats || !stats.cpu_stats || !stats.precpu_stats) return 0.0;
    const cpuDelta = stats.cpu_stats.cpu_usage.total_usage - stats.precpu_stats.cpu_usage.total_usage;
    const systemCpuDelta = stats.cpu_stats.system_cpu_usage - stats.precpu_stats.system_cpu_usage;
    if (systemCpuDelta > 0.0 && cpuDelta > 0.0) {
        const numberCpus = stats.cpu_stats.online_cpus || stats.cpu_stats.cpu_usage.percpu_usage?.length || 1;
        return (cpuDelta / systemCpuDelta) * numberCpus * 100.0;
    }
    return 0.0;
}

async function updateContainersList() {
    try {
        const containers = await docker.listContainers({ all: true });
        const currentIds = new Set(containers.map(c => c.Id));
        
        // Remove dead streams
        for (const id of activeStreams.keys()) {
            if (!currentIds.has(id)) {
                activeStreams.get(id).destroy();
                activeStreams.delete(id);
                delete state.containers[id];
            }
        }
        
        for (const c of containers) {
            if (!state.containers[c.Id]) {
                const inspect = await docker.getContainer(c.Id).inspect();
                let name = inspect.Name;
                if (name.startsWith('/')) name = name.substring(1);
                
                state.containers[c.Id] = {
                    id: c.Id.substring(0, 12),
                    name,
                    service: inspect.Config?.Labels?.['com.docker.compose.service'] || 'unknown',
                    status: inspect.State.Status,
                    startedAt: inspect.State.StartedAt,
                    restartCount: inspect.RestartCount,
                    cpuPercent: 0,
                    memoryUsedBytes: 0,
                    memoryLimitBytes: 0
                };
            } else {
                state.containers[c.Id].status = c.State;
            }
            
            if (c.State === 'running' && !activeStreams.has(c.Id)) {
                try {
                    const stream = await docker.getContainer(c.Id).stats({ stream: true });
                    activeStreams.set(c.Id, stream);
                    
                    let buffer = '';
                    stream.on('data', (chunk) => {
                        buffer += chunk.toString();
                        let index;
                        while ((index = buffer.indexOf('\n')) !== -1) {
                            const line = buffer.substring(0, index).trim();
                            buffer = buffer.substring(index + 1);
                            if (!line) continue;
                            try {
                                const stats = JSON.parse(line);
                                const cpuPercent = calculateContainerCpuPercent(stats);
                                let memoryUsedBytes = stats.memory_stats?.usage || 0;
                                if (stats.memory_stats?.stats?.cache) {
                                    memoryUsedBytes -= stats.memory_stats.stats.cache;
                                }
                                const memoryLimitBytes = stats.memory_stats?.limit || 0;
                                
                                if (state.containers[c.Id]) {
                                    state.containers[c.Id].cpuPercent = cpuPercent;
                                    state.containers[c.Id].memoryUsedBytes = memoryUsedBytes;
                                    state.containers[c.Id].memoryLimitBytes = memoryLimitBytes;
                                }
                            } catch (e) {
                                // Ignore parse error
                            }
                        }
                    });
                    
                    stream.on('error', () => {
                        activeStreams.delete(c.Id);
                    });
                    stream.on('end', () => {
                        activeStreams.delete(c.Id);
                    });
                } catch (e) {
                    logDebug(`Could not attach to container ${c.Id}:`, e.message);
                }
            }
        }
    } catch (e) {
        logDebug('Error listing containers:', e.message);
    }
}

// Data Aggregation
let currentBucket = [];

function getSnapshot() {
    return {
        host: { ...state.host },
        containers: Object.values(state.containers),
        volumes: [...state.volumes],
        collectedAt: new Date().toISOString()
    };
}

function recordSnapshot() {
    const snap = getSnapshot();
    currentBucket.push(snap);
    
    // Broadcast to SSE clients
    const payload = `data: ${JSON.stringify(snap)}\n\n`;
    for (const res of clients) {
        res.write(payload);
    }
}

function aggregateBucket() {
    if (currentBucket.length === 0) return;
    
    const count = currentBucket.length;
    
    // Aggregate Host
    const avgHost = { ...currentBucket[0].host };
    for (const key of ['cpuUsagePercent', 'memoryUsedBytes', 'diskUsedBytes']) {
        let sum = 0;
        for (const snap of currentBucket) sum += snap.host[key];
        avgHost[key] = sum / count;
    }
    
    // Aggregate Containers
    const containerMap = {};
    for (const snap of currentBucket) {
        for (const c of snap.containers) {
            if (!containerMap[c.id]) {
                containerMap[c.id] = { ...c, cpuSum: 0, memSum: 0, cCount: 0 };
            }
            containerMap[c.id].cpuSum += c.cpuPercent;
            containerMap[c.id].memSum += c.memoryUsedBytes;
            containerMap[c.id].cCount++;
        }
    }
    
    const avgContainers = [];
    for (const id in containerMap) {
        const c = containerMap[id];
        avgContainers.push({
            ...c,
            cpuPercent: c.cpuSum / c.cCount,
            memoryUsedBytes: c.memSum / c.cCount,
            cpuSum: undefined, memSum: undefined, cCount: undefined
        });
    }
    
    history.push({
        host: avgHost,
        containers: avgContainers,
        volumes: currentBucket[currentBucket.length - 1].volumes,
        collectedAt: currentBucket[currentBucket.length - 1].collectedAt,
        aggregatedFrom: count
    });
    
    if (history.length > 60) {
        history.shift(); // Keep last 10 minutes (60 * 10s)
    }
    
    currentBucket = [];
}

// Start Background Loops
pollHostCpuAndMem();
setInterval(pollHostCpuAndMem, 1000);

pollDisksAndVolumes();
setInterval(pollDisksAndVolumes, 3 * 60 * 1000);

updateContainersList();
setInterval(updateContainersList, 5000);

setInterval(recordSnapshot, 1000);
setInterval(aggregateBucket, 10000);


// Server
const server = http.createServer((req, res) => {
    res.setHeader('Access-Control-Allow-Origin', '*');
    
    if (req.method === 'GET' && req.url === '/health') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ status: 'ok' }));
        return;
    }
    
    if (req.method === 'GET' && req.url === '/metrics') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(getSnapshot()));
        return;
    }
    
    if (req.method === 'GET' && req.url === '/metrics/history') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(history));
        return;
    }
    
    if (req.method === 'GET' && req.url === '/metrics/stream') {
        res.writeHead(200, {
            'Content-Type': 'text/event-stream',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive'
        });
        
        // Send initial snapshot
        res.write(`data: ${JSON.stringify(getSnapshot())}\n\n`);
        
        clients.add(res);
        req.on('close', () => {
            clients.delete(res);
        });
        return;
    }
    
    res.writeHead(404, { 'Content-Type': 'text/plain' });
    res.end('Not Found');
});

server.listen(PORT, () => {
    console.log(`Metrics collector listening on port ${PORT}`);
});
