#!/bin/bash
#
# Generate system specification summary for academic benchmark documentation.
# Works on Ubuntu, Fedora, and other Linux distributions.
#

echo "=============================================="
echo "SYSTEM SPECIFICATION FOR BENCHMARK"
echo "=============================================="
echo ""

# OS Information
echo "## Operating System"
if [ -f /etc/os-release ]; then
    . /etc/os-release
    echo "Distribution: $NAME $VERSION"
else
    echo "Distribution: $(uname -s)"
fi
echo "Kernel: $(uname -r)"
echo "Architecture: $(uname -m)"
echo ""

# CPU Information
echo "## CPU"
if command -v lscpu &> /dev/null; then
    CPU_MODEL=$(lscpu | grep "Model name" | sed 's/Model name:[[:space:]]*//')
    CPU_CORES=$(lscpu | grep "^CPU(s):" | awk '{print $2}')
    CPU_SOCKETS=$(lscpu | grep "Socket(s):" | awk '{print $2}')
    CORES_PER_SOCKET=$(lscpu | grep "Core(s) per socket:" | awk '{print $4}')
    THREADS_PER_CORE=$(lscpu | grep "Thread(s) per core:" | awk '{print $4}')
    CPU_MAX_MHZ=$(lscpu | grep "CPU max MHz" | awk '{print $4}')
    CPU_MIN_MHZ=$(lscpu | grep "CPU min MHz" | awk '{print $4}')
    L1D_CACHE=$(lscpu | grep "L1d cache" | sed 's/L1d cache:[[:space:]]*//')
    L1I_CACHE=$(lscpu | grep "L1i cache" | sed 's/L1i cache:[[:space:]]*//')
    L2_CACHE=$(lscpu | grep "L2 cache" | sed 's/L2 cache:[[:space:]]*//')
    L3_CACHE=$(lscpu | grep "L3 cache" | sed 's/L3 cache:[[:space:]]*//')

    echo "Model: $CPU_MODEL"
    echo "Sockets: $CPU_SOCKETS"
    echo "Cores per socket: $CORES_PER_SOCKET"
    echo "Threads per core: $THREADS_PER_CORE"
    echo "Total logical CPUs: $CPU_CORES"
    [ -n "$CPU_MAX_MHZ" ] && echo "Max frequency: ${CPU_MAX_MHZ} MHz"
    [ -n "$CPU_MIN_MHZ" ] && echo "Min frequency: ${CPU_MIN_MHZ} MHz"
    echo "Cache:"
    [ -n "$L1D_CACHE" ] && echo "  L1d: $L1D_CACHE"
    [ -n "$L1I_CACHE" ] && echo "  L1i: $L1I_CACHE"
    [ -n "$L2_CACHE" ] && echo "  L2: $L2_CACHE"
    [ -n "$L3_CACHE" ] && echo "  L3: $L3_CACHE"
else
    grep "model name" /proc/cpuinfo | head -1 | sed 's/model name[[:space:]]*: /Model: /'
    echo "Cores: $(grep -c processor /proc/cpuinfo)"
fi
echo ""

# Memory Information
echo "## Memory"
if [ -f /proc/meminfo ]; then
    TOTAL_MEM_KB=$(grep MemTotal /proc/meminfo | awk '{print $2}')
    TOTAL_MEM_GB=$(echo "scale=2; $TOTAL_MEM_KB / 1024 / 1024" | bc)
    echo "Total RAM: ${TOTAL_MEM_GB} GB"
fi

# Try to get memory type/speed from dmidecode (requires root)
if command -v dmidecode &> /dev/null && [ "$EUID" -eq 0 ]; then
    MEM_TYPE=$(dmidecode -t memory 2>/dev/null | grep -m1 "Type:" | grep -v "Error" | awk '{print $2}')
    MEM_SPEED=$(dmidecode -t memory 2>/dev/null | grep -m1 "Speed:" | grep -v "Unknown" | awk '{print $2, $3}')
    [ -n "$MEM_TYPE" ] && echo "Type: $MEM_TYPE"
    [ -n "$MEM_SPEED" ] && echo "Speed: $MEM_SPEED"
fi
echo ""

# Storage Information
echo "## Storage"
if command -v lsblk &> /dev/null; then
    echo "Block devices:"
    lsblk -d -o NAME,SIZE,TYPE,MODEL,ROTA 2>/dev/null | head -10
    echo ""
    echo "(ROTA: 1=HDD, 0=SSD)"
fi
echo ""

# GPU Information (if present)
echo "## GPU"
GPU_FOUND=false
if command -v nvidia-smi &> /dev/null; then
    nvidia-smi --query-gpu=name,memory.total,driver_version --format=csv,noheader 2>/dev/null && GPU_FOUND=true
fi
if command -v lspci &> /dev/null; then
    VGA_INFO=$(lspci 2>/dev/null | grep -i "vga\|3d\|display" | sed 's/.*: //')
    if [ -n "$VGA_INFO" ]; then
        echo "$VGA_INFO"
        GPU_FOUND=true
    fi
fi
if [ "$GPU_FOUND" = false ]; then
    echo "No GPU detected or lspci not available"
fi
echo ""

# Virtualization
echo "## Virtualization"
if command -v systemd-detect-virt &> /dev/null; then
    VIRT=$(systemd-detect-virt)
    if [ "$VIRT" = "none" ]; then
        echo "Running on bare metal"
    else
        echo "Virtualization: $VIRT"
    fi
elif [ -f /proc/cpuinfo ] && grep -q "hypervisor" /proc/cpuinfo; then
    echo "Running in virtual machine"
else
    echo "Likely running on bare metal"
fi
echo ""

# Docker info (if relevant)
if command -v docker &> /dev/null; then
    echo "## Docker"
    docker --version 2>/dev/null
    echo ""
fi

# Date of capture
echo "## Benchmark Environment Captured"
echo "Date: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo "Hostname: $(hostname)"
echo ""

echo "=============================================="
echo "END OF SYSTEM SPECIFICATION"
echo "=============================================="
