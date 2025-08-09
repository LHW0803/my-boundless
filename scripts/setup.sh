#!/bin/bash

# =============================================================================
# Script Name: setup.sh
# Description:
#   - Updates the system packages.
#   - Installs essential boundless packages.
#   - Installs GPU drivers for provers.   <-- (요청에 따라 실제 설치는 스킵)
#   - Installs Docker with NVIDIA support.
#   - Installs Rust programming language.
#   - Installs CUDA Toolkit.              <-- (컨테이너 사용 전제: 스킵)
#   - Performs system cleanup.
#   - Verifies Docker with NVIDIA support.
#
# =============================================================================

set -euo pipefail

# =============================================================================
# Constants
# =============================================================================

SCRIPT_NAME="$(basename "$0")"
LOG_FILE="/var/log/${SCRIPT_NAME%.sh}.log"

# =============================================================================
# Functions
# =============================================================================

info()   { printf "\e[34m[INFO]\e[0m %s\n" "$1"; }
success(){ printf "\e[32m[SUCCESS]\e[0m %s\n" "$1"; }
error()  { printf "\e[31m[ERROR]\e[0m %s\n" "$1" >&2; }

is_package_installed() {
    dpkg -s "$1" &> /dev/null
}

check_os() {
    if [[ -f /etc/os-release ]]; then
        # shellcheck source=/dev/null
        . /etc/os-release
        if [[ "${ID,,}" != "ubuntu" ]]; then
            error "Unsupported operating system: $NAME. This script is intended for Ubuntu."
            exit 1
        elif [[ "${VERSION_ID,,}" != "22.04" && "${VERSION_ID,,}" != "20.04" ]]; then
            error "Unsupported operating system verion: $VERSION. This script is intended for Ubuntu 20.04 or 22.04."
            exit 1
        else
            info "Operating System: $PRETTY_NAME"
        fi
    else
        error "/etc/os-release not found. Unable to determine the operating system."
        exit 1
    fi
}

update_system() {
    info "Updating and upgrading the system packages..."
    {
        sudo apt update -y
        sudo apt upgrade -y
    } >> "$LOG_FILE" 2>&1
    success "System packages updated and upgraded successfully."
}

install_packages() {
    local packages=(
        nvtop
        ubuntu-drivers-common
        build-essential
        libssl-dev
        curl
        gnupg
        ca-certificates
        lsb-release
        jq
    )
    info "Installing essential packages: ${packages[*]}..."
    {
        sudo apt install -y "${packages[@]}"
    } >> "$LOG_FILE" 2>&1
    success "Essential packages installed successfully."
}

# ====== CHANGED: GPU 드라이버 설치 완전 스킵 ======
install_gpu_drivers() {
    info "Skipping GPU driver installation (per request). Assuming drivers are preinstalled and nvidia-smi works."
    return 0
}

install_rust() {
    if command -v rustc &> /dev/null; then
        info "Rust is already installed. Skipping Rust installation."
    else
        info "Installing Rust programming language..."
        {
            curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
        } >> "$LOG_FILE" 2>&1
        if [[ -f "$HOME/.cargo/env" ]]; then
            # shellcheck source=/dev/null
            source "$HOME/.cargo/env"
            success "Rust installed successfully."
        else
            error "Rust installation failed. ~/.cargo/env not found."
            exit 1
        fi
    fi
}

install_just() {
    if command -v just &>/dev/null; then
        info "'just' is already installed. Skipping."
        return
    fi
    info "Installing the 'just' command-runner…"
    {
        curl --proto '=https' --tlsv1.2 -sSf https://just.systems/install.sh \
        | sudo bash -s -- --to /usr/local/bin
    } >> "$LOG_FILE" 2>&1
    success "'just' installed successfully."
}

# ====== CHANGED: CUDA Toolkit 호스트 설치 스킵 ======
install_cuda() {
    info "Skipping host CUDA Toolkit installation (containers will provide CUDA)."
    return 0
}

install_docker() {
    if command -v docker &> /dev/null; then
        info "Docker is already installed. Skipping Docker installation."
    else
        info "Installing Docker..."
        {
            sudo apt install -y apt-transport-https ca-certificates curl gnupg-agent software-properties-common
            curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
            echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
            sudo apt update -y
            sudo apt install -y docker-ce docker-ce-cli containerd.io
            sudo systemctl enable docker
            sudo systemctl start docker
        } >> "$LOG_FILE" 2>&1
        success "Docker installed and started successfully."
    fi
}

add_user_to_docker_group() {
    local username
    username=$(logname 2>/dev/null || echo "$SUDO_USER")

    if id -nG "$username" | grep -qw "docker"; then
        info "User '$username' is already in the 'docker' group."
    else
        info "Adding user '$username' to the 'docker' group..."
        {
            sudo usermod -aG docker "$username"
        } >> "$LOG_FILE" 2>&1
        success "User '$username' added to the 'docker' group."
        info "To apply the new group membership, please log out and log back in."
    fi
}

# ====== CHANGED: 최신 nvidia-ctk 방식으로 설치 ======
install_nvidia_container_toolkit() {
    info "Checking NVIDIA Container Toolkit installation..."
    if is_package_installed "nvidia-container-toolkit"; then
        success "NVIDIA Container Toolkit is already installed."
        return
    fi

    info "Installing NVIDIA Container Toolkit (modern repo/keyring)…"
    {
        sudo mkdir -p /etc/apt/keyrings
        curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey \
          | sudo gpg --dearmor -o /etc/apt/keyrings/nvidia-container-toolkit.gpg
        distro=$(. /etc/os-release; echo ${ID}${VERSION_ID})
        curl -fsSL https://nvidia.github.io/libnvidia-container/${distro}/libnvidia-container.list \
          | sed 's#deb https://#deb [signed-by=/etc/apt/keyrings/nvidia-container-toolkit.gpg] https://#' \
          | sudo tee /etc/apt/sources.list.d/nvidia-container-toolkit.list >/dev/null

        sudo apt update -y
        sudo apt install -y nvidia-container-toolkit
    } >> "$LOG_FILE" 2>&1
    success "NVIDIA Container Toolkit installed successfully."
}

# ====== CHANGED: daemon.json 수동 편집 대신 nvidia-ctk ======
configure_docker_nvidia() {
    info "Configuring Docker to use NVIDIA runtime via nvidia-ctk..."
    {
        sudo nvidia-ctk runtime configure --runtime=docker
        sudo systemctl restart docker
    } >> "$LOG_FILE" 2>&1
    success "Docker configured to use NVIDIA runtime."
}

cleanup() {
    info "Cleaning up unnecessary packages..."
    {
        sudo apt autoremove -y
        sudo apt autoclean -y
    } >> "$LOG_FILE" 2>&1
    success "Cleanup completed."
}

init_git_submodules() {
    info "ensuring submodules are initialized..."
    {
        git submodule update --init --recursive
    } >> "$LOG_FILE" 2>&1
    success "git submodules initialized successfully"
}

# =============================================================================
# Main Script Execution
# =============================================================================

exec > >(tee -a "$LOG_FILE") 2>&1

info "===== Script Execution Started at $(date) ====="

check_os
init_git_submodules
update_system
install_packages
install_gpu_drivers          # 이제는 no-op(스킵)
install_docker
add_user_to_docker_group
install_nvidia_container_toolkit
configure_docker_nvidia
install_rust
install_just
install_cuda                 # 스킵
cleanup

success "All tasks completed successfully!"

if [ -t 0 ]; then
    read -rp "Do you want to reboot now to apply all changes? (y/N): " REBOOT
    case "$REBOOT" in
        [yY][eE][sS]|[yY])
            info "Rebooting the system..."
            reboot
            ;;
        *)
            info "Reboot skipped. Please consider rebooting your system to apply all changes."
            ;;
    esac
else
    info "Running in non-interactive mode. Skipping reboot prompt."
fi

info "===== Script Execution Ended at $(date) ====="
exit 0
