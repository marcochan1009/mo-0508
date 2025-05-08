#!/bin/bash

# ===== 配置 =====
# 自动生成随机用户名
TIMESTAMP=$(date +%s)
RANDOM_CHARS=$(cat /dev/urandom | tr -dc 'a-z0-9' | fold -w 4 | head -n 1)
EMAIL_USERNAME="momo${RANDOM_CHARS}${TIMESTAMP:(-4)}"
PROJECT_PREFIX="gemini-key"
TOTAL_PROJECTS=175  # 默认项目数 (可能会根据配额检查结果自动调整)
MAX_PARALLEL_JOBS=20  # 默认设置为20
MAX_RETRY_ATTEMPTS=3  # 重试次数
# 只保留纯密钥和逗号分隔密钥文件
PURE_KEY_FILE="key.txt"
COMMA_SEPARATED_KEY_FILE="comma_separated_keys_${EMAIL_USERNAME}.txt"
SECONDS=0
DELETION_LOG="project_deletion_$(date +%Y%m%d_%H%M%S).log"
CLEANUP_LOG="api_keys_cleanup_$(date +%Y%m%d_%H%M%S).log" # 日志文件名移到配置区
TEMP_DIR="/tmp/gcp_script_${TIMESTAMP}"
# ===== 配置结束 =====

# ===== 初始化 =====
# 创建临时目录
mkdir -p "$TEMP_DIR"

# 统一日志函数 (脚本内部使用)
_log_internal() {
  local level=$1
  local msg=$2
  local timestamp
  timestamp=$(date '+%Y-%m-%d %H:%M:%S')
  echo "[$timestamp] [$level] $msg"
}

_log_internal "INFO" "JSON 解析将仅使用备用方法 (sed/grep)。"
sleep 1
# ===== 初始化结束 =====

# ===== 工具函数 =====
# 统一日志函数 (对外暴露)
log() {
  local level=$1
  local msg=$2
  _log_internal "$level" "$msg"
}

# 解析JSON并提取字段（仅使用备用方法）
parse_json() {
  local json="$1"
  local field="$2"
  local value=""

  if [ -z "$json" ]; then return 1; fi

  case "$field" in
    ".keyString")
      value=$(echo "$json" | sed -n 's/.*"keyString": *"\([^"]*\)".*/\1/p')
      ;;
    ".[0].name")
      value=$(echo "$json" | sed -n 's/.*"name": *"\([^"]*\)".*/\1/p' | head -n 1)
      ;;
    *)
      local field_name=$(echo "$field" | tr -d '.["]')
      value=$(echo "$json" | grep -oP "(?<=\"$field_name\":\s*\")[^\"]*" 2>/dev/null)
      if [ -z "$value" ]; then
           value=$(echo "$json" | grep -oP "(?<=\"$field_name\":\s*)[^,\s\}]+" 2>/dev/null | head -n 1)
      fi
      ;;
  esac

  if [ -n "$value" ]; then
    echo "$value"
    return 0
  else
    log "ERROR" "parse_json: 备用方法未能提取有效值 '$field'"
    return 1
  fi
}

# 仅在成功时写入纯密钥文件的函数
write_keys_to_files() {
    local api_key="$1"

    if [ -z "$api_key" ]; then
        log "ERROR" "write_keys_to_files called with empty API key!"
        return
    fi

    # 使用文件锁确保写入原子性
    (
        flock 200
        # 写入纯密钥文件 (只有密钥，每行一个)
        echo "$api_key" >> "$PURE_KEY_FILE"
        # 写入逗号分隔文件 (只有密钥，用逗号分隔)
        if [[ -s "$COMMA_SEPARATED_KEY_FILE" ]]; then
            echo -n "," >> "$COMMA_SEPARATED_KEY_FILE"
        fi
        echo -n "$api_key" >> "$COMMA_SEPARATED_KEY_FILE"
    ) 200>"${TEMP_DIR}/key_files.lock" # 使用一个统一的锁文件
}


# 改进的指数退避重试函数
retry_with_backoff() {
  local max_attempts=$1
  local cmd=$2
  local attempt=1
  local timeout=5
  local error_log="${TEMP_DIR}/error_$(date +%s)_$RANDOM.log"

  while [ $attempt -le $max_attempts ]; do
    if bash -c "$cmd" 2>"$error_log"; then
      rm -f "$error_log"; return 0
    else
      local error_code=$?; local error_msg=$(cat "$error_log")
      log "INFO" "命令尝试 $attempt/$max_attempts 失败 (退出码: $error_code)，错误: ${error_msg:-'未知错误'}"
      if [[ "$error_msg" == *"Permission denied"* ]] || [[ "$error_msg" == *"Authentication failed"* ]]; then
          log "ERROR" "检测到权限或认证错误，停止重试。"; rm -f "$error_log"; return $error_code
      elif [[ "$error_msg" == *"Quota exceeded"* ]]; then
         log "WARN" "检测到配额错误，重试可能无效。"
      fi
      if [ $attempt -lt $max_attempts ]; then
          log "INFO" "等待 $timeout 秒后重试..."; sleep $timeout
          timeout=$((timeout * 2)); if [ $timeout -gt 60 ]; then timeout=60; fi
      fi; attempt=$((attempt + 1))
    fi
  done
  log "ERROR" "命令在 $max_attempts 次尝试后最终失败。"
  if [ -f "$error_log" ]; then local final_error=$(cat "$error_log"); log "ERROR" "最后一次错误信息: ${final_error:-'未知错误'}"; rm -f "$error_log"; fi
  return 1
}

# 进度条显示（优化版）
show_progress() {
    local completed=$1
    local total=$2
    if [ $total -le 0 ]; then printf "\r%-80s" " "; printf "\r[总数无效: %d]" "$total"; return; fi
    # 修复：确保 completed 不超过 total
    if [ $completed -gt $total ]; then completed=$total; fi

    local percent=$((completed * 100 / total))
    local completed_chars=$((percent * 50 / 100))
    if [ $completed_chars -lt 0 ]; then completed_chars=0; fi
    if [ $completed_chars -gt 50 ]; then completed_chars=50; fi
    local remaining_chars=$((50 - completed_chars))
    local progress_bar=$(printf "%${completed_chars}s" "" | tr ' ' '#')
    local remaining_bar=$(printf "%${remaining_chars}s" "")
    printf "\r%-80s" " "; printf "\r[%s%s] %d%% (%d/%d)" "$progress_bar" "$remaining_bar" "$percent" "$completed" "$total"
}


# 配额检查及调整（改进版）
check_quota() {
  log "INFO" "检查GCP项目创建配额..."
  local current_project=$(gcloud config get-value project 2>/dev/null)
  if [ -z "$current_project" ]; then log "WARN" "无法获取当前GCP项目ID，无法准确检查配额。将跳过配额检查。"; return 0; fi

  local projects_quota; local quota_cmd; local quota_output; local error_msg;
  quota_cmd="gcloud services quota list --service=cloudresourcemanager.googleapis.com --consumer=projects/$current_project --filter='metric=cloudresourcemanager.googleapis.com/project_create_requests' --format=json 2>${TEMP_DIR}/quota_error.log"
  if quota_output=$(retry_with_backoff 2 "$quota_cmd"); then
      projects_quota=$(echo "$quota_output" | grep -oP '(?<="effectiveLimit": ")[^"]+' | head -n 1)
  else
    log "INFO" "GA services quota list 命令失败，尝试 alpha services quota list..."
    quota_cmd="gcloud alpha services quota list --service=cloudresourcemanager.googleapis.com --consumer=projects/$current_project --filter='metric(cloudresourcemanager.googleapis.com/project_create_requests)' --format=json 2>${TEMP_DIR}/quota_error.log"
    if quota_output=$(retry_with_backoff 2 "$quota_cmd"); then
        projects_quota=$(echo "$quota_output" | grep -oP '(?<="INT64": ")[^"]+' | head -n 1)
    else
        error_msg=$(cat "${TEMP_DIR}/quota_error.log" 2>/dev/null); rm -f "${TEMP_DIR}/quota_error.log"
        log "WARN" "无法获取配额信息 (尝试GA和alpha命令均失败): ${error_msg:-'命令执行失败'}"; log "WARN" "将使用默认设置继续，但强烈建议手动检查配额，避免失败。"
        read -p "无法检查配额，是否继续执行? [y/N]: " continue_no_quota
        if [[ "$continue_no_quota" =~ ^[Yy]$ ]]; then return 0; else log "INFO" "操作已取消。"; return 1; fi
    fi
  fi; rm -f "${TEMP_DIR}/quota_error.log"

  if [ -z "$projects_quota" ] || ! [[ "$projects_quota" =~ ^[0-9]+$ ]]; then
    log "WARN" "无法从输出中准确提取项目创建配额值。将使用默认设置 ($TOTAL_PROJECTS) 继续。"; return 0;
  fi

  local quota_limit=$projects_quota; log "INFO" "检测到项目创建配额限制大约为: $quota_limit"
  if [ "$TOTAL_PROJECTS" -gt "$quota_limit" ]; then
    log "WARN" "计划创建的项目数($TOTAL_PROJECTS) 大于检测到的配额限制($quota_limit)"
    echo "选项:"; echo "1. 继续尝试创建 $TOTAL_PROJECTS 个项目 (很可能部分失败)"; echo "2. 调整为创建 $quota_limit 个项目 (更符合配额限制)"; echo "3. 取消操作"
    read -p "请选择 [1/2/3]: " quota_option
    case $quota_option in
      1) log "INFO" "将尝试创建 $TOTAL_PROJECTS 个项目，请注意配额限制。" ;;
      2) TOTAL_PROJECTS=$quota_limit; log "INFO" "已调整计划，将创建 $TOTAL_PROJECTS 个项目" ;;
      3|*) log "INFO" "操作已取消"; return 1 ;;
    esac
  else log "SUCCESS" "计划创建的项目数($TOTAL_PROJECTS) 在检测到的配额限制($quota_limit)之内。"; fi
  return 0
}

# Function to automatically download the comma-separated key file
auto_download_keys() {
  local file_to_download="$1"
  if [ -z "$file_to_download" ] || [ ! -f "$file_to_download" ]; then
    log "WARN" "auto_download_keys: 文件 '$file_to_download' 未找到或未指定。"
    return 1
  fi

  if command -v sz >/dev/null 2>&1; then
    log "INFO" "检测到 'sz' 命令，尝试自动下载 '$file_to_download'..."
    echo "请准备接收文件 '$file_to_download' (ZMODEM)... (如果您的终端不支持，此步骤将卡住或失败)"
    sleep 3 # Give a moment for the user to see the message and for terminal to prepare
    if sz "$file_to_download"; then
      log "SUCCESS" "文件 '$file_to_download' 已通过 sz 发送。"
      echo "如果您的终端支持ZMODEM，文件应该已开始下载。如果没有反应，请按 Ctrl+C 数次，然后手动下载。"
    else
      log "ERROR" "使用 'sz' 发送文件 '$file_to_download' 失败 (可能是终端不支持或sz命令问题)。"
      log "INFO" "请手动从服务器下载文件: $PWD/$file_to_download"
    fi
  else
    log "INFO" "未检测到 'sz' 命令。无法自动下载文件。"
    log "INFO" "请手动从服务器下载文件: $PWD/$file_to_download"
  fi
}

# 生成报告
generate_report() {
  local success=$1
  local failed=$2
  local total=$3
  local success_rate=0
  if [ $total -gt 0 ]; then success_rate=$(echo "scale=2; $success * 100 / $total" | bc); fi
  local duration=$SECONDS
  local minutes=$((duration / 60))
  local seconds_rem=$((duration % 60))
  echo ""; echo "========== 执行报告 =========="
  echo "总计尝试: $total 个项目"
  echo "成功获取密钥: $success 个"
  echo "失败: $failed 个"
  echo "成功率: $success_rate%"
  if [ $success -gt 0 ]; then local avg_time=$((duration / success)); echo "平均处理时间 (成功项目): $avg_time 秒/项目"; elif [ $total -gt 0 ]; then echo "平均处理时间: N/A (无成功项目)"; fi
  echo "总执行时间: $minutes 分 $seconds_rem 秒"
  echo "API密钥已保存至:"
  # 只报告两个纯密钥文件
  echo "- 纯API密钥 (每行一个): $PURE_KEY_FILE"
  echo "- 逗号分隔密钥 (单行): $COMMA_SEPARATED_KEY_FILE"
  echo "=========================="

  if [ $success -gt 0 ] && [ -s "$COMMA_SEPARATED_KEY_FILE" ]; then
    auto_download_keys "$COMMA_SEPARATED_KEY_FILE"
  elif [ $success -gt 0 ]; then
    log "WARN" "有 $success 个成功密钥，但逗号分隔文件 '$COMMA_SEPARATED_KEY_FILE' 为空或未找到，跳过自动下载。"
  fi
}

# 项目处理函数
process_project() {
  local project_id="$1"
  local project_num="$2"
  local total="$3"
  local error_log="${TEMP_DIR}/project_${project_id}_error.log"; rm -f "$error_log"

  log "INFO" ">>> [$project_num/$total] 开始处理项目: $project_id"

  # --- 1. 创建项目 ---
  log "INFO" "[$project_num] 1/3 创建项目: $project_id ..."
  if ! retry_with_backoff $MAX_RETRY_ATTEMPTS "gcloud projects create \"$project_id\" --name=\"$project_id\" --no-set-as-default --quiet 2> \"$error_log\""; then
    local creation_error=$(cat "$error_log" 2>/dev/null); rm -f "$error_log"
    log "ERROR" "[$project_num] 错误: 无法创建项目 $project_id: ${creation_error:-未知错误}"
    log "INFO" "<<< [$project_num] 项目 $project_id 处理失败 (创建)"; return 1
  fi; log "INFO" "[$project_num] 项目 $project_id 创建请求已发送成功"; rm -f "$error_log"

  # 等待项目创建完成传播
  local wait_time=10; log "INFO" "[$project_num] 等待项目创建操作完成传播 (${wait_time}秒)..."; sleep $wait_time

  # --- 2. 启用 API ---
  log "INFO" "[$project_num] 2/3 启用 Generative Language API..."; rm -f "$error_log"
  if ! retry_with_backoff $MAX_RETRY_ATTEMPTS "gcloud services enable generativelanguage.googleapis.com --project=\"$project_id\" --quiet 2> \"$error_log\""; then
    local enable_error=$(cat "$error_log" 2>/dev/null); rm -f "$error_log"
    log "ERROR" "[$project_num] 错误: 无法为项目 $project_id 启用 API: ${enable_error:-未知错误}"
    log "INFO" "<<< [$project_num] 项目 $project_id 处理失败 (API启用)"; return 1
  fi; log "INFO" "[$project_num] API 已为 $project_id 成功启用"; rm -f "$error_log"

  # --- 3. 创建密钥并提取 ---
  log "INFO" "[$project_num] 3/3 创建 API 密钥..."; local create_output; rm -f "$error_log"
  if ! create_output=$(retry_with_backoff $MAX_RETRY_ATTEMPTS "gcloud services api-keys create --project=\"$project_id\" --display-name=\"Gemini API Key for $project_id\" --format=\"json\" --quiet 2> \"$error_log\""); then
      local key_create_error=$(cat "$error_log" 2>/dev/null); rm -f "$error_log"
      log "ERROR" "[$project_num] 错误: 无法为 $project_id 创建 API 密钥: ${key_create_error:-未知错误}"
      log "INFO" "<<< [$project_num] 项目 $project_id 处理失败 (密钥创建)"; return 1
  fi; rm -f "$error_log"

  if [ -n "$create_output" ]; then
    local api_key=""; api_key=$(parse_json "$create_output" ".keyString")
    if [ -n "$api_key" ]; then
      log "SUCCESS" "[$project_num] 成功: 为 $project_id 提取到 API 密钥"
      # 仅在成功时写入纯密钥文件
      write_keys_to_files "$api_key"
      log "INFO" "<<< [$project_num] 项目 $project_id 处理成功"; return 0
    else
      log "ERROR" "[$project_num] 错误: 成功创建密钥但无法从输出中提取 keyString (使用备用方法)"
      log "INFO" "<<< [$project_num] 项目 $project_id 处理失败 (密钥提取)"; return 1
    fi
  else
    log "ERROR" "[$project_num] 错误: 创建密钥命令成功执行，但没有输出"
    log "INFO" "<<< [$project_num] 项目 $project_id 处理失败 (密钥创建输出为空)"; return 1
  fi
}

# 删除单个项目函数
delete_project() {
  local project_id="$1"
  local project_num="$2"
  local total="$3"
  local error_log="${TEMP_DIR}/delete_${project_id}_error.log"; rm -f "$error_log"

  log "INFO" ">>> [$project_num/$total] 删除项目: $project_id"
  if gcloud projects delete "$project_id" --quiet 2>"$error_log"; then
    log "SUCCESS" "<<< [$project_num/$total] 成功删除项目: $project_id"
    ( flock 201; echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$project_num/$total] 已删除: $project_id" >> "$DELETION_LOG"; ) 201>"${TEMP_DIR}/${DELETION_LOG%.log}.lock"
    rm -f "$error_log"; return 0
  else
    local error_msg=$(cat "$error_log" 2>/dev/null); rm -f "$error_log"
    log "ERROR" "<<< [$project_num/$total] 删除项目失败: $project_id: ${error_msg:-'未知错误'}"
    ( flock 201; echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$project_num/$total] 删除失败: $project_id - ${error_msg:-'未知错误'}" >> "$DELETION_LOG"; ) 201>"${TEMP_DIR}/${DELETION_LOG%.log}.lock"
    return 1
  fi
}

# 提取现有项目API密钥
extract_key_from_project() {
  local project_id="$1"
  local project_num="$2"
  local total="$3"
  local error_log="${TEMP_DIR}/extract_${project_id}_error.log"; rm -f "$error_log"

  log "INFO" ">>> [$project_num/$total] 为项目获取API密钥: $project_id"

  # --- 1. 启用 API ---
  log "INFO" "[$project_num] 1/2 确保 Generative Language API 已启用..."; rm -f "$error_log"
  if ! retry_with_backoff $MAX_RETRY_ATTEMPTS "gcloud services enable generativelanguage.googleapis.com --project=\"$project_id\" --quiet 2> \"$error_log\""; then
    local enable_error=$(cat "$error_log" 2>/dev/null); rm -f "$error_log"
    log "ERROR" "[$project_num] 错误: 无法为项目 $project_id 启用 API: ${enable_error:-未知错误}"
    log "INFO" "<<< [$project_num] 项目 $project_id 处理失败 (API启用)"; return 1
  fi; log "INFO" "[$project_num] API 已为 $project_id 成功启用"; rm -f "$error_log"

  # --- 2. 获取或创建 API 密钥 ---
  log "INFO" "[$project_num] 2/2 获取或创建 API 密钥..."; local existing_keys_json; rm -f "$error_log"
  existing_keys_json=$(gcloud services api-keys list --project="$project_id" --format="json" 2>"$error_log")
  local list_ec=$?; local list_error_msg=$(cat "$error_log" 2>/dev/null); rm -f "$error_log"

  if [ $list_ec -eq 0 ] && [ -n "$existing_keys_json" ] && [ "$existing_keys_json" != "[]" ]; then
    local key_name=""; key_name=$(parse_json "$existing_keys_json" ".[0].name")
    if [ -n "$key_name" ]; then
      log "INFO" "[$project_num] 找到现有密钥名称: $key_name"; rm -f "$error_log"
      local key_details=$(gcloud services api-keys get-key-string "$key_name" --format="json" 2>"$error_log")
      local get_key_ec=$?; local get_key_error=$(cat "$error_log" 2>/dev/null); rm -f "$error_log"
      if [ $get_key_ec -eq 0 ] && [ -n "$key_details" ]; then
        local api_key=$(parse_json "$key_details" ".keyString")
        if [ -n "$api_key" ]; then
          log "SUCCESS" "[$project_num] 成功: 为 $project_id 获取到现有 API 密钥"
          write_keys_to_files "$api_key"
          log "INFO" "<<< [$project_num] 项目 $project_id 处理成功 (使用现有密钥)"; return 0
        else log "WARN" "[$project_num] 警告: 找到密钥 $key_name 但无法提取 keyString: ${get_key_error}"; fi
      else log "WARN" "[$project_num] 警告: 无法获取密钥 $key_name 的详细信息: ${get_key_error}"; fi
    else log "INFO" "[$project_num] 未能从列表输出中解析到现有密钥名称。"; fi
  else log "INFO" "[$project_num] 未找到现有API密钥或列出密钥时出错: ${list_error_msg}"; fi

  # --- 如果没有现有密钥或无法获取，创建新密钥 ---
  log "INFO" "[$project_num] 尝试创建新密钥..."; local create_output; rm -f "$error_log"
  if ! create_output=$(retry_with_backoff $MAX_RETRY_ATTEMPTS "gcloud services api-keys create --project=\"$project_id\" --display-name=\"Gemini API Key for $project_id (new)\" --format=\"json\" --quiet 2> \"$error_log\""); then
      local create_error=$(cat "$error_log" 2>/dev/null); rm -f "$error_log"
      log "ERROR" "[$project_num] 错误: 无法为 $project_id 创建新 API 密钥: ${create_error:-未知错误}"
      log "INFO" "<<< [$project_num] 项目 $project_id 处理失败 (新密钥创建)"; return 1
  fi; rm -f "$error_log"

  if [ -n "$create_output" ]; then
    local api_key=$(parse_json "$create_output" ".keyString")
    if [ -n "$api_key" ]; then
      log "SUCCESS" "[$project_num] 成功: 为 $project_id 创建并提取到新 API 密钥"
      write_keys_to_files "$api_key"
      log "INFO" "<<< [$project_num] 项目 $project_id 处理成功 (创建新密钥)"; return 0
    else
      log "ERROR" "[$project_num] 错误: 成功创建新密钥但无法从输出中提取 keyString (使用备用方法)"
      log "INFO" "<<< [$project_num] 项目 $project_id 处理失败 (新密钥提取)"; return 1
    fi
  else
    log "ERROR" "[$project_num] 错误: 创建新密钥命令成功执行，但没有输出"
    log "INFO" "<<< [$project_num] 项目 $project_id 处理失败 (新密钥创建输出为空)"; return 1
  fi
}

# 清理项目中的API密钥
cleanup_api_keys() {
  local project_id="$1"
  local project_num="$2"
  local total="$3"
  local error_log="${TEMP_DIR}/cleanup_task_${project_id}_error.log"; rm -f "$error_log" # Renamed to avoid conflict with wrapper log

  log "INFO" ">>> [$project_num/$total] 清理项目API密钥: $project_id"
  local existing_keys_json; existing_keys_json=$(gcloud services api-keys list --project="$project_id" --format="json" 2>"$error_log")
  local list_ec=$?; local list_error_msg=$(cat "$error_log" 2>/dev/null); rm -f "$error_log"

  if [ $list_ec -ne 0 ] || [ -z "$existing_keys_json" ] || [ "$existing_keys_json" = "[]" ]; then
    log "INFO" "[$project_num] 信息: 项目 $project_id 没有API密钥或无法获取密钥列表: ${list_error_msg:-'没有密钥或列表为空'}"; return 0
  fi

  local key_count=0; local deleted_count=0; local key_names=()
  mapfile -t key_names < <(echo "$existing_keys_json" | grep '"name":' | sed -n 's/.*"name": *"\([^"]*\)".*/\1/p')
  key_count=${#key_names[@]}; log "INFO" "[$project_num] 使用备用方法找到 $key_count 个密钥名称"

  if [ $key_count -eq 0 ]; then log "INFO" "[$project_num] 未能解析到任何密钥名称进行删除。"; return 0; fi

  local delete_failed=false
  for key_name in "${key_names[@]}"; do
    if [ -z "$key_name" ]; then continue; fi
    log "INFO" "[$project_num] 尝试删除API密钥: $key_name"; rm -f "$error_log"
    if gcloud services api-keys delete "$key_name" --quiet 2>>"$error_log"; then
      log "INFO" "[$project_num] 成功删除密钥: $key_name"; ((deleted_count++))
    else
      local delete_error=$(cat "$error_log" 2>/dev/null); log "WARN" "[$project_num] 删除密钥 $key_name 失败: ${delete_error}"; delete_failed=true
    fi; rm -f "$error_log"; sleep 0.1
  done

  log "INFO" "<<< [$project_num/$total] 项目 $project_id 的API密钥清理完成 (尝试删除 $key_count 个, 成功 $deleted_count 个)"
  if $delete_failed; then log "WARN" "[$project_num] 清理过程中至少有一个密钥删除失败。"; return 1; fi
  return 0
}

# Wrapper for cleanup_api_keys to handle detailed logging for run_parallel
wrapper_cleanup_api_keys() {
    local project_id="$1"
    local project_num="$2"
    local total_items="$3"
    # CLEANUP_LOG and TEMP_DIR need to be available (exported)
    local proj_log_file="${TEMP_DIR}/cleanup_wrapper_${project_id}.log" # Per-project log from wrapper
    
    # Execute the original function, capturing its output
    cleanup_api_keys "$project_id" "$project_num" "$total_items" > "$proj_log_file" 2>&1
    local func_ec=$?

    # Append to the main log file with locking
    (
        flock 202 # Use a specific lock fd for the main cleanup log
        echo -e "\n===== Log for project $project_id (TASK $project_num/$total_items, Exit Code: $func_ec) =====" >> "$CLEANUP_LOG"
        cat "$proj_log_file" >> "$CLEANUP_LOG"
        echo "===== End log for project $project_id =====" >> "$CLEANUP_LOG"
        rm -f "$proj_log_file" # Clean up per-project temp log
    ) 202>"${TEMP_DIR}/cleanup_main_log.lock"
    
    return $func_ec
}

# 资源清理函数
cleanup_resources() {
  log "INFO" "执行退出清理..."
  if [ -d "$TEMP_DIR" ]; then log "INFO" "删除临时目录: $TEMP_DIR"; rm -rf "$TEMP_DIR"; fi
  log "INFO" "资源清理完成"
}
# ===== 工具函数结束 =====

# ===== 功能模块 =====
# 修复并行处理中的计数和进度条显示问题
run_parallel() {
    local task_func="$1" # 要执行的函数名 (e.g., delete_project, process_project)
    local items=("${@:2}") # 要处理的项目列表
    local total_items=${#items[@]}
    local description="$task_func" # 简单的描述

    if [ $total_items -eq 0 ]; then
        log "INFO" "没有项目需要 $description。"
        echo "0 0" # success_count fail_count
        return 0
    fi

    local active_jobs=0
    local completed_count=0
    local success_count=0
    local fail_count=0
    local pids=()

    log "INFO" "开始并行执行 '$description' (最多 $MAX_PARALLEL_JOBS 个并行)..."

    for i in "${!items[@]}"; do
        local item="${items[i]}"
        local item_num=$((i + 1))

        # 在后台启动任务函数
        "$task_func" "$item" "$item_num" "$total_items" &
        pids+=($!)
        ((active_jobs++))

        if [[ "$active_jobs" -ge "$MAX_PARALLEL_JOBS" ]]; then
            wait -n
            local exit_status=$?
            ((completed_count++))
            if [ $exit_status -eq 0 ]; then
                ((success_count++))
            else
                ((fail_count++))
            fi
            ((active_jobs--))
            show_progress $completed_count $total_items
            echo -n " $description 中 (S:$success_count F:$fail_count T:$total_items)..."
        fi
        sleep 0.1
    done

    log "INFO" "" 
    log "INFO" "所有 $total_items 个 '$description' 任务已启动, 等待剩余 $active_jobs 个任务完成..."
    while [ $active_jobs -gt 0 ]; do
        wait -n
        local exit_status=$?
        ((completed_count++))
         if [ $exit_status -eq 0 ]; then
            ((success_count++))
        else
            ((fail_count++))
        fi
        ((active_jobs--))
        show_progress $completed_count $total_items
        echo -n " 完成 $description (S:$success_count F:$fail_count T:$total_items)..."
    done
    wait 

    log "INFO" "" 
    log "INFO" "所有 '$description' 任务已执行完毕"
    log "INFO" "======================================================"
    
    echo "$success_count $fail_count" # Output counts for caller
    if [ $fail_count -gt 0 ]; then return 1; else return 0; fi
}

# 功能1：新建项目并获取密钥 (原功能2)
create_projects_and_get_keys() {
  SECONDS=0
  log "INFO" "======================================================"; log "INFO" "功能1: 新建项目并获取API密钥"; log "INFO" "======================================================"
  if ! check_quota; then return 1; fi
  if [ $TOTAL_PROJECTS -le 0 ]; then log "WARN" "调整后的计划创建项目数为 0 或无效，操作结束。"; return 0; fi
  log "INFO" "将使用随机生成的用户名: ${EMAIL_USERNAME}"; log "INFO" "项目前缀: ${PROJECT_PREFIX}"; log "INFO" "即将开始创建 $TOTAL_PROJECTS 个新项目..."; log "INFO" "脚本将在 5 秒后开始执行..."; sleep 5
  > "$PURE_KEY_FILE"; > "$COMMA_SEPARATED_KEY_FILE"

  export -f _log_internal log process_project retry_with_backoff write_keys_to_files parse_json show_progress run_parallel
  export PURE_KEY_FILE COMMA_SEPARATED_KEY_FILE TEMP_DIR MAX_RETRY_ATTEMPTS MAX_PARALLEL_JOBS

  local projects_to_create=()
  for i in $(seq 1 $TOTAL_PROJECTS); do
    project_num=$(printf "%03d" $i); local base_id="${PROJECT_PREFIX}-${EMAIL_USERNAME}-${project_num}"; project_id=$(echo "$base_id" | tr -cd 'a-z0-9-' | cut -c 1-30 | sed 's/-$//'); if ! [[ "$project_id" =~ ^[a-z] ]]; then project_id="g${project_id:1}"; project_id=$(echo "$project_id" | cut -c 1-30 | sed 's/-$//'); fi
    projects_to_create+=("$project_id")
  done

  local counts_output
  local create_status
  if counts_output=$(run_parallel process_project "${projects_to_create[@]}"); then
    create_status=0
  else
    create_status=1
  fi
  local successful_keys fail_count
  read -r successful_keys fail_count <<< "$counts_output"
  # successful_keys from run_parallel is count of successful process_project calls.
  # This should align with keys written if process_project only returns 0 on full success.

  generate_report "$successful_keys" "$fail_count" "$TOTAL_PROJECTS"
  log "INFO" "======================================================"; log "INFO" "请检查文件 '$PURE_KEY_FILE' 和 '$COMMA_SEPARATED_KEY_FILE' 中的内容"
   if [ "$fail_count" -gt 0 ]; then log "WARN" "有 $fail_count 个项目处理失败，请检查控制台输出日志。"; log "WARN" "失败原因可能是触发了 GCP 配额限制、API 错误、权限问题或项目ID命名冲突。"; fi
  log "INFO" "提醒：项目需要关联有效的结算账号才能实际使用 API 密钥"; log "INFO" "======================================================"
  return $create_status
}

# 功能2：获取现有项目的API密钥 (原功能3)
get_keys_from_existing_projects() {
  SECONDS=0
  log "INFO" "======================================================"; log "INFO" "功能2: 获取现有项目的API密钥"; log "INFO" "======================================================"
  log "INFO" "正在获取项目列表..."; local list_error="${TEMP_DIR}/list_projects_error.log"; local ALL_PROJECTS=($(gcloud projects list --format="value(projectId)" --filter="projectId!~^sys-" --quiet 2>"$list_error")); local list_ec=$?; rm -f "$list_error"
  if [ $list_ec -ne 0 ]; then local error_msg=$(cat "$list_error" 2>/dev/null); log "ERROR" "无法获取项目列表: ${error_msg:-'gcloud命令失败'}"; return 1; fi
  if [ ${#ALL_PROJECTS[@]} -eq 0 ]; then log "INFO" "未找到任何用户项目，无法获取API密钥"; return 0; fi
  local total_to_get=${#ALL_PROJECTS[@]}
  log "INFO" "找到 $total_to_get 个用户项目"; echo "前5个项目示例："; for ((i=0; i<5 && i<${#ALL_PROJECTS[@]}; i++)); do printf " - %s\n" "${ALL_PROJECTS[i]}"; done; if [ ${#ALL_PROJECTS[@]} -gt 5 ]; then echo " - ... 以及其他 $((${#ALL_PROJECTS[@]} - 5)) 个项目"; fi
  read -p "确认要为这 $total_to_get 个项目获取或创建API密钥吗？[y/N]: " confirm; if [[ ! "$confirm" =~ ^[Yy]$ ]]; then log "INFO" "操作已取消，返回主菜单"; return 1; fi
  > "$PURE_KEY_FILE"; > "$COMMA_SEPARATED_KEY_FILE"

  export -f _log_internal log extract_key_from_project retry_with_backoff write_keys_to_files parse_json show_progress run_parallel
  export PURE_KEY_FILE COMMA_SEPARATED_KEY_FILE TEMP_DIR MAX_RETRY_ATTEMPTS MAX_PARALLEL_JOBS

  local counts_output
  local get_status
  if counts_output=$(run_parallel extract_key_from_project "${ALL_PROJECTS[@]}"); then
    get_status=0
  else
    get_status=1
  fi
  local successful_keys fail_count
  read -r successful_keys fail_count <<< "$counts_output"

  generate_report "$successful_keys" "$fail_count" "$total_to_get"
  log "INFO" "======================================================"; log "INFO" "请检查文件 '$PURE_KEY_FILE' 和 '$COMMA_SEPARATED_KEY_FILE' 中的内容"
   if [ "$fail_count" -gt 0 ]; then log "WARN" "有 $fail_count 个项目处理失败，请检查控制台输出日志。"; fi
  log "INFO" "======================================================"
  return $get_status
}

# 功能3：删除所有现有项目 (原功能4)
delete_all_existing_projects() {
  SECONDS=0
  log "INFO" "======================================================"; log "INFO" "功能3: 删除所有现有项目"; log "INFO" "======================================================"
  log "INFO" "正在获取项目列表..."; local list_error="${TEMP_DIR}/list_projects_error.log"; local ALL_PROJECTS=($(gcloud projects list --format="value(projectId)" --filter="projectId!~^sys-" --quiet 2>"$list_error")); local list_ec=$?; rm -f "$list_error"
  if [ $list_ec -ne 0 ]; then local error_msg=$(cat "$list_error" 2>/dev/null); log "ERROR" "无法获取项目列表: ${error_msg:-'gcloud命令失败'}"; return 1; fi
  if [ ${#ALL_PROJECTS[@]} -eq 0 ]; then log "INFO" "未找到任何用户项目，无需删除"; return 0; fi
  local total_to_delete=${#ALL_PROJECTS[@]}
  log "INFO" "找到 $total_to_delete 个用户项目需要删除"; echo "前5个项目示例："; for ((i=0; i<5 && i<${#ALL_PROJECTS[@]}; i++)); do printf " - %s\n" "${ALL_PROJECTS[i]}"; done; if [ ${#ALL_PROJECTS[@]} -gt 5 ]; then echo " - ... 以及其他 $((${#ALL_PROJECTS[@]} - 5)) 个项目"; fi
  read -p "!!! 危险操作 !!! 确认要删除所有 $total_to_delete 个项目吗？此操作不可撤销！(输入 'DELETE-ALL' 确认): " confirm; if [ "$confirm" != "DELETE-ALL" ]; then log "INFO" "删除操作已取消，返回主菜单"; return 1; fi
  echo "项目删除日志 ($(date +%Y-%m-%d_%H:%M:%S))" > "$DELETION_LOG"; echo "------------------------------------" >> "$DELETION_LOG"
  
  export -f _log_internal log delete_project parse_json show_progress retry_with_backoff run_parallel
  export DELETION_LOG TEMP_DIR MAX_PARALLEL_JOBS MAX_RETRY_ATTEMPTS

  local counts_output
  local delete_status
  if counts_output=$(run_parallel delete_project "${ALL_PROJECTS[@]}"); then
    delete_status=0
  else
    delete_status=1
  fi
  local successful_deletions failed_deletions
  read -r successful_deletions failed_deletions <<< "$counts_output"
  
  local duration=$SECONDS; local minutes=$((duration / 60)); local seconds_rem=$((duration % 60))
  echo ""; echo "========== 删除报告 =========="; echo "总计尝试删除: $total_to_delete 个项目"; echo "成功删除: $successful_deletions 个项目"; echo "删除失败: $failed_deletions 个项目"; echo "总执行时间: $minutes 分 $seconds_rem 秒"; echo "详细日志已保存至: $DELETION_LOG"; echo "=========================="
  return $delete_status
}

# 功能4：清理项目API密钥（不删除项目）(原功能5)
cleanup_project_api_keys() {
  SECONDS=0
  log "INFO" "======================================================"; log "INFO" "功能4: 清理项目API密钥（不删除项目）"; log "INFO" "======================================================"
  log "INFO" "正在获取项目列表..."; local list_error="${TEMP_DIR}/list_projects_error.log"; local ALL_PROJECTS=($(gcloud projects list --format="value(projectId)" --filter="projectId!~^sys-" --quiet 2>"$list_error")); local list_ec=$?; rm -f "$list_error"
  if [ $list_ec -ne 0 ]; then local error_msg=$(cat "$list_error" 2>/dev/null); log "ERROR" "无法获取项目列表: ${error_msg:-'gcloud命令失败'}"; return 1; fi
  if [ ${#ALL_PROJECTS[@]} -eq 0 ]; then log "INFO" "未找到任何用户项目，无法清理API密钥"; return 0; fi
  local total_to_cleanup=${#ALL_PROJECTS[@]}
  log "INFO" "找到 $total_to_cleanup 个用户项目"; echo "前5个项目示例："; for ((i=0; i<5 && i<${#ALL_PROJECTS[@]}; i++)); do printf " - %s\n" "${ALL_PROJECTS[i]}"; done; if [ ${#ALL_PROJECTS[@]} -gt 5 ]; then echo " - ... 以及其他 $((${#ALL_PROJECTS[@]} - 5)) 个项目"; fi
  read -p "确认要删除这 $total_to_cleanup 个项目中的所有API密钥吗？[y/N]: " confirm; if [[ ! "$confirm" =~ ^[Yy]$ ]]; then log "INFO" "操作已取消，返回主菜单"; return 1; fi
  echo "API密钥清理日志 ($(date +%Y-%m-%d_%H:%M:%S))" > "$CLEANUP_LOG"; echo "------------------------------------" >> "$CLEANUP_LOG"; log "INFO" "详细清理日志将记录在: $CLEANUP_LOG"
  
  export -f _log_internal log wrapper_cleanup_api_keys cleanup_api_keys parse_json show_progress retry_with_backoff run_parallel
  export TEMP_DIR MAX_PARALLEL_JOBS CLEANUP_LOG MAX_RETRY_ATTEMPTS

  local counts_output
  local cleanup_status
  if counts_output=$(run_parallel wrapper_cleanup_api_keys "${ALL_PROJECTS[@]}"); then
    cleanup_status=0
  else
    cleanup_status=1
  fi
  local cleanup_success_count cleanup_fail_count
  read -r cleanup_success_count cleanup_fail_count <<< "$counts_output"

  log "INFO" ""; log "INFO" "所有任务已执行完毕"; log "INFO" "======================================================"
  local duration=$SECONDS; local minutes=$((duration / 60)); local seconds_rem=$((duration % 60))
  echo ""; echo "========== API密钥清理报告 =========="; echo "总计处理: $total_to_cleanup 个项目"; echo "清理函数成功执行: $cleanup_success_count 个"; echo "清理函数执行失败: $cleanup_fail_count 个"; echo "总执行时间: $minutes 分 $seconds_rem 秒"; echo "详细清理日志已保存至: $CLEANUP_LOG"; echo "(注意：成功执行不代表一定删除了密钥，可能项目原本就没有密钥。失败则表示清理函数本身报错。)"; echo "=========================="
  return $cleanup_status
}

# 显示主菜单
show_menu() {
  clear
  echo "======================================================"
  echo "     GCP Gemini API 密钥懒人管理工具 v2.7 " 
  echo "======================================================"
  local current_account; current_account=$(gcloud auth list --filter=status:ACTIVE --format="value(account)" 2>/dev/null | head -n 1); if [ -z "$current_account" ]; then current_account="无法获取 (gcloud auth list 失败?)"; fi
  local current_project; current_project=$(gcloud config get-value project 2>/dev/null); if [ -z "$current_project" ]; then current_project="未设置 (gcloud config get-value project 失败?)"; fi
  echo "当前账号: $current_account"; echo "当前项目: $current_project"; echo "并行任务数: $MAX_PARALLEL_JOBS"; echo "重试次数: $MAX_RETRY_ATTEMPTS"
  echo "JSON解析: 仅使用备用方法 (sed/grep)"
  echo ""; echo "请选择功能:"; 
  echo "1. 一键新建项目并获取API密钥"
  echo "2. 一键获取现有项目的API密钥"
  echo "3. 一键删除所有现有项目 (删除项目后 要等30天以上 才算彻底删除了)"
  echo "4. 清理项目API密钥（不删除项目）"
  echo "5. 修改配置参数"
  echo "0. 退出"; echo "======================================================"
  read -p "请输入选项 [0-5]: " choice

  case $choice in
    1) create_projects_and_get_keys ;; 
    2) get_keys_from_existing_projects ;; 
    3) delete_all_existing_projects ;; 
    4) cleanup_project_api_keys ;; 
    5) configure_settings ;;
    0) log "INFO" "正在退出..."; exit 0 ;; 
    *) echo "无效选项 '$choice'，请重新选择。"; sleep 2 ;;
  esac
  if [[ "$choice" =~ ^[1-5]$ ]]; then echo ""; read -p "按回车键返回主菜单..."; fi
}

# 配置设置
configure_settings() {
  local setting_changed=false
  while true; do
      clear; echo "======================================================"; echo "配置参数"; echo "======================================================"
      echo "当前设置:"; echo "1. 项目前缀 (用于新建项目): $PROJECT_PREFIX"; echo "2. 计划创建的项目数量 (用于功能1): $TOTAL_PROJECTS"; echo "3. 最大并行任务数: $MAX_PARALLEL_JOBS"; echo "4. 最大重试次数 (用于API调用): $MAX_RETRY_ATTEMPTS"; echo "0. 返回主菜单"; echo "======================================================"
      read -p "请选择要修改的设置 [0-4]: " setting_choice
      case $setting_choice in
        1) read -p "请输入新的项目前缀 (留空取消): " new_prefix; if [ -n "$new_prefix" ]; then if [[ "$new_prefix" =~ ^[a-z][a-z0-9-]{0,19}$ ]]; then PROJECT_PREFIX="$new_prefix"; log "INFO" "项目前缀已更新为: $PROJECT_PREFIX"; setting_changed=true; else echo "错误：前缀必须以小写字母开头，只能包含小写字母、数字和连字符，长度1-20。"; sleep 2; fi; fi ;;
        2) read -p "请输入计划创建的项目数量 (留空取消): " new_total; if [[ "$new_total" =~ ^[1-9][0-9]*$ ]]; then TOTAL_PROJECTS=$new_total; log "INFO" "计划创建的项目数量已更新为: $TOTAL_PROJECTS"; setting_changed=true; elif [ -n "$new_total" ]; then echo "错误：请输入一个大于0的整数。"; sleep 2; fi ;;
        3) read -p "请输入最大并行任务数 (建议 5-50，留空取消): " new_parallel; if [[ "$new_parallel" =~ ^[1-9][0-9]*$ ]]; then MAX_PARALLEL_JOBS=$new_parallel; log "INFO" "最大并行任务数已更新为: $MAX_PARALLEL_JOBS"; setting_changed=true; elif [ -n "$new_parallel" ]; then echo "错误：请输入一个大于0的整数。"; sleep 2; fi ;;
        4) read -p "请输入最大重试次数 (建议 1-5，留空取消): " new_retries; if [[ "$new_retries" =~ ^[1-9][0-9]*$ ]]; then MAX_RETRY_ATTEMPTS=$new_retries; log "INFO" "最大重试次数已更新为: $MAX_RETRY_ATTEMPTS"; setting_changed=true; elif [ -n "$new_retries" ]; then echo "错误：请输入一个大于等于1的整数。"; sleep 2; fi ;;
        0) return ;; *) echo "无效选项 '$setting_choice'，请重新选择。"; sleep 2 ;;
      esac; if $setting_changed; then sleep 1; setting_changed=false; fi
  done
}

# ===== 主程序 =====
# 设置退出处理函数
trap cleanup_resources EXIT SIGINT SIGTERM

# --- 登录和项目检查 ---
log "INFO" "检查 GCP 登录状态..."; if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" >/dev/null 2>&1; then log "WARN" "无法获取活动账号信息，或者尚未登录。请尝试登录:"; if ! gcloud auth login; then log "ERROR" "登录失败。请确保您可以通过 'gcloud auth login' 成功登录后再运行脚本。"; exit 1; fi; log "INFO" "再次检查 GCP 登录状态..."; if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" >/dev/null 2>&1; then log "ERROR" "登录后仍无法获取账号信息，脚本无法继续。请检查 'gcloud auth list' 的输出。"; exit 1; fi; fi; log "INFO" "GCP 账号检查通过。"
log "INFO" "检查 GCP 项目配置..."; if ! gcloud config get-value project >/dev/null 2>&1; then log "WARN" "尚未设置默认GCP项目。某些操作（如配额检查）可能无法正常工作。"; log "WARN" "建议使用 'gcloud config set project YOUR_PROJECT_ID' 设置一个默认项目。"; sleep 3; else log "INFO" "GCP 项目配置检查完成 (当前项目: $(gcloud config get-value project))。"; fi

# --- 主菜单循环 ---
while true; do show_menu; done
# exit 0
