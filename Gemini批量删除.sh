#!/bin/bash

# ===== 配置 =====
# 自动生成随机用户名
TIMESTAMP=$(date +%s)
RANDOM_CHARS=$(cat /dev/urandom | tr -dc 'a-z0-9' | fold -w 4 | head -n 1)
EMAIL_USERNAME="momo${RANDOM_CHARS}${TIMESTAMP:(-4)}"
PROJECT_PREFIX="gemini-key"
TOTAL_PROJECTS=180  # 这个值可能会根据配额检查结果自动调整
MAX_PARALLEL_JOBS=20  # 默认设置为20
MAX_RETRY_ATTEMPTS=3  # 重试次数，之前是1（实际上没有重试）
AGGREGATED_KEY_FILE="gemini_api_keys_${EMAIL_USERNAME}_parallel.txt"
PURE_KEY_FILE="key.txt"
COMMA_SEPARATED_KEY_FILE="comma_separated_keys_${EMAIL_USERNAME}.txt"
SECONDS=0
DELETION_LOG="project_deletion_$(date +%Y%m%d_%H%M%S).log"
TEMP_DIR="/tmp/gcp_script_${TIMESTAMP}"
# ===== 配置结束 =====

# ===== 初始化 =====
# 创建临时目录
mkdir -p "$TEMP_DIR"

# 检查是否安装了jq（用于JSON解析）
check_jq() {
  if ! command -v jq &> /dev/null; then
    echo "警告: 未检测到jq工具，这将影响JSON解析的可靠性"
    echo "建议安装jq以提高脚本稳定性"
    echo "  - Debian/Ubuntu: sudo apt-get install jq"
    echo "  - CentOS/RHEL: sudo yum install jq"
    echo "  - macOS: brew install jq"
    read -p "是否继续执行？(y/n): " continue_without_jq
    if [[ ! "$continue_without_jq" =~ ^[Yy]$ ]]; then
      echo "操作已取消"
      exit 1
    fi
    echo "将使用备选方法解析JSON"
    return 1
  fi
  return 0
}

# 调用检查
HAS_JQ=$(check_jq && echo true || echo false)
# ===== 初始化结束 =====

# ===== 工具函数 =====
# 统一日志函数
log() {
  local level=$1
  local msg=$2
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$level] $msg"
}

# 解析JSON并提取字段（鲁棒性更强）
parse_json() {
  local json="$1"
  local field="$2"

  # 检查输入是否为空
  if [ -z "$json" ]; then
    return 1
  fi

  if [ "$HAS_JQ" = true ]; then
    # 使用jq解析（更可靠）
    local value=$(echo "$json" | jq -r "$field" 2>/dev/null)
    if [ $? -eq 0 ] && [ "$value" != "null" ] && [ -n "$value" ]; then
      echo "$value"
      return 0
    fi
  else
    # 备用方法：根据字段类型使用不同的解析策略
    case "$field" in
      ".keyString")
        local value=$(echo "$json" | grep -oP '(?<="keyString": ")[^"]+' 2>/dev/null)
        ;;
      ".[0].name")
        local value=$(echo "$json" | grep -o '"name": "[^"]*"' | head -1 | cut -d'"' -f4 2>/dev/null)
        ;;
      *)
        # 其他字段的通用提取方案，基于字段名称提取
        local field_name=$(echo "$field" | tr -d '.["]')
        local value=$(echo "$json" | grep -oP "(?<=\"$field_name\":\s*\")[^\"]*" 2>/dev/null)
        ;;
    esac

    if [ -n "$value" ]; then
      echo "$value"
      return 0
    fi
  fi

  return 1
}

# 统一文件写入函数
write_to_files() {
  local result_line="$1"
  local api_key="$2"
  local is_success="$3"

  (
    flock 200
    echo "$result_line" >> "$AGGREGATED_KEY_FILE"
    # 如果成功，同时写入纯密钥文件和逗号分隔文件
    if [[ "$is_success" == "true" && -n "$api_key" ]]; then
      echo "$api_key" >> "$PURE_KEY_FILE"
      # 将密钥追加到逗号分隔文件
      if [[ -s "$COMMA_SEPARATED_KEY_FILE" ]]; then
        echo -n "," >> "$COMMA_SEPARATED_KEY_FILE"
      fi
      echo -n "$api_key" >> "$COMMA_SEPARATED_KEY_FILE"
    fi
  ) 200>"$AGGREGATED_KEY_FILE.lock"
}

# 改进的指数退避重试函数
retry_with_backoff() {
  local max_attempts=$1
  local cmd=$2
  local attempt=1
  local timeout=5
  local error_log="${TEMP_DIR}/error_$(date +%s)_$RANDOM.log"

  while [ $attempt -le $max_attempts ]; do
    # 执行命令，捕获错误输出
    if eval "$cmd" 2>"$error_log"; then
      rm -f "$error_log"  # 成功时删除错误日志
      return 0
    else
      local error_msg=$(cat "$error_log")
      log "INFO" "尝试 $attempt/$max_attempts 失败，错误: ${error_msg:-'未知错误'}"
      log "INFO" "等待 $timeout 秒后重试..."
      sleep $timeout
      attempt=$((attempt + 1))
      timeout=$((timeout * 2))  # 指数退避
    fi
  done

  # 最终失败时保留并显示错误信息
  if [ -f "$error_log" ]; then
    local final_error=$(cat "$error_log")
    log "ERROR" "最终失败，错误信息: ${final_error:-'未知错误'}"
    rm -f "$error_log"
  fi

  return 1
}

# 进度条显示（优化版）
show_progress() {
  local completed=$1
  local total=$2
  local percent=$((completed * 100 / total))
  local completed_chars=$((percent / 2))

  # 先清除当前行
  printf "\r%-80s" " "
  # 显示进度条
  printf "\r[%-50s] %d%% (%d/%d)" "$(printf '#%.0s' $(seq 1 $completed_chars))" "$percent" "$completed" "$total"
}

# 配额检查及调整（改进版）
check_quota() {
  log "INFO" "检查GCP配额..."

  # 尝试获取项目创建配额（注意：使用alpha命令）
  local projects_quota
  local quota_cmd="gcloud alpha service-management quota --service=cloudresourcemanager.googleapis.com 2>${TEMP_DIR}/quota_error.log"
  local quota_output

  # 使用改进的重试逻辑获取配额信息
  if quota_output=$(eval "$quota_cmd"); then
    projects_quota=$(echo "$quota_output" | grep "project-creation-requests" | awk '{print $2}')
  else
    local error_msg=$(cat "${TEMP_DIR}/quota_error.log")
    log "WARN" "无法获取配额信息: ${error_msg:-'未知错误'}"
    log "WARN" "将使用默认设置继续，但可能会遇到配额限制"
    rm -f "${TEMP_DIR}/quota_error.log"
    return 0
  fi

  rm -f "${TEMP_DIR}/quota_error.log"

  # 如果无法获取配额信息，给出警告但继续执行
  if [ -z "$projects_quota" ]; then
    log "WARN" "无法从输出中提取项目创建配额，将使用默认设置继续"
    return 0
  fi

  # 如果配额小于计划创建的项目数，调整计划数量
  if [ "$projects_quota" -lt "$TOTAL_PROJECTS" ]; then
    log "WARN" "您的项目创建配额($projects_quota)小于计划创建的项目数($TOTAL_PROJECTS)"
    echo "选项:"
    echo "1. 继续创建 $TOTAL_PROJECTS 个项目 (可能部分失败)"
    echo "2. 调整为创建 $projects_quota 个项目 (符合配额)"
    echo "3. 取消操作"
    read -p "请选择 [1/2/3]: " quota_option

    case $quota_option in
      1)
        log "INFO" "将尝试创建 $TOTAL_PROJECTS 个项目"
        ;;
      2)
        TOTAL_PROJECTS=$projects_quota
        log "INFO" "已调整计划，将创建 $TOTAL_PROJECTS 个项目"
        ;;
      3|*)
        log "INFO" "操作已取消"
        return 1
        ;;
    esac
  else
    log "SUCCESS" "配额检查通过，您的项目创建配额($projects_quota)足够创建 $TOTAL_PROJECTS 个项目"
  fi

  return 0
}

# 生成报告
generate_report() {
  local success=$1
  local failed=$2
  local total=$3
  local success_rate=0

  if [ $total -gt 0 ]; then
    success_rate=$((success * 100 / total))
  fi

  local duration=$SECONDS
  local minutes=$((duration / 60))
  local seconds=$((duration % 60))

  echo ""
  echo "========== 执行报告 =========="
  echo "总计尝试: $total 个项目"
  echo "成功率: $success_rate% ($success 成功 / $failed 失败)"

  if [ $total -gt 0 ]; then
    echo "平均处理时间: $((duration / total)) 秒/项目"
  fi

  echo "总执行时间: $minutes 分 $seconds 秒"
  echo "API密钥已保存至:"
  echo "- 详细信息: $AGGREGATED_KEY_FILE"
  echo "- 纯API密钥: $PURE_KEY_FILE"
  echo "- 逗号分隔密钥: $COMMA_SEPARATED_KEY_FILE"
  echo "=========================="
}

# 项目处理函数
process_project() {
  local project_id="$1"
  local project_num="$2"
  local total="$3"

  # 创建项目特定的错误日志文件
  local error_log="${TEMP_DIR}/project_${project_id}_error.log"

  echo ">>> [$project_num/$total] 开始处理项目: $project_id"

  # 1. 创建项目
  echo "[$project_num] 1/3 创建项目: $project_id ..."
  if ! gcloud projects create "$project_id" --name="$project_id" --no-set-as-default --quiet 2>"$error_log"; then
    local error_msg=$(cat "$error_log")
    echo "[$project_num] 错误: 无法创建项目 $project_id: ${error_msg:-'未知错误'}"
    write_to_files "$project_id: 【项目创建失败】" "" "false"
    echo "<<< [$project_num] 项目 $project_id 处理失败 (创建)"
    rm -f "$error_log"
    return 1
  fi

  # 等待项目创建完成传播（必要的延时，因为GCP API是最终一致性的）
  log "INFO" "等待项目创建操作完成传播 (2秒)..."
  sleep 2

  # 2. 启用 API (使用改进的重试逻辑)
  echo "[$project_num] 2/3 启用 Generative Language API..."

  if ! retry_with_backoff $MAX_RETRY_ATTEMPTS "gcloud services enable generativelanguage.googleapis.com --project=\"$project_id\" --quiet"; then
    echo "[$project_num] 错误: 无法为项目 $project_id 启用 API"
    write_to_files "$project_id: 【API启用失败】" "" "false"
    echo "<<< [$project_num] 项目 $project_id 处理失败 (API启用)"
    return 1
  fi

  echo "[$project_num] API 已为 $project_id 启用"

  # 3. 创建密钥并提取
  echo "[$project_num] 3/3 创建 API 密钥..."
  local create_output
  create_output=$(gcloud services api-keys create --project="$project_id" \
    --display-name="Gemini API Key for $project_id" --format="json" --quiet 2>"$error_log")

  if [ $? -eq 0 ] && [ -n "$create_output" ]; then
    local api_key=""

    # 使用改进的JSON解析获取API密钥
    api_key=$(parse_json "$create_output" ".keyString")

    if [ -n "$api_key" ]; then
      echo "[$project_num] 成功: 为 $project_id 提取到 API 密钥"
      write_to_files "$project_id: $api_key" "$api_key" "true"
      echo "<<< [$project_num] 项目 $project_id 处理成功"
      rm -f "$error_log"
      return 0
    fi
  fi

  local error_msg=$(cat "$error_log")
  echo "[$project_num] 警告: 无法为 $project_id 创建或提取 API 密钥: ${error_msg:-'未知错误'}"
  write_to_files "$project_id: 【密钥创建/提取失败】" "" "false"
  echo "<<< [$project_num] 项目 $project_id 处理失败 (密钥创建/提取)"
  rm -f "$error_log"
  return 1
}

# 删除单个项目函数
delete_project() {
  local project_id="$1"
  local project_num="$2"
  local total="$3"
  local error_log="${TEMP_DIR}/delete_${project_id}_error.log"

  echo ">>> [$project_num/$total] 删除项目: $project_id"

  # 尝试删除项目
  if gcloud projects delete "$project_id" --quiet 2>"$error_log"; then
    echo "<<< [$project_num/$total] 成功删除项目: $project_id"
    # 添加到日志文件，使用 flock 防止并发写入冲突
    (
      flock 201
      echo "[$project_num/$total] 已删除: $project_id" >> "$DELETION_LOG"
    ) 201>"$DELETION_LOG.lock"
    rm -f "$error_log"
    return 0
  else
    local error_msg=$(cat "$error_log")
    echo "<<< [$project_num/$total] 删除项目失败: $project_id: ${error_msg:-'未知错误'}"
    (
      flock 201
      echo "[$project_num/$total] 删除失败: $project_id" >> "$DELETION_LOG"
    ) 201>"$DELETION_LOG.lock"
    rm -f "$error_log"
    return 1
  fi
}

# 提取现有项目API密钥
extract_key_from_project() {
  local project_id="$1"
  local project_num="$2"
  local total="$3"
  local error_log="${TEMP_DIR}/extract_${project_id}_error.log"

  echo ">>> [$project_num/$total] 为项目获取API密钥: $project_id"

  # 启用 API (如果尚未启用)
  echo "[$project_num] 1/2 确保 Generative Language API 已启用..."

  if ! retry_with_backoff $MAX_RETRY_ATTEMPTS "gcloud services enable generativelanguage.googleapis.com --project=\"$project_id\" --quiet"; then
    echo "[$project_num] 错误: 无法为项目 $project_id 启用 API"
    write_to_files "$project_id: 【API启用失败】" "" "false"
    echo "<<< [$project_num] 项目 $project_id 处理失败 (API启用)"
    return 1
  fi

  # 获取现有密钥或创建新密钥
  echo "[$project_num] 2/2 获取或创建 API 密钥..."

  # 尝试列出现有密钥
  local existing_keys=$(gcloud services api-keys list --project="$project_id" --format="json" 2>"$error_log")

  # 如果存在密钥，获取第一个密钥的详细信息
  if [ $? -eq 0 ] && [ -n "$existing_keys" ] && [ "$existing_keys" != "[]" ]; then
    local key_name=""

    # 使用改进的JSON解析获取key_name
    if [ "$HAS_JQ" = true ]; then
      key_name=$(echo "$existing_keys" | jq -r '.[0].name // empty' 2>/dev/null)
    else
      key_name=$(echo "$existing_keys" | grep -o '"name": "[^"]*"' | head -1 | cut -d'"' -f4 2>/dev/null)
    fi

    if [ -n "$key_name" ]; then
      local key_details=$(gcloud services api-keys get-key-string "$key_name" --format="json" 2>"$error_log")

      if [ $? -eq 0 ] && [ -n "$key_details" ]; then
        # 使用改进的JSON解析获取API密钥
        local api_key=$(parse_json "$key_details" ".keyString")

        if [ -n "$api_key" ]; then
          echo "[$project_num] 成功: 为 $project_id 获取到现有 API 密钥"
          write_to_files "$project_id: $api_key" "$api_key" "true"
          echo "<<< [$project_num] 项目 $project_id 处理成功 (使用现有密钥)"
          rm -f "$error_log"
          return 0
        fi
      fi
    fi
  fi

  rm -f "$error_log"  # 清理错误日志，准备创建新密钥

  # 如果没有现有密钥或无法获取，创建新密钥
  local create_output=$(gcloud services api-keys create --project="$project_id" \
    --display-name="Gemini API Key for $project_id" --format="json" --quiet 2>"$error_log")

  if [ $? -eq 0 ] && [ -n "$create_output" ]; then
    # 使用改进的JSON解析获取API密钥
    local api_key=$(parse_json "$create_output" ".keyString")

    if [ -n "$api_key" ]; then
      echo "[$project_num] 成功: 为 $project_id 创建并提取到新 API 密钥"
      write_to_files "$project_id: $api_key" "$api_key" "true"
      echo "<<< [$project_num] 项目 $project_id 处理成功 (创建新密钥)"
      rm -f "$error_log"
      return 0
    fi
  fi

  local error_msg=$(cat "$error_log")
  echo "[$project_num] 警告: 无法为 $project_id 获取或创建 API 密钥: ${error_msg:-'未知错误'}"
  write_to_files "$project_id: 【密钥获取/创建失败】" "" "false"
  echo "<<< [$project_num] 项目 $project_id 处理失败"
  rm -f "$error_log"
  return 1
}

# 清理项目中的API密钥（新增功能）
cleanup_api_keys() {
  local project_id="$1"
  local project_num="$2"
  local total="$3"
  local error_log="${TEMP_DIR}/cleanup_${project_id}_error.log"

  echo ">>> [$project_num/$total] 清理项目API密钥: $project_id"

  # 获取项目中的所有API密钥
  local existing_keys=$(gcloud services api-keys list --project="$project_id" --format="json" 2>"$error_log")

  if [ $? -ne 0 ] || [ -z "$existing_keys" ] || [ "$existing_keys" = "[]" ]; then
    local error_msg=$(cat "$error_log" 2>/dev/null)
    echo "[$project_num] 信息: 项目 $project_id 没有API密钥或无法获取密钥列表: ${error_msg:-'没有密钥'}"
    rm -f "$error_log"
    return 0
  fi

  # 解析并删除每个密钥
  local key_count=0
  local deleted_count=0

  if [ "$HAS_JQ" = true ]; then
    # 使用jq解析JSON数组
    local key_names=($(echo "$existing_keys" | jq -r '.[].name' 2>/dev/null))
    key_count=${#key_names[@]}

    for key_name in "${key_names[@]}"; do
      echo "[$project_num] 删除API密钥: $key_name"
      if gcloud services api-keys delete "$key_name" --quiet 2>>"$error_log"; then
        ((deleted_count++))
      fi
    done
  else
    # 备用方法：使用grep和cut提取密钥名称
    local key_lines=$(echo "$existing_keys" | grep -o '"name": "[^"]*"')
    local IFS=$'\n'  # 设置内部字段分隔符为换行符
    local key_name_lines=($key_lines)
    key_count=${#key_name_lines[@]}

    for line in "${key_name_lines[@]}"; do
      local key_name=$(echo "$line" | cut -d'"' -f4)
      echo "[$project_num] 删除API密钥: $key_name"
      if gcloud services api-keys delete "$key_name" --quiet 2>>"$error_log"; then
        ((deleted_count++))
      fi
    done
  fi

  echo "<<< [$project_num/$total] 项目 $project_id 的API密钥清理完成 (删除了 $deleted_count/$key_count 个密钥)"
  rm -f "$error_log"

  return 0
}

# 资源清理函数
cleanup_resources() {
  log "INFO" "清理临时文件..."
  rm -rf "$TEMP_DIR"
  rm -f "$AGGREGATED_KEY_FILE.lock" "$DELETION_LOG.lock"
  log "INFO" "资源清理完成"
}
# ===== 工具函数结束 =====

# ===== 功能模块 =====
# 功能1：删除所有项目并重建
delete_and_rebuild() {
  SECONDS=0
  echo "======================================================"
  echo "功能1: 删除所有项目并重建获取API密钥"
  echo "======================================================"

  # 获取所有项目列表
  echo "正在获取项目列表..."
  local list_error="${TEMP_DIR}/list_projects_error.log"
  local ALL_PROJECTS=($(gcloud projects list --format="value(projectId)" 2>"$list_error"))

  if [ $? -ne 0 ]; then
    local error_msg=$(cat "$list_error")
    log "ERROR" "无法获取项目列表: ${error_msg:-'未知错误'}"
    rm -f "$list_error"
    return 1
  fi
  rm -f "$list_error"

  if [ ${#ALL_PROJECTS[@]} -eq 0 ]; then
    log "INFO" "未找到任何项目，将直接开始创建新项目"
  else
    echo "找到 ${#ALL_PROJECTS[@]} 个项目需要删除"
    echo "前5个项目示例："
    for ((i=0; i<5 && i<${#ALL_PROJECTS[@]}; i++)); do
      echo " - ${ALL_PROJECTS[i]}"
    done

    if [ ${#ALL_PROJECTS[@]} -gt 5 ]; then
      echo " - ... 以及其他 $((${#ALL_PROJECTS[@]} - 5)) 个项目"
    fi

    # 确认删除
    read -p "确认要删除所有 ${#ALL_PROJECTS[@]} 个项目吗？删除后将重新创建！(输入 'DELETE-ALL' 确认): " confirm
    if [ "$confirm" != "DELETE-ALL" ]; then
      log "INFO" "删除操作已取消，返回主菜单"
      return 1
    fi

    # 初始化日志文件
    echo "项目删除日志 ($(date))" > "$DELETION_LOG"
    echo "------------------------------------" >> "$DELETION_LOG"

    # 导出函数和变量供后台进程使用
    export -f delete_project log parse_json
    export DELETION_LOG TEMP_DIR HAS_JQ

    # 并行删除项目
    active_jobs=0
    completed=0
    total=${#ALL_PROJECTS[@]}

    echo "开始并行删除项目..."

    for i in "${!ALL_PROJECTS[@]}"; do
      project_id="${ALL_PROJECTS[i]}"
      project_num=$((i + 1))

      # 在后台启动删除
      delete_project "$project_id" "$project_num" "$total" &

      ((active_jobs++))
      ((completed++))

      # 显示进度
      show_progress $completed $total

      echo ""
      echo "--- 当前运行 $active_jobs 个并行删除任务 (项目 $completed/$total) ---"

      # 控制并行数
      if [[ "$active_jobs" -ge "$MAX_PARALLEL_JOBS" ]]; then
        wait -n
        ((active_jobs--))
      fi

      sleep 0.2
    done

    # 等待所有剩余任务完成
    echo ""
    echo "======================================================"
    echo "所有 $total 个删除任务已启动"
    echo "正在等待剩余 $active_jobs 个任务完成..."
    wait
    echo "所有删除任务已执行完毕"
    echo "======================================================"

    # 清理 lock 文件
    rm -f "$DELETION_LOG.lock"

    # 统计结果
    successful_deletions=$(grep -c "已删除:" "$DELETION_LOG")
    failed_deletions=$(grep -c "删除失败:" "$DELETION_LOG")

    echo "删除结果统计："
    echo " - 成功: $successful_deletions"
    echo " - 失败: $failed_deletions"
    echo "详细日志已保存到: $DELETION_LOG"
    echo ""

    # 等待一小段时间，确保GCP系统处理完删除操作
    log "INFO" "等待系统处理完删除操作 (15秒)..."
    sleep 15  # 增加延时以确保GCP资源完全释放
  fi

  # 检查配额并准备创建新项目
  if ! check_quota; then
    return 1
  fi

  echo "即将开始创建新项目..."
  echo "将使用随机生成的用户名: ${EMAIL_USERNAME}"
  echo "脚本将在 5 秒后开始执行..."
  sleep 5

  # 初始化文件
  echo "Project_ID: API_Key (注：并行执行，顺序可能混合)" > "$AGGREGATED_KEY_FILE"
  echo "------------------------------------" >> "$AGGREGATED_KEY_FILE"
  > "$PURE_KEY_FILE"
  > "$COMMA_SEPARATED_KEY_FILE"

  # 导出函数和变量，以便后台进程访问
  export -f process_project retry_with_backoff log write_to_files parse_json
  export AGGREGATED_KEY_FILE PURE_KEY_FILE COMMA_SEPARATED_KEY_FILE TEMP_DIR MAX_RETRY_ATTEMPTS HAS_JQ

  # 主循环，启动并行任务
  active_jobs=0
  completed=0

  for i in $(seq 1 $TOTAL_PROJECTS); do
    project_num=$(printf "%03d" $i)
    project_id="${PROJECT_PREFIX}-${EMAIL_USERNAME}-${project_num}"
    project_id=${project_id:0:30}
    # 移除尾部的连字符（如果有）
    if [[ "$project_id" == *- ]]; then
      project_id=${project_id%-}
    fi

    # 在后台启动处理函数
    process_project "$project_id" "$i" "$TOTAL_PROJECTS" &

    ((active_jobs++))
    ((completed++))

    # 显示进度条
    show_progress $completed $TOTAL_PROJECTS

    echo ""
    echo "--- 已启动 ${active_jobs} 个任务 (当前处理项目 ${i}/${TOTAL_PROJECTS}) ---"

    # 控制并行数
    if [[ "$active_jobs" -ge "$MAX_PARALLEL_JOBS" ]]; then
      wait -n
      ((active_jobs--))
    fi

    sleep 0.2  # 短暂延时
  done

  # 等待所有剩余任务完成
  echo ""
  echo "======================================================"
  echo "所有 ${TOTAL_PROJECTS} 个项目的处理任务已启动"
  echo "正在等待剩余的 ${active_jobs} 个任务完成..."
  wait
  echo "所有任务已执行完毕"
  echo "======================================================"

  # 清理 lock 文件
  rm -f "$AGGREGATED_KEY_FILE.lock"

  # 统计结果
  successful_keys=$(grep -v '【' "$AGGREGATED_KEY_FILE" | grep -c ': ')
  if [ $successful_keys -gt 0 ]; then ((successful_keys--)); fi  # 排除标题行
  failed_entries=$(grep -c '【' "$AGGREGATED_KEY_FILE")

  # 生成报告
  generate_report $successful_keys $failed_entries $TOTAL_PROJECTS

  echo "======================================================"
  echo "请检查文件 '$AGGREGATED_KEY_FILE' 和 '$PURE_KEY_FILE' 和 '$COMMA_SEPARATED_KEY_FILE' 中的内容"
  echo "如果失败条目过多，可能是触发了 GCP 配额限制"
  echo "提醒：项目需要关联有效的结算账号才能实际使用 API 密钥"
  echo "======================================================"

  return 0
}

# 功能2：新建项目并获取密钥
create_projects_and_get_keys() {
  SECONDS=0
  echo "======================================================"
  echo "功能2: 新建项目并获取API密钥"
  echo "======================================================"

  # 检查配额
  if ! check_quota; then
    return 1
  fi

  echo "将使用随机生成的用户名: ${EMAIL_USERNAME}"
  echo "即将开始创建 $TOTAL_PROJECTS 个新项目..."
  echo "脚本将在 5 秒后开始执行..."
  sleep 5

  # 初始化文件
  echo "Project_ID: API_Key (注：并行执行，顺序可能混合)" > "$AGGREGATED_KEY_FILE"
  echo "------------------------------------" >> "$AGGREGATED_KEY_FILE"
  > "$PURE_KEY_FILE"
  > "$COMMA_SEPARATED_KEY_FILE"

  # 导出函数和变量，以便后台进程访问
  export -f process_project retry_with_backoff log write_to_files parse_json
  export AGGREGATED_KEY_FILE PURE_KEY_FILE COMMA_SEPARATED_KEY_FILE TEMP_DIR MAX_RETRY_ATTEMPTS HAS_JQ

  # 主循环，启动并行任务
  active_jobs=0
  completed=0

  for i in $(seq 1 $TOTAL_PROJECTS); do
    project_num=$(printf "%03d" $i)
    project_id="${PROJECT_PREFIX}-${EMAIL_USERNAME}-${project_num}"
    project_id=${project_id:0:30}
    # 移除尾部的连字符（如果有）
    if [[ "$project_id" == *- ]]; then
      project_id=${project_id%-}
    fi

    # 在后台启动处理函数
    process_project "$project_id" "$i" "$TOTAL_PROJECTS" &

    ((active_jobs++))
    ((completed++))

    # 显示进度条
    show_progress $completed $TOTAL_PROJECTS

    echo ""
    echo "--- 已启动 ${active_jobs} 个任务 (当前处理项目 ${i}/${TOTAL_PROJECTS}) ---"

    # 控制并行数
    if [[ "$active_jobs" -ge "$MAX_PARALLEL_JOBS" ]]; then
      wait -n
      ((active_jobs--))
    fi

    sleep 0.2  # 短暂延时
  done

  # 等待所有剩余任务完成
  echo ""
  echo "======================================================"
  echo "所有 ${TOTAL_PROJECTS} 个项目的处理任务已启动"
  echo "正在等待剩余的 ${active_jobs} 个任务完成..."
  wait
  echo "所有任务已执行完毕"
  echo "======================================================"

  # 清理 lock 文件
  rm -f "$AGGREGATED_KEY_FILE.lock"

  # 统计结果
  successful_keys=$(grep -v '【' "$AGGREGATED_KEY_FILE" | grep -c ': ')
  if [ $successful_keys -gt 0 ]; then ((successful_keys--)); fi  # 排除标题行
  failed_entries=$(grep -c '【' "$AGGREGATED_KEY_FILE")

  # 生成报告
  generate_report $successful_keys $failed_entries $TOTAL_PROJECTS

  echo "======================================================"
  echo "请检查文件 '$AGGREGATED_KEY_FILE' 和 '$PURE_KEY_FILE' 和 '$COMMA_SEPARATED_KEY_FILE' 中的内容"
  echo "如果失败条目过多，可能是触发了 GCP 配额限制"
  echo "提醒：项目需要关联有效的结算账号才能实际使用 API 密钥"
  echo "======================================================"

  return 0
}

# 功能3：获取现有项目的API密钥
get_keys_from_existing_projects() {
  SECONDS=0
  echo "======================================================"
  echo "功能3: 获取现有项目的API密钥"
  echo "======================================================"

  # 获取项目列表
  echo "正在获取项目列表..."
  local list_error="${TEMP_DIR}/list_projects_error.log"
  local ALL_PROJECTS=($(gcloud projects list --format="value(projectId)" 2>"$list_error"))

  if [ $? -ne 0 ]; then
    local error_msg=$(cat "$list_error")
    log "ERROR" "无法获取项目列表: ${error_msg:-'未知错误'}"
    rm -f "$list_error"
    return 1
  fi
  rm -f "$list_error"

  if [ ${#ALL_PROJECTS[@]} -eq 0 ]; then
    log "ERROR" "未找到任何项目，无法获取API密钥"
    return 1
  fi

  echo "找到 ${#ALL_PROJECTS[@]} 个项目"
  echo "前5个项目示例："
  for ((i=0; i<5 && i<${#ALL_PROJECTS[@]}; i++)); do
    echo " - ${ALL_PROJECTS[i]}"
  done

  if [ ${#ALL_PROJECTS[@]} -gt 5 ]; then
    echo " - ... 以及其他 $((${#ALL_PROJECTS[@]} - 5)) 个项目"
  fi

  read -p "确认要为这些项目获取API密钥吗？[y/N]: " confirm
  if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
    log "INFO" "操作已取消，返回主菜单"
    return 1
  fi

  # 初始化文件
  echo "Project_ID: API_Key (注：并行执行，顺序可能混合)" > "$AGGREGATED_KEY_FILE"
  echo "------------------------------------" >> "$AGGREGATED_KEY_FILE"
  > "$PURE_KEY_FILE"
  > "$COMMA_SEPARATED_KEY_FILE"

  # 导出函数和变量，以便后台进程访问
  export -f extract_key_from_project retry_with_backoff log write_to_files parse_json
  export AGGREGATED_KEY_FILE PURE_KEY_FILE COMMA_SEPARATED_KEY_FILE TEMP_DIR MAX_RETRY_ATTEMPTS HAS_JQ

  # 并行获取API密钥
  active_jobs=0
  completed=0
  total=${#ALL_PROJECTS[@]}

  echo "开始为项目获取API密钥..."

  for i in "${!ALL_PROJECTS[@]}"; do
    project_id="${ALL_PROJECTS[i]}"
    project_num=$((i + 1))

    # 在后台启动处理
    extract_key_from_project "$project_id" "$project_num" "$total" &

    ((active_jobs++))
    ((completed++))

    # 显示进度
    show_progress $completed $total

    echo ""
    echo "--- 当前运行 $active_jobs 个并行任务 (项目 $completed/$total) ---"

    # 控制并行数
    if [[ "$active_jobs" -ge "$MAX_PARALLEL_JOBS" ]]; then
      wait -n
      ((active_jobs--))
    fi

    sleep 0.2
  done

  # 等待所有剩余任务完成
  echo ""
  echo "======================================================"
  echo "所有 $total 个项目的处理任务已启动"
  echo "正在等待剩余 $active_jobs 个任务完成..."
  wait
  echo "所有任务已执行完毕"
  echo "======================================================"

  # 清理 lock 文件
  rm -f "$AGGREGATED_KEY_FILE.lock"

  # 统计结果
  successful_keys=$(grep -v '【' "$AGGREGATED_KEY_FILE" | grep -c ': ')
  if [ $successful_keys -gt 0 ]; then ((successful_keys--)); fi  # 排除标题行
  failed_entries=$(grep -c '【' "$AGGREGATED_KEY_FILE")

  # 生成报告
  generate_report $successful_keys $failed_entries $total

  echo "======================================================"
  echo "请检查文件 '$AGGREGATED_KEY_FILE' 和 '$PURE_KEY_FILE' 和 '$COMMA_SEPARATED_KEY_FILE' 中的内容"
  echo "======================================================"

  return 0
}

# 功能4：删除所有现有项目
delete_all_existing_projects() {
  SECONDS=0
  echo "======================================================"
  echo "功能4: 删除所有现有项目"
  echo "======================================================"

  # 获取所有项目列表
  echo "正在获取项目列表..."
  local list_error="${TEMP_DIR}/list_projects_error.log"
  local ALL_PROJECTS=($(gcloud projects list --format="value(projectId)" 2>"$list_error"))

  if [ $? -ne 0 ]; then
    local error_msg=$(cat "$list_error")
    log "ERROR" "无法获取项目列表: ${error_msg:-'未知错误'}"
    rm -f "$list_error"
    return 1
  fi
  rm -f "$list_error"

  if [ ${#ALL_PROJECTS[@]} -eq 0 ]; then
    log "INFO" "未找到任何项目，无需删除"
    return 1
  fi

  echo "找到 ${#ALL_PROJECTS[@]} 个项目需要删除"
  echo "前5个项目示例："
  for ((i=0; i<5 && i<${#ALL_PROJECTS[@]}; i++)); do
    echo " - ${ALL_PROJECTS[i]}"
  done

  if [ ${#ALL_PROJECTS[@]} -gt 5 ]; then
    echo " - ... 以及其他 $((${#ALL_PROJECTS[@]} - 5)) 个项目"
  fi

  # 确认删除
  read -p "确认要删除所有 ${#ALL_PROJECTS[@]} 个项目吗？此操作不可撤销！(输入 'DELETE-ALL' 确认): " confirm
  if [ "$confirm" != "DELETE-ALL" ]; then
    log "INFO" "删除操作已取消，返回主菜单"
    return 1
  fi

  # 初始化日志文件
  echo "项目删除日志 ($(date))" > "$DELETION_LOG"
  echo "------------------------------------" >> "$DELETION_LOG"

  # 导出函数和变量供后台进程使用
  export -f delete_project log parse_json
  export DELETION_LOG TEMP_DIR HAS_JQ

  # 并行删除项目
  active_jobs=0
  completed=0
  total=${#ALL_PROJECTS[@]}

  echo "开始并行删除项目..."

  for i in "${!ALL_PROJECTS[@]}"; do
    project_id="${ALL_PROJECTS[i]}"
    project_num=$((i + 1))

    # 在后台启动删除
    delete_project "$project_id" "$project_num" "$total" &

    ((active_jobs++))
    ((completed++))

    # 显示进度
    show_progress $completed $total

    echo ""
    echo "--- 当前运行 $active_jobs 个并行删除任务 (项目 $completed/$total) ---"

    # 控制并行数
    if [[ "$active_jobs" -ge "$MAX_PARALLEL_JOBS" ]]; then
      wait -n
      ((active_jobs--))
    fi

    sleep 0.2
  done

  # 等待所有剩余任务完成
  echo ""
  echo "======================================================"
  echo "所有 $total 个删除任务已启动"
  echo "正在等待剩余 $active_jobs 个任务完成..."
  wait
  echo "所有删除任务已执行完毕"
  echo "======================================================"

  # 清理 lock 文件
  rm -f "$DELETION_LOG.lock"

  # 统计结果
  successful_deletions=$(grep -c "已删除:" "$DELETION_LOG")
  failed_deletions=$(grep -c "删除失败:" "$DELETION_LOG")

  # 显示删除报告
  local duration=$SECONDS
  local minutes=$((duration / 60))
  local seconds=$((duration % 60))

  echo ""
  echo "========== 删除报告 =========="
  echo "总计处理: $total 个项目"
  echo "成功: $successful_deletions 个项目"
  echo "失败: $failed_deletions 个项目"
  echo "总执行时间: $minutes 分 $seconds 秒"
  echo "详细日志已保存至: $DELETION_LOG"
  echo "=========================="

  return 0
}

# 新增功能5：清理项目API密钥（不删除项目）
cleanup_project_api_keys() {
  SECONDS=0
  echo "======================================================"
  echo "功能5: 清理项目API密钥（不删除项目）"
  echo "======================================================"

  # 获取项目列表
  echo "正在获取项目列表..."
  local list_error="${TEMP_DIR}/list_projects_error.log"
  local ALL_PROJECTS=($(gcloud projects list --format="value(projectId)" 2>"$list_error"))

  if [ $? -ne 0 ]; then
    local error_msg=$(cat "$list_error")
    log "ERROR" "无法获取项目列表: ${error_msg:-'未知错误'}"
    rm -f "$list_error"
    return 1
  fi
  rm -f "$list_error"

  if [ ${#ALL_PROJECTS[@]} -eq 0 ]; then
    log "ERROR" "未找到任何项目，无法清理API密钥"
    return 1
  fi

  echo "找到 ${#ALL_PROJECTS[@]} 个项目"
  echo "前5个项目示例："
  for ((i=0; i<5 && i<${#ALL_PROJECTS[@]}; i++)); do
    echo " - ${ALL_PROJECTS[i]}"
  done

  if [ ${#ALL_PROJECTS[@]} -gt 5 ]; then
    echo " - ... 以及其他 $((${#ALL_PROJECTS[@]} - 5)) 个项目"
  fi

  read -p "确认要删除这些项目中的API密钥吗？[y/N]: " confirm
  if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
    log "INFO" "操作已取消，返回主菜单"
    return 1
  fi

  # 初始化日志文件
  local CLEANUP_LOG="api_keys_cleanup_$(date +%Y%m%d_%H%M%S).log"
  echo "API密钥清理日志 ($(date))" > "$CLEANUP_LOG"
  echo "------------------------------------" >> "$CLEANUP_LOG"

  # 导出函数和变量供后台进程使用
  export -f cleanup_api_keys log parse_json
  export TEMP_DIR HAS_JQ

  # 并行清理API密钥
  active_jobs=0
  completed=0
  total=${#ALL_PROJECTS[@]}

  echo "开始清理项目API密钥..."

  for i in "${!ALL_PROJECTS[@]}"; do
    project_id="${ALL_PROJECTS[i]}"
    project_num=$((i + 1))

    # 在后台启动清理任务
    cleanup_api_keys "$project_id" "$project_num" "$total" &

    ((active_jobs++))
    ((completed++))

    # 显示进度
    show_progress $completed $total

    echo ""
    echo "--- 当前运行 $active_jobs 个并行清理任务 (项目 $completed/$total) ---"

    # 控制并行数
    if [[ "$active_jobs" -ge "$MAX_PARALLEL_JOBS" ]]; then
      wait -n
      ((active_jobs--))
    fi

    sleep 0.2
  done

  # 等待所有剩余任务完成
  echo ""
  echo "======================================================"
  echo "所有 $total 个清理任务已启动"
  echo "正在等待剩余 $active_jobs 个任务完成..."
  wait
  echo "所有任务已执行完毕"
  echo "======================================================"

  # 显示清理报告
  local duration=$SECONDS
  local minutes=$((duration / 60))
  local seconds=$((duration % 60))

  echo ""
  echo "========== API密钥清理报告 =========="
  echo "总计处理: $total 个项目"
  echo "总执行时间: $minutes 分 $seconds 秒"
  echo "=========================="

  return 0
}

# 显示主菜单
show_menu() {
  clear
  echo "======================================================"
  echo "     GCP Gemini API 密钥懒人管理工具 v2.0 (优化版)"
  echo "======================================================"
  echo "当前账号: $(gcloud config get-value account 2>/dev/null || echo "未登录")"
  echo "并行任务数: $MAX_PARALLEL_JOBS"
  echo "重试次数: $MAX_RETRY_ATTEMPTS"
  echo "JSON解析: $([ "$HAS_JQ" = true ] && echo "使用jq (推荐)" || echo "使用备选方法")"
  echo ""
  echo "请选择功能:"
  echo "1. 一键删除所有现有项目并重建获取API密钥"
  echo "2. 一键新建项目并获取API密钥"
  echo "3. 一键获取现有项目的API密钥"
  echo "4. 一键删除所有现有项目"
  echo "5. 清理项目API密钥（不删除项目）"
  echo "6. 修改配置参数"
  echo "0. 退出"
  echo "======================================================"
  read -p "请输入选项 [0-6]: " choice

  case $choice in
    1)
      delete_and_rebuild
      ;;
    2)
      create_projects_and_get_keys
      ;;
    3)
      get_keys_from_existing_projects
      ;;
    4)
      delete_all_existing_projects
      ;;
    5)
      cleanup_project_api_keys
      ;;
    6)
      configure_settings
      ;;
    0)
      cleanup_resources
      echo "谢谢使用，再见！"
      exit 0
      ;;
    *)
      echo "无效选项，请重新选择"
      sleep 2
      ;;
  esac

  echo ""
  read -p "按回车键返回主菜单..."
  show_menu
}

# 配置设置
configure_settings() {
  echo "======================================================"
  echo "配置参数"
  echo "======================================================"
  echo "当前设置:"
  echo "1. 项目前缀: $PROJECT_PREFIX"
  echo "2. 计划创建的项目数量: $TOTAL_PROJECTS"
  echo "3. 最大并行任务数: $MAX_PARALLEL_JOBS"
  echo "4. 最大重试次数: $MAX_RETRY_ATTEMPTS"
  echo "======================================================"

  read -p "请选择要修改的设置 [1-4] 或按回车返回: " setting_choice

  case $setting_choice in
    1)
      read -p "请输入新的项目前缀: " new_prefix
      if [ -n "$new_prefix" ]; then
        PROJECT_PREFIX="$new_prefix"
        echo "项目前缀已更新为: $PROJECT_PREFIX"
      fi
      ;;
    2)
      read -p "请输入计划创建的项目数量: " new_total
      if [[ "$new_total" =~ ^[0-9]+$ ]] && [ "$new_total" -gt 0 ]; then
        TOTAL_PROJECTS=$new_total
        echo "计划创建的项目数量已更新为: $TOTAL_PROJECTS"
      else
        echo "无效的数值，保持原设置"
      fi
      ;;
    3)
      read -p "请输入最大并行任务数: " new_parallel
      if [[ "$new_parallel" =~ ^[0-9]+$ ]] && [ "$new_parallel" -gt 0 ]; then
        MAX_PARALLEL_JOBS=$new_parallel
        echo "最大并行任务数已更新为: $MAX_PARALLEL_JOBS"
      else
        echo "无效的数值，保持原设置"
      fi
      ;;
    4)
      read -p "请输入最大重试次数: " new_retries
      if [[ "$new_retries" =~ ^[0-9]+$ ]] && [ "$new_retries" -ge 1 ]; then
        MAX_RETRY_ATTEMPTS=$new_retries
        echo "最大重试次数已更新为: $MAX_RETRY_ATTEMPTS"
      else
        echo "无效的数值，保持原设置"
      fi
      ;;
    *)
      return
      ;;
  esac

  sleep 2
  configure_settings
}

# ===== 主程序 =====
# 设置退出处理函数
trap cleanup_resources EXIT

# 检查是否已登录gcloud
if ! gcloud config get-value account &>/dev/null; then
  echo "尚未登录Google Cloud账号，请先登录:"
  gcloud auth login

  if [ $? -ne 0 ]; then
    echo "登录失败，请重新运行脚本并确保能够成功登录"
    exit 1
  fi
fi

# 显示主菜单
show_menu
