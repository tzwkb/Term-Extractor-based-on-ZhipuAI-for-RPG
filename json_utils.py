"""JSON处理工具，用于修复和解析AI返回的JSON数据。"""

import json
import logging
import re
import ast
import importlib.util
import time

# 设置日志
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# 检查json_repair库是否可用
try:
    import json_repair
    HAS_JSON_REPAIR = True
    log.info("✅ json_repair库已可用")
except ImportError:
    HAS_JSON_REPAIR = False
    log.info("ℹ️ json_repair库未安装，将使用内置JSON修复工具")

# 定义备选json_repair函数
def repair_json(json_str, return_objects=False):
    """
    修复JSON字符串
    
    Args:
        json_str: 待修复的JSON字符串
        return_objects: 是否返回对象而非字符串
        
    Returns:
        修复后的JSON字符串
    """
    if not json_str:
        log.warning("⚠️ 修复空JSON字符串")
        return "{}" if not return_objects else {}
        
    log.debug(f"🔧 开始修复JSON字符串: {json_str[:50]}...")
    
    if HAS_JSON_REPAIR:
        # 使用实际的json_repair库
        try:
            start_time = time.time()
            result = json_repair.repair_json(json_str=json_str, return_objects=return_objects)
            duration = time.time() - start_time
            log.debug(f"✓ json_repair库修复完成 (耗时: {duration:.2f}秒)")
            return result
        except Exception as e:
            log.warning(f"⚠️ json_repair库修复失败: {str(e)}，将使用内置修复工具")
            # 回退到内置实现
    
    # 使用内置的修复实现
    log.debug("🔧 使用内置JSON修复工具")
    fixed = json_str
    
    try:
        # 修复常见错误
        # 1. 替换单引号为双引号
        fixed = re.sub(r"(?<!\\)'([^']*?)(?<!\\)'", r'"\1"', fixed)
        
        # 2. 修复键值对中缺少引号的问题
        fixed = re.sub(r'([{,]\s*)([a-zA-Z0-9_]+)(\s*:)', r'\1"\2"\3', fixed)
        
        # 3. 修复值缺少引号的问题 
        fixed = re.sub(r':\s*([a-zA-Z][a-zA-Z0-9_]*)\s*([,}])', r': "\1"\2', fixed)
        
        # 4. 移除多余的逗号
        fixed = re.sub(r',\s*([}\]])', r'\1', fixed)
        
        # 5. 添加缺失的引号
        fixed = re.sub(r':\s*([^",{\[\]\s][^,}\]]*?)([,}\]])', r': "\1"\2', fixed)
        
        # 6. 添加缺失的括号
        open_braces = fixed.count('{')
        close_braces = fixed.count('}')
        open_brackets = fixed.count('[')
        close_brackets = fixed.count(']')
        
        if open_braces > close_braces:
            fixed += '}' * (open_braces - close_braces)
            log.debug(f"✓ 添加了 {open_braces - close_braces} 个缺失的右花括号")
        if open_brackets > close_brackets:
            fixed += ']' * (open_brackets - close_brackets)
            log.debug(f"✓ 添加了 {open_brackets - close_brackets} 个缺失的右方括号")
    
        log.debug(f"🔍 修复后的JSON: {fixed[:100]}...")
        
        if return_objects:
            try:
                return json.loads(fixed)
            except Exception as e:
                log.warning(f"⚠️ 修复后的JSON仍无法解析: {str(e)}")
                return {}
        
        return fixed
    except Exception as e:
        log.warning(f"⚠️ 内置JSON修复出错: {str(e)}")
        if return_objects:
            return {}
        return json_str


def try_parse_ast_to_json(s):
    """
    尝试使用AST解析可能格式不标准的JSON
    
    Args:
        s: 可能格式不标准的JSON字符串
        
    Returns:
        tuple: (解析后的JSON字符串, Python对象)
    """
    try:
        parsed = ast.literal_eval(s)
        # 构建JSON字符串
        if isinstance(parsed, dict):
            json_info = json.dumps(parsed)
            return json_info, parsed
        else:
            log.warning(f"❌ AST解析结果不是字典: {type(parsed)}")
    except Exception as e:
        log.debug(f"⚠️ AST解析失败: {str(e)}")
    
    return s, {}


def try_parse_json_object(input_str: str) -> tuple[str, dict]:
    """
    尝试解析可能格式不正确的JSON字符串
    
    Args:
        input_str: 要解析的JSON字符串
        
    Returns:
        tuple: (清理后的JSON字符串, 解析后的JSON数据)
    """
    if not input_str:
        log.warning("⚠️ 输入为空字符串")
        return "", {}
    
    log.debug(f"🔍 开始解析JSON: {input_str[:100]}...")
    
    # 先尝试直接解析
    result = None
    try:
        result = json.loads(input_str)
        log.debug("✅ 直接解析JSON成功")
    except json.JSONDecodeError as e:
        log.info(f"⚠️ JSON解析错误: {str(e)}，尝试修复")

    if result:
        return input_str, result

    # 提取JSON部分
    try:
        json_patterns = [
            r"\{.*\}", # 花括号格式
            r"\[.*\]"  # 方括号格式
        ]
        
        for pattern in json_patterns:
            _match = re.search(pattern, input_str, re.DOTALL)
            if _match:
                extracted = _match.group(0)
                log.debug(f"✓ 成功提取JSON部分: {extracted[:50]}...")
                input_str = extracted
                break
    except Exception as e:
        log.warning(f"⚠️ 提取JSON部分出错: {str(e)}")

    # 清理JSON字符串
    try:
        # 移除Markdown代码块标记
        markdown_patterns = [
            (r"```json\s*", ""),
            (r"```\s*", ""),
            (r"~~~json\s*", ""),
            (r"~~~\s*", "")
        ]
        
        cleaned_str = input_str
        for pattern, replacement in markdown_patterns:
            cleaned_str = re.sub(pattern, replacement, cleaned_str)
        
        # 基本清理
        cleaned_str = (
            cleaned_str.replace("{{", "{")
            .replace("}}", "}")
            .replace('"[{', "[{")
            .replace('}]"', "}]")
            .replace("\\", " ")
            .strip()
        )
        
        # 保留换行符但移除多余空格
        cleaned_str = re.sub(r'[ \t]+', ' ', cleaned_str)
        
        log.debug(f"✓ 清理后的JSON: {cleaned_str[:100]}...")
    except Exception as e:
        log.warning(f"⚠️ 清理JSON字符串出错: {str(e)}")
        cleaned_str = input_str

    # 尝试解析清理后的字符串
    try:
        result = json.loads(cleaned_str)
        log.debug("✅ 清理后的JSON解析成功")
        return cleaned_str, result
    except json.JSONDecodeError as e:
        log.info(f"⚠️ 清理后的JSON仍无法解析: {str(e)}，继续尝试修复")
    
    # 修复并尝试解析
    try:
        # 使用json_repair
        fixed_json = repair_json(cleaned_str)
        try:
            result = json.loads(fixed_json)
            log.info("✅ JSON修复成功并解析")
            return fixed_json, result
        except json.JSONDecodeError as e:
            log.warning(f"⚠️ 修复后的JSON仍然无法解析: {str(e)}")
    except Exception as e:
        log.warning(f"⚠️ JSON修复出错: {str(e)}")
        
    # 尝试使用AST解析
    try:
        log.info("🔄 尝试使用AST解析JSON...")
        json_info, result = try_parse_ast_to_json(cleaned_str)
        if result:
            log.info("✅ AST解析成功")
            return json_info, result
    except Exception as e:
        log.warning(f"⚠️ AST解析失败: {str(e)}")

    # 最后尝试手动修复常见错误
    try:
        log.info("🔄 尝试手动修复JSON...")
        # 修复键值对中缺少引号的问题
        fixed_json = re.sub(r'([{,]\s*)([a-zA-Z0-9_]+)(\s*:)', r'\1"\2"\3', cleaned_str)
        # 修复值缺少引号的问题
        fixed_json = re.sub(r':\s*([a-zA-Z][a-zA-Z0-9_]*)\s*([,}])', r': "\1"\2', fixed_json)
        
        try:
            result = json.loads(fixed_json)
            log.info("✅ 手动修复JSON成功")
            return fixed_json, result
        except Exception:
            # 最后一次尝试：强制创建JSON对象
            log.warning("⚠️ 所有修复尝试失败，创建空对象并添加可能的内容")
            # 尝试从原始文本中提取可能的键值对
            pattern = r'"([^"]+)"\s*:\s*"([^"]+)"'
            matches = re.findall(pattern, cleaned_str)
            
            fallback = {}
            if matches:
                log.info(f"📄 找到 {len(matches)} 个可能的键值对")
                for key, value in matches:
                    fallback[key] = value
            
            return cleaned_str, fallback
    except Exception as e:
        log.exception(f"❌ 修复JSON完全失败: {str(e)}")
        # 返回空对象作为最后的回退
        return cleaned_str, {}

def fix_json_response(content: str, expected_keys: list = None) -> dict:
    """
    修复并解析JSON响应
    
    Args:
        content: JSON字符串内容
        expected_keys: 预期的JSON键列表
        
    Returns:
        dict: 解析后的JSON数据
    """
    start_time = time.time()
    log.info(f"🔄 开始处理JSON响应 (长度: {len(content)} 字符)")
    
    # 移除可能存在的前缀说明和尾部解释
    try:
        # 寻找第一个左花括号或左方括号的位置
        json_start = min(
            content.find("{") if content.find("{") >= 0 else float('inf'),
            content.find("[") if content.find("[") >= 0 else float('inf')
        )
        
        # 寻找最后一个右花括号或右方括号的位置
        json_end = max(
            content.rfind("}") if content.rfind("}") >= 0 else -1,
            content.rfind("]") if content.rfind("]") >= 0 else -1
        )
        
        if json_start < float('inf') and json_end > -1:
            if json_start > 0 or json_end < len(content) - 1:
                trimmed = content[json_start:json_end+1]
                log.info(f"✂️ 截取了JSON部分 (从位置 {json_start} 到 {json_end})")
                content = trimmed
    except Exception as e:
        log.warning(f"⚠️ 提取JSON内容出错: {str(e)}")
    
    # 修复并解析JSON
    _, result = try_parse_json_object(content)
    
    # 确保返回的是字典类型
    if not isinstance(result, dict):
        log.warning(f"⚠️ 解析结果不是字典类型: {type(result)}")
        # 尝试将非字典结果转换为字典
        if isinstance(result, list):
            if expected_keys and len(expected_keys) > 0:
                log.info(f"🔄 将列表结果转换为包含 '{expected_keys[0]}' 键的字典")
                result = {expected_keys[0]: result}
            else:
                result = {"items": result}
        else:
            result = {"error": "返回的不是JSON对象", "content": str(content)}
    
    # 如果提供了预期键，检查并确保这些键存在
    if expected_keys and isinstance(result, dict):
        for key in expected_keys:
            if key not in result:
                log.warning(f"⚠️ 预期的键 '{key}' 不存在，添加为空列表")
                result[key] = []
    
    duration = time.time() - start_time
    log.info(f"✅ JSON处理完成 (耗时: {duration:.2f}秒，结果包含 {len(result)} 个顶级键)")
    
    return result 