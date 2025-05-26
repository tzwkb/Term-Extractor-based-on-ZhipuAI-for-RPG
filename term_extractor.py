"""
术语提取工具 - 批处理实现
提供从文本中提取专业术语的功能，使用智谱AI批处理API
"""

import os
import re
import json
import pandas as pd
import xlsxwriter
import requests
import time
import logging
from typing import List, Dict, Any, Optional, Tuple, Union
from pathlib import Path
import threading

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 检查可选依赖
try:
    from zhipuai import ZhipuAI
    HAS_ZHIPUAI = True
except ImportError:
    HAS_ZHIPUAI = False
    logger.info("未安装zhipuai库，无法使用批处理功能")

# JSON处理工具导入
try:
    from json_utils import fix_json_response, parse_json_safely, is_likely_json
    HAS_JSON_UTILS = True
except ImportError:
    HAS_JSON_UTILS = False
    logger.info("未安装json_utils模块，将使用备用JSON处理")

class TermExtractor:
    """术语提取器核心类 - 批处理模式"""
    
    def __init__(self, api_key: Optional[str] = None, api_url: Optional[str] = None):
        """
        初始化术语提取器
        
        Args:
            api_key: API密钥
            api_url: API端点URL
        """
        # API设置
        self.api_key = api_key
        self.api_url = api_url or "https://open.bigmodel.cn/api/paas/v4/chat/completions"
        self.model = "glm-4-flash"  # 默认使用更快的GLM-4-Flash模型
        
        # 提取设置
        self.min_term_length = 2  # 术语最小长度
        self.max_retries = 3  # 最大重试次数
        
        # 批处理相关数据
        self.batch_id = None
        self.input_file_id = None
        self.output_file_id = None
        self.error_file_id = None
        
        # 进度回调
        self.status_callback = None
        self.progress_callback = None
        self.complete_callback = None
        self.stop_event = None
        
        # 配置
        self.config = {"output_dir": "results"}

    def set_callbacks(self, status_callback=None, progress_callback=None, complete_callback=None):
        """
        设置回调函数
        
        Args:
            status_callback: 状态更新回调函数
            progress_callback: 进度更新回调函数
            complete_callback: 完成回调函数
        """
        self.status_callback = status_callback
        self.progress_callback = progress_callback
        self.complete_callback = complete_callback
        
    def set_status(self, status: str):
        """更新状态"""
        if self.status_callback:
            self.status_callback(status)
            
    def update_progress(self, value: float):
        """更新进度"""
        if self.progress_callback:
            self.progress_callback(value)
            
    def notify_complete(self):
        """通知完成"""
        if self.complete_callback:
            self.complete_callback()
            
    def stop(self):
        """停止处理"""
        if hasattr(self, 'stop_event') and self.stop_event:
            self.stop_event.set()
    
    def process_data(self, excel_file: str, chunks_dir: str = 'chunks', output_file: str = 'extracted_terms.xlsx') -> None:
        """处理数据主方法"""
        if not HAS_ZHIPUAI:
            logger.error("未安装zhipuai库，无法使用批处理功能")
            self.set_status("错误: 未安装zhipuai库，无法使用批处理功能")
            return
            
        logger.info(f"开始处理数据: {excel_file}")
        
        # 检查停止事件
        if hasattr(self, 'stop_event') and self.stop_event and self.stop_event.is_set():
            logger.info("检测到停止请求，终止处理")
            return
            
        # 处理输入数据
        df, id_col, text_cols = self._process_input_data(excel_file)
        if df.empty:
            logger.error("处理输入数据失败")
            return
            
        # 准备批处理文件并执行
        try:
            # 创建必要的目录
            os.makedirs(chunks_dir, exist_ok=True)
            
            # 生成JSONL文件
            jsonl_path = os.path.join(chunks_dir, f"batch_requests_{os.path.basename(excel_file)}.jsonl")
            jsonl_path = self.prepare_batch_jsonl(df, id_col, text_cols, jsonl_path)
            
            # 上传文件并创建批处理任务
            self.set_status("正在上传批处理文件...")
            input_file_id = self.upload_batch_file(jsonl_path)
            
            self.set_status("正在创建批处理任务...")
            batch_id = self.create_batch_job(input_file_id, f"处理文件: {os.path.basename(excel_file)}")
            
            # 监控任务状态
            self.set_status("批处理任务正在进行...")
            status = self._wait_for_batch_completion(batch_id)
            
            # 如果任务被取消或失败
            if not status or status['status'] != 'completed':
                return
                
            # 下载和处理结果
            output_file_id = status.get('output_file_id')
            if not output_file_id:
                logger.error("未找到输出文件ID")
                self.set_status("未找到输出文件ID")
                return
                
            self.set_status("正在下载批处理结果...")
            results_file = os.path.join(chunks_dir, f"batch_results_{batch_id}.jsonl")
            results_file = self.download_batch_results(output_file_id, results_file)
            
            self.set_status("正在解析批处理结果...")
            results = self.parse_batch_results(results_file)
            
            self.set_status("正在导出结果到Excel...")
            result = self.export_to_excel(results, output_file)
            
            # 完成
            self.set_status("批处理完成")
            self.notify_complete()
            logger.info(f"批处理模式处理数据完成: {output_file}")
            
        except Exception as e:
            logger.error(f"批处理过程出错: {str(e)}")
            self.set_status(f"错误: {str(e)}")
    
    def _wait_for_batch_completion(self, batch_id: str) -> Optional[Dict]:
        """等待批处理任务完成"""
        iteration = 0
        start_time = time.time()
        estimated_total_time = 300  # 估计需要5分钟完成
        
        while True:
            # 检查是否请求停止
            if self.stop_event and self.stop_event.is_set():
                logger.info("检测到停止请求，终止批处理监控")
                self.set_status("用户已取消操作")
                return None
                
            # 检查状态
            status = self.check_batch_status(batch_id)
            status_text = f"批处理任务状态: {status['status']}..."
            
            # 估计进度并更新
            elapsed_time = time.time() - start_time
            if status['status'] == 'running':
                # 如果API返回了进度信息，使用它
                if 'progress' in status and status['progress'] is not None:
                    progress = 0.1 + (status['progress'] * 0.8)  # 10%-90%的进度区间
                else:
                    # 基于已经经过的时间估计进度
                    progress = min(0.1 + (elapsed_time / estimated_total_time * 0.8), 0.9)
                    
                # 计算预计剩余时间
                if progress > 0.1:
                    remaining_seconds = (elapsed_time / (progress - 0.1)) * (0.9 - progress)
                    remaining_mins = int(remaining_seconds / 60)
                    remaining_secs = int(remaining_seconds % 60)
                    status_text += f" 预计剩余时间：{remaining_mins}分{remaining_secs}秒"
                
                # 更新进度条
                if self.progress_callback:
                    self.progress_callback(progress)
            
            # 更新状态文本
            self.set_status(status_text)
                
            # 检查是否完成
            if status['status'] == 'completed':
                # 设置进度为90%，留10%给后续处理
                if self.progress_callback:
                    self.progress_callback(0.9)
                return status
            
            # 检查是否失败
            if status['status'] in ['failed', 'canceled']:
                logger.error(f"批处理任务失败: {status.get('error', '未知错误')}")
                self.set_status(f"批处理任务失败: {status.get('error', '未知错误')}")
                return None
                
            # 等待一段时间再检查
            # 前几次快速检查，之后降低频率，避免频繁API调用
            if iteration < 3:
                time.sleep(5)  # 开始时每5秒检查一次
            else:
                time.sleep(15)  # 之后每15秒检查一次
            
            iteration += 1

    def prepare_batch_jsonl(self, df: pd.DataFrame, id_col: str, text_cols: List[str], output_path: str) -> str:
        """准备批处理JSONL文件"""
        logger.info(f"准备批处理JSONL文件: {output_path}")
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # 生成系统提示
        system_prompt = "你是一个术语提取专家。"
        
        # 检查是否是单列文件（使用了虚拟ID）
        is_virtual_id = id_col == "virtual_id"
        
        # 创建JSONL文件
        total_rows = len(df)
        with open(output_path, 'w', encoding='utf-8') as f:
            for idx, row in df.iterrows():
                # 检查是否请求停止
                if hasattr(self, 'stop_event') and self.stop_event and self.stop_event.is_set():
                    logger.info("检测到停止请求，终止批处理文件准备")
                    return output_path
                    
                # 获取行ID
                if is_virtual_id and "virtual_id" in df.columns:
                    # 使用添加的虚拟ID
                    row_id = str(row["virtual_id"])
                else:
                    # 使用原始ID列
                    row_id = str(row[id_col])
                
                # 获取文本内容（单列或多列）
                if text_cols:
                    # 使用指定的文本列
                    text_content = " ".join([str(row[col]) for col in text_cols if col in row and pd.notna(row[col])])
                else:
                    # 如果没有文本列（不应该发生，因为我们前面做了检查）
                    logger.warning(f"行 {row_id} 没有有效的文本列，将跳过")
                    continue
                
                # 跳过空文本
                if not text_content or text_content.strip() == "":
                    continue
            
                # 创建用户提示
                user_prompt = self._create_extraction_prompt(text_content)
                
                # 创建custom_id，确保不超过64个字符
                # 使用行索引来生成custom_id，确保不会超长
                custom_id = f"request-row{idx}"
                
                # 创建请求体
                request_body = {
                    "custom_id": custom_id,
                    "method": "POST",
                    "url": "/v4/chat/completions",
                    "body": {
                        "model": self.model,
                        "messages": [
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": user_prompt}
                        ],
                        "temperature": 0.3,
                        "max_tokens": 2000
                    }
                }
                
                # 写入JSONL
                f.write(json.dumps(request_body, ensure_ascii=False) + '\n')
                
                # 更新进度
                if self.progress_callback:
                    self.progress_callback((idx + 1) / total_rows)
                    
                # 定期更新状态
                if idx % 100 == 0:
                    self.set_status(f"正在准备批处理文件... ({idx + 1}/{total_rows})")
        
        logger.info(f"批处理JSONL文件准备完成: {output_path}")
        return output_path
        
    def _create_extraction_prompt(self, text_content: str) -> str:
        """创建术语提取提示"""
        return f'''# 角色：RPG游戏术语提取器

# 任务
1. 从下面给定的游戏文本中提取所有RPG游戏术语和专有名词
2. 提取对象：技能名称、职业名称、物品名称、地名、角色名、怪物名等
3. 排除一般常用词汇和非术语性表达
4. 如果文本本身就是术语，也要提取
5. 严禁重复提取相同术语
6. 严禁编造或生成不存在于给定文本中的术语
7. 必须为每个术语提供类型信息

# 术语类型示例
- 技能：表示游戏中的能力、招式、法术
- 职业：表示游戏中的角色职业、定位
- 物品：表示游戏中的道具、装备、消耗品
- 地点：表示游戏中的地区、场景、位置
- 角色：表示游戏中的NPC、主角、配角
- 怪物：表示游戏中的敌人、BOSS
- 系统：表示游戏机制、界面元素、功能
- 其他：如果不属于以上类型，请分类为其他

# 输出格式
{{"terms": [
  {{"term": "术语1", "type": "类型1", "context": "出现上下文"}}, 
  {{"term": "术语2", "type": "类型2", "context": "出现上下文"}}
]}}

# 说明
1. 只输出JSON格式结果，不要添加额外解释
2. 术语必须来自给定文本，不要编造
3. 只提取游戏相关的专业术语，不提取普通词汇
4. 如果给定文本中没有游戏相关术语，返回空数组: {{"terms": []}}
5. 必须为每个术语提供一个类型，如"技能"、"物品"、"地点"等

# 需要提取的文本：
{text_content}'''

    def upload_batch_file(self, file_path: str) -> str:
        """
        上传批处理文件并获取文件ID
        
        Args:
            file_path: 文件路径
            
        Returns:
            文件ID
        """
        if not HAS_ZHIPUAI:
            raise ImportError("未安装zhipuai库，无法使用批处理功能")
            
        logger.info(f"上传批处理文件: {file_path}")
        
        client = ZhipuAI(api_key=self.api_key.strip())
        
        try:
            with open(file_path, "rb") as f:
                result = client.files.create(
                    file=f,
                    purpose="batch"
                )
            
            file_id = result.id
            logger.info(f"文件上传成功，ID: {file_id}")
            return file_id
            
        except Exception as e:
            logger.error(f"文件上传失败: {str(e)}")
            raise
            
    def create_batch_job(self, input_file_id: str, description: str = "Term Extraction") -> str:
        """
        创建批处理任务
        
        Args:
            input_file_id: 输入文件ID
            description: 任务描述
            
        Returns:
            批处理任务ID
        """
        if not HAS_ZHIPUAI:
            raise ImportError("未安装zhipuai库，无法使用批处理功能")
            
        logger.info(f"创建批处理任务，输入文件ID: {input_file_id}")
        
        client = ZhipuAI(api_key=self.api_key.strip())
        
        try:
            result = client.batches.create(
                input_file_id=input_file_id,
                endpoint="/v4/chat/completions",
                completion_window="24h",  # 添加24小时的完成窗口
                metadata={
                    "description": description
                }
            )
            
            batch_id = result.id
            logger.info(f"批处理任务创建成功，ID: {batch_id}")
            self.batch_id = batch_id
            self.input_file_id = input_file_id
            return batch_id
            
        except Exception as e:
            logger.error(f"创建批处理任务失败: {str(e)}")
            raise

    def check_batch_status(self, batch_id: Optional[str] = None) -> Dict:
        """
        检查批处理任务状态
        
        Args:
            batch_id: 批处理任务ID，如果为None则使用当前任务ID
            
        Returns:
            任务状态信息
        """
        if not HAS_ZHIPUAI:
            raise ImportError("未安装zhipuai库，无法使用批处理功能")
            
        batch_id = batch_id or self.batch_id
        if not batch_id:
            raise ValueError("未指定批处理任务ID")
            
        logger.info(f"检查批处理任务状态，ID: {batch_id}")
        
        client = ZhipuAI(api_key=self.api_key.strip())
        
        try:
            result = client.batches.retrieve(batch_id)
            
            # 保存输出和错误文件ID
            if hasattr(result, 'output_file_id'):
                self.output_file_id = result.output_file_id
            if hasattr(result, 'error_file_id'):
                self.error_file_id = result.error_file_id
                
            # 转换为字典
            status_dict = {
                'id': result.id,
                'status': result.status,
                'created_at': result.created_at,
                'expires_at': result.expires_at if hasattr(result, 'expires_at') else None,
                'error': result.error if hasattr(result, 'error') else None,
                'output_file_id': result.output_file_id if hasattr(result, 'output_file_id') else None,
                'error_file_id': result.error_file_id if hasattr(result, 'error_file_id') else None
            }
            
            logger.info(f"批处理任务状态: {status_dict['status']}")
            return status_dict
            
        except Exception as e:
            logger.error(f"检查批处理任务状态失败: {str(e)}")
            raise

    def download_batch_results(self, file_id: Optional[str] = None, output_path: str = None) -> str:
        """
        下载批处理结果
        
        Args:
            file_id: 文件ID，如果为None则使用当前输出文件ID
            output_path: 输出文件路径
            
        Returns:
            输出文件路径
        """
        if not HAS_ZHIPUAI:
            raise ImportError("未安装zhipuai库，无法使用批处理功能")
            
        file_id = file_id or self.output_file_id
        if not file_id:
            raise ValueError("未指定文件ID")
            
        if not output_path:
            output_path = os.path.join(os.getcwd(), f"batch_results_{file_id}.jsonl")
            
        logger.info(f"下载批处理结果，文件ID: {file_id}，输出路径: {output_path}")
        
        client = ZhipuAI(api_key=self.api_key.strip())
        
        try:
            content = client.files.content(file_id)
            
            # 写入文件
            content.write_to_file(output_path)
            
            logger.info(f"批处理结果下载成功: {output_path}")
            return output_path
                
        except Exception as e:
            logger.error(f"下载批处理结果失败: {str(e)}")
            raise

    def parse_batch_results(self, results_file: str) -> List[Dict]:
        """
        解析批处理结果文件
        
        Args:
            results_file: 结果文件路径
            
        Returns:
            解析后的结果列表
        """
        logger.info(f"解析批处理结果文件: {results_file}")
        
        # 检查是否使用了虚拟ID（通过文件名判断）
        is_virtual_id = "virtual_id" in results_file
        
        results = []
        
        try:
            with open(results_file, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        # 解析每一行
                        data = json.loads(line)
                        
                        # 提取请求ID和响应内容
                        custom_id = data.get('custom_id', '')
                        
                        # 从custom_id中解析row_id
                        row_id = "unknown"
                        
                        # 尝试处理各种可能的自定义ID格式
                        if custom_id.startswith("request-row"):
                            # 处理格式 "request-row{idx}"
                            row_id = custom_id.replace("request-row", "")
                        elif '-' in custom_id and custom_id.split('-')[0].isdigit():
                            # 处理格式 "row_id-truncated"
                            row_id = custom_id.split('-')[0]
                        else:
                            # 其他格式，直接使用custom_id
                            row_id = custom_id
                            
                        # 确保row_id是字符串
                        row_id = str(row_id)
                        
                        # 获取响应内容
                        response_data = data.get('response', {})
                        body = response_data.get('body', {})
                        choices = body.get('choices', [])
                        
                        if choices and len(choices) > 0:
                            content = choices[0].get('message', {}).get('content', '')
                            
                            # 处理响应内容
                            extracted_terms = self._process_api_response(content)
                            
                            # 添加行ID到每个术语
                            for term in extracted_terms:
                                term['row_id'] = row_id
                                
                            # 添加到结果列表
                            results.extend(extracted_terms)
                    except Exception as e:
                        logger.warning(f"解析批处理结果行失败: {str(e)}")
                        continue
                        
            # 保存解析结果到JSON文件
            if self.batch_id:
                try:
                    # 确保输出目录存在
                    output_dir = os.path.join(self.config.get('output_dir', 'results'), self.batch_id)
                    os.makedirs(output_dir, exist_ok=True)
                    
                    # 保存解析结果
                    results_path = os.path.join(output_dir, f"{self.batch_id}_parsed_results.json")
                    with open(results_path, 'w', encoding='utf-8') as f:
                        json.dump(results, f, ensure_ascii=False, indent=2)
                        
                    logger.info(f"解析结果已保存到: {results_path}")
                except Exception as e:
                    logger.error(f"保存解析结果时出错: {str(e)}")
                    
        except Exception as e:
            logger.error(f"解析批处理结果文件失败: {str(e)}")
            raise
            
        logger.info(f"批处理结果解析完成，共提取 {len(results)} 个术语")
        return results
    
    def _process_api_response(self, content: str) -> List[Dict[str, str]]:
        """处理API响应内容"""
        if not content or content.strip() == "":
            logger.warning("⚠️ 收到空响应")
            return []
        
        logger.debug(f"📝 原始响应内容: {content[:100]}...")
        
        # 尝试多种方法解析JSON
        result = None
        
        # 方法1: 使用高级JSON处理工具
        if HAS_JSON_UTILS:
            try:
                result = fix_json_response(content, expected_keys=["terms"])
            except Exception:
                pass
                
        # 方法2: 标准JSON解析
        if result is None:
            try:
                # 清理内容
                cleaned_content = re.sub(r'```json|\s*```', '', content)
                cleaned_content = re.sub(r'//.*?$', '', cleaned_content, flags=re.MULTILINE)
                result = json.loads(cleaned_content)
            except json.JSONDecodeError:
                pass
        
        # 方法3: 正则表达式提取
        if result is None:
            try:
                json_pattern = r'\{[^{}]*(((?R)[^{}]*)*)\}|\[[^\[\]]*(((?R)[^\[\]]*)*)\]'
                for match in re.finditer(json_pattern, content, re.DOTALL):
                    try:
                        result = json.loads(match.group(0))
                        break
                    except:
                        continue
            except:
                pass
        
        # 处理结果
        if result is None:
            logger.warning("⚠️ 所有解析方法都失败，返回空列表")
            return []
            
        # 提取术语列表
        terms = []
        if "terms" in result:
            terms = result["terms"]
        elif isinstance(result, list):
            terms = result
            
        # 规范化术语
        return [self._normalize_term(term) for term in terms if term]
    
    def _normalize_term(self, term_data: Any) -> Dict[str, str]:
        """规范化术语数据"""
        # 如果是字符串，创建基本术语对象
        if isinstance(term_data, str):
            return {"term": term_data}
            
        # 如果既不是字符串也不是字典，尝试转换为字符串
        if not isinstance(term_data, dict):
            try:
                return {"term": str(term_data)}
            except:
                return {"term": "未知术语"}
        
        # 规范化字段
        normalized = {}
        
        # 字段映射表
        field_mappings = {
            "term": ["term", "术语", "name", "名称", "术语名称", "术语名"],
            "type": ["type", "类型", "术语类型"],
            "context": ["context", "上下文", "句子", "sentence"]
        }
        
        # 查找和映射字段
        for target_field, possible_keys in field_mappings.items():
            for key in possible_keys:
                if key in term_data and term_data[key]:
                    normalized[target_field] = str(term_data[key])
                    break
        
        # 确保至少有术语字段
        if "term" not in normalized:
            # 如果没有找到术语字段，使用第一个非空字段
            for key, value in term_data.items():
                if value:
                    normalized["term"] = str(value)
                    break
            
            # 如果仍然没有术语字段，使用默认值
            if "term" not in normalized:
                normalized["term"] = "未知术语"
            
        return normalized

    def _process_input_data(self, excel_file: str) -> Tuple[pd.DataFrame, str, List[str]]:
        """
        处理输入Excel数据
        
        Args:
            excel_file: Excel文件路径
            
        Returns:
            DataFrame, ID列, 文本列列表
        """
        # 读取Excel文件
        df = self.read_excel(excel_file)
        if df.empty:
            logger.error("Excel文件为空或格式错误")
            return pd.DataFrame(), "", []
            
        # 识别列
        id_col, text_cols = self.identify_columns(df)
        
        # 检测虚拟ID - 单列Excel文件的特殊处理
        if len(df.columns) == 1:
            logger.info("处理单列Excel文件 - 将添加索引作为虚拟ID")
            # 添加一个虚拟ID列，使用数据框的索引作为ID值
            df["virtual_id"] = df.index.astype(str)
            id_col = "virtual_id" if not id_col else id_col
            
        # 检查文本列是否存在    
        if not text_cols:
            logger.error("未找到有效的文本列")
            return pd.DataFrame(), "", []
            
        logger.info(f"识别到ID列: {id_col}, 文本列: {', '.join(text_cols)}")
        
        return df, id_col, text_cols
        
    def read_excel(self, file_path: str) -> pd.DataFrame:
        """
        读取Excel文件，返回DataFrame
        
        Args:
            file_path: Excel文件路径
            
        Returns:
            DataFrame
        """
        try:
            logger.info(f"读取Excel文件: {file_path}")
            
            # 检测文件类型
            if file_path.lower().endswith('.csv'):
                df = pd.read_csv(file_path, encoding='utf-8-sig')
            else:
                df = pd.read_excel(file_path)
            
            if df.empty:
                logger.warning(f"Excel文件为空: {file_path}")
            else:
                logger.info(f"成功读取文件: {len(df)}行 x {len(df.columns)}列")
                
            return df
        except Exception as e:
            logger.error(f"读取文件失败: {str(e)}")
            return pd.DataFrame()
    
    def identify_columns(self, df: pd.DataFrame, id_col: Optional[str] = None) -> Tuple[str, List[str]]:
        """
        识别Excel中的ID列和文本列
        
        Args:
            df: Excel数据DataFrame
            id_col: 指定的ID列名，如果为None则自动识别
            
        Returns:
            (ID列名, 文本列名列表)
        """
        if df.empty:
            return "", []
            
        # 确保所有列名都是字符串
        df.columns = df.columns.astype(str)
        
        # 特殊处理：单列Excel文件
        if len(df.columns) == 1:
            logger.info("检测到单列Excel文件")
            # 对于单列文件，创建一个虚拟ID列，将唯一列作为文本列
            column_name = df.columns[0]
            logger.info(f"单列文件：将唯一列 {column_name} 作为文本列处理")
            # 返回一个虚拟ID列名和实际的文本列名
            return "virtual_id", [column_name]
        
        # 如果指定了ID列，验证它是否存在
        if id_col is not None and id_col in df.columns:
            identified_id_col = id_col
        else:
            # 自动识别ID列
            # 优先级：(1) 包含"id"的列名 (2) 第一列
            id_candidates = [col for col in df.columns if 'id' in col.lower()]
            
            if id_candidates:
                identified_id_col = id_candidates[0]
            else:
                # 使用第一列作为ID列
                identified_id_col = df.columns[0]
        
        # 识别文本列：除ID列外的所有列
        text_cols = [col for col in df.columns if col != identified_id_col]
        
        # 过滤掉可能不包含有用文本的列
        useful_text_cols = []
        for col in text_cols:
            # 检查列是否为可能的文本列
            sample = df[col].dropna().astype(str).iloc[:10] if len(df) > 10 else df[col].dropna().astype(str)
            
            # 检查是否有足够的文本内容
            if any(len(str(text)) > 5 for text in sample):
                useful_text_cols.append(col)
        
        if not useful_text_cols:
            # 如果没有找到有用的文本列，退回到使用所有非ID列
            logger.warning("未找到有用的文本列，将使用所有非ID列")
            useful_text_cols = text_cols
            
        logger.info(f"识别到ID列: {identified_id_col}")
        logger.info(f"识别到文本列: {', '.join(useful_text_cols)}")
        
        return identified_id_col, useful_text_cols

    def export_to_excel(self, results: List[Dict], output_file: str) -> Dict[str, Any]:
        """将提取的术语导出到Excel文件"""
        logger.info(f"导出 {len(results)} 个术语到: {output_file}")
        
        # 确保输出目录存在
        output_dir = os.path.dirname(output_file)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
        
        # 创建输出DataFrame
        df = pd.DataFrame(results)
        
        # 确保列顺序一致
        required_columns = ["row_id", "term", "type", "context"]
        for col in required_columns:
            if col not in df.columns:
                df[col] = ""
        
        # 重新排序列
        column_order = ["row_id"] + [col for col in df.columns if col != "row_id"]
        df = df[column_order]
        
        try:
            # 尝试保存为Excel格式
            logger.info(f"导出为Excel格式: {output_file}")
            df.to_excel(output_file, index=False)
            logger.info(f"成功导出到Excel: {output_file}")
            return {"success": True, "message": "导出Excel成功", "output_file": output_file}
                
        except Exception as e:
            logger.warning(f"导出Excel格式失败，尝试CSV格式: {str(e)}")
            
            # 尝试CSV格式
            try:
                csv_file = output_file.replace('.xlsx', '.csv')
                df.to_csv(csv_file, index=False, encoding='utf-8-sig')
                logger.info(f"成功导出为CSV: {csv_file}")
                return {"success": True, "message": "导出为CSV格式", "output_file": csv_file}
            except Exception as csv_e:
                logger.error(f"导出结果失败: {str(csv_e)}")
                return {"success": False, "message": f"导出失败: {str(e)}", "output_file": ""}

    def test_api_key(self) -> bool:
        """
        测试API密钥是否有效
        
        Returns:
            是否有效
        """
        if not self.api_key:
            logger.error("API密钥为空")
            return False
            
        if not HAS_ZHIPUAI:
            logger.error("未安装zhipuai库，无法测试API密钥")
            return False
            
        try:
            # 使用zhipuai库测试
            client = ZhipuAI(api_key=self.api_key.strip())
            
            # 发送简单的请求
            response = client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "user", "content": "你好"}
                ],
                max_tokens=10
            )
            
            logger.info("API密钥测试成功")
            return True
                
        except Exception as e:
            logger.error(f"API密钥测试失败: {str(e)}")
            return False

    def _extract_via_zhipuai(self, prompt, model=None):
        """使用智谱AI批量处理接口进行提取"""
        try:
            client = ZhipuAI(api_key=self.api_key)
            response = client.chat.completions.create(
                model=model or self.model,
                temperature=0.3,
                messages=[
                    {"role": "system", "content": "你是一个术语提取专家，擅长从文本中提取专业术语和专有名词。"},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=2000  # 使用固定的max_tokens值
            )
            return response.choices[0].message.content
        except Exception as e:
            logging.error(f"智谱API调用失败: {str(e)}")
            return None

    def process_file(self, file_path, id_column=None, text_column=None) -> Tuple[pd.DataFrame, str]:
        """
        处理单个文件，提取术语并返回结果
        
        Args:
            file_path: 文件路径
            id_column: ID列名
            text_column: 文本列名
            
        Returns:
            元组(提取的术语DataFrame, 结果目录)
        """
        # 生成唯一的批次ID，基于文件名和时间戳
        filename = os.path.basename(file_path)
        self.batch_id = f"{os.path.splitext(filename)[0]}_{int(time.time())}"
        logger.info(f"开始处理文件 {file_path}, 批次ID: {self.batch_id}")
        
        try:
            # 读取Excel或CSV文件
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path)
            else:
                df = pd.read_excel(file_path)
                
            logger.info(f"成功读取文件，包含 {len(df)} 行数据，列: {df.columns.tolist()}")
            
            is_virtual_id = False
            
            # 检查是否只有一列
            single_column = len(df.columns) == 1
            
            # 如果只有一列且没有指定列，将其视为文本列并创建虚拟ID
            if single_column:
                text_column = df.columns[0]
                if id_column is None:
                    logger.info(f"单列文件检测到，将使用虚拟ID并将列 '{text_column}' 设为文本列")
                    # 创建虚拟ID列
                    df['virtual_id'] = [f"row_{i}" for i in range(len(df))]
                    id_column = 'virtual_id'
                    is_virtual_id = True
                else:
                    # 如果用户明确指定了ID列，我们需要检查它是否是唯一列
                    if id_column == df.columns[0]:
                        logger.warning(f"唯一的列 '{id_column}' 被指定为ID列，但没有文本列可用")
                        logger.info(f"将创建虚拟文本列，复制ID列的内容")
                        # 创建虚拟文本列
                        df['virtual_text'] = df[id_column].copy()
                        text_column = 'virtual_text'
                    
            # 检查列是否存在
            if id_column and id_column not in df.columns:
                if id_column == 'virtual_id':
                    logger.info("使用虚拟ID列")
                    df['virtual_id'] = [f"row_{i}" for i in range(len(df))]
                    is_virtual_id = True
                else:
                    logger.error(f"ID列 '{id_column}' 在文件中不存在")
                    raise ValueError(f"ID列 '{id_column}' 在文件中不存在")
                    
            if text_column and text_column not in df.columns:
                logger.error(f"文本列 '{text_column}' 在文件中不存在")
                raise ValueError(f"文本列 '{text_column}' 在文件中不存在")
                
            # 如果没有指定列，尝试猜测
            if not id_column:
                potential_id_cols = [col for col in df.columns if 'id' in col.lower()]
                if potential_id_cols:
                    id_column = potential_id_cols[0]
                    logger.info(f"自动选择 '{id_column}' 为ID列")
                else:
                    # 如果找不到ID列，创建虚拟ID
                    logger.info("无法识别ID列，将创建虚拟ID")
                    df['virtual_id'] = [f"row_{i}" for i in range(len(df))]
                    id_column = 'virtual_id'
                    is_virtual_id = True
                    
            if not text_column:
                # 排除ID列，选择第一个非ID列作为文本列
                text_candidates = [col for col in df.columns if col != id_column]
                if text_candidates:
                    text_column = text_candidates[0]
                    logger.info(f"自动选择 '{text_column}' 为文本列")
                else:
                    logger.error("无法识别文本列，请明确指定")
                    raise ValueError("无法识别文本列，请明确指定")
            
            # 确保输出目录存在
            output_dir = os.path.join(self.config.get('output_dir', 'results'), self.batch_id)
            os.makedirs(output_dir, exist_ok=True)
            
            # 准备批处理数据
            batch_data = []
            for idx, row in df.iterrows():
                row_id = str(row[id_column])
                text = str(row[text_column])
                
                # 清理文本
                if text:
                    text = re.sub(r'\s+', ' ', text).strip()
                    
                if not text:
                    logger.warning(f"行 {idx} (ID: {row_id}) 文本为空，跳过")
                    continue
                    
                # 准备请求数据
                custom_id = f"{row_id}"
                if len(custom_id) > 60:  # API可能对ID长度有限制
                    custom_id = f"{row_id[:25]}-{row_id[-25:]}"
                    
                prompt = self._create_extraction_prompt(text)
                
                batch_data.append({
                    "custom_id": custom_id,
                    "prompt": prompt
                })
                
            logger.info(f"准备了 {len(batch_data)} 条记录进行处理")
            
            # 保存批处理输入数据
            batch_input_file = os.path.join(output_dir, f"{self.batch_id}_input.json")
            with open(batch_input_file, 'w', encoding='utf-8') as f:
                json.dump(batch_data, f, ensure_ascii=False, indent=2)
            logger.info(f"批处理输入数据已保存到: {batch_input_file}")
            
            # 上传批处理文件并获取结果
            results_file = self.upload_batch_file(batch_input_file)
            
            if not results_file:
                logger.error("批处理失败，未返回结果文件")
                return pd.DataFrame(), output_dir
                
            # 解析结果
            parsed_results = self.parse_batch_results(results_file)
            
            if not parsed_results:
                logger.error("解析批处理结果失败")
                return pd.DataFrame(), output_dir
                
            # 创建结果DataFrame
            result_data = []
            for term_info in parsed_results:
                row_id = term_info.get('row_id', 'unknown')
                
                # 查找原始文本
                original_text = ""
                if is_virtual_id:
                    # 对于虚拟ID，尝试从row_id中提取行号
                    try:
                        if row_id.startswith('row_'):
                            row_idx = int(row_id.replace('row_', ''))
                            if 0 <= row_idx < len(df):
                                original_text = df.iloc[row_idx][text_column]
                    except:
                        pass
                else:
                    # 对于真实ID，通过ID查找文本
                    try:
                        matches = df[df[id_column] == row_id]
                        if not matches.empty:
                            original_text = matches.iloc[0][text_column]
                    except:
                        pass
                
                result_data.append({
                    'row_id': row_id,
                    'original_text': original_text,
                    'term': term_info.get('term', ''),
                    'normalized_term': term_info.get('normalized', ''),
                    'pos': term_info.get('pos', ''),
                    'frequency': term_info.get('frequency', 1),
                    'score': term_info.get('score', 0.0)
                })
                
            results_df = pd.DataFrame(result_data)
            
            # 导出结果到Excel
            excel_file = os.path.join(output_dir, f"{self.batch_id}_results.xlsx")
            self.export_to_excel(results_df, excel_file)
            
            logger.info(f"文件处理完成，结果已保存到: {excel_file}")
            return results_df, output_dir
            
        except Exception as e:
            logger.error(f"处理文件时出错: {str(e)}")
            logger.exception(e)
            raise

def main():
    """主函数"""
    try:
        import argparse
        parser = argparse.ArgumentParser(description='术语提取工具 - 批处理模式')
        parser.add_argument('--input', help='输入Excel文件路径')
        parser.add_argument('--output', help='输出Excel文件路径')
        parser.add_argument('--api-key', help='智谱API密钥')
        args = parser.parse_args()
        
        # 打印当前工作目录和参数
        logger.info(f"当前工作目录: {os.getcwd()}")
        logger.info(f"输入文件: {args.input}")
        logger.info(f"输出文件: {args.output}")
        
        # 检查zhipuai库是否已安装
        if not HAS_ZHIPUAI:
            logger.error("未安装zhipuai库，无法使用批处理功能")
            print("错误: 未安装zhipuai库。请使用 'pip install zhipuai' 安装")
            return
        
        # 如果未指定输入文件，提示并退出
        if not args.input:
            print("请使用--input参数指定输入Excel文件")
            return
            
        # 设置默认输出文件
        output_file = args.output or "extracted_terms.xlsx"
        
        # 获取API密钥
        api_key = args.api_key
        if not api_key:
            api_key = input("请输入智谱API密钥: ")
            
        # 创建术语提取器
        extractor = TermExtractor(api_key=api_key)
    
    # 处理数据
        extractor.process_data(args.input, output_file=output_file)
        
        print(f"术语提取完成，结果已保存到: {output_file}")
        
    except Exception as e:
        logger.error(f"运行出错: {str(e)}")
        print(f"运行出错: {str(e)}")


if __name__ == "__main__":
    main() 