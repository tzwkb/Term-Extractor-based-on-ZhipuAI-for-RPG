"""
数据预处理器模块
用于清洗和预处理文本数据
"""

import os
import re
import pandas as pd
import logging
from typing import List, Optional, Tuple, Dict, Any, Union
from pathlib import Path

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataPreprocessor:
    """数据预处理器，提供文本清洗功能"""
    
    def __init__(self):
        """初始化数据预处理器"""
        # 初始化正则表达式模式
        self.patterns = {
            'numbers': r'\d+',                              # 阿拉伯数字
            'html_tags': r'<[^>]+>',                        # HTML标签
            'hyperlinks': r'https?://\S+|www\.\S+',         # 超链接
            'punctuation': r'[^\w\s]',                      # 标点符号
            'placeholders': r'\{\{.*?\}\}|\{\%.*?\%\}',     # 占位符，如 {{var}} 或 {% include %}
            'markdown_links': r'\[([^\]]+)\]\(([^)]+)\)',   # Markdown格式链接 [text](url)
            'email': r'\S+@\S+',                            # 电子邮件地址
            'multiple_spaces': r'\s+',                      # 多余空格
        }
        
        # 默认清洗选项
        self.default_options = {
            'numbers': True,
            'html_tags': True,
            'hyperlinks': True,
            'punctuation': True,
            'placeholders': True,
            'markdown_links': True,
            'email': False,
            'game_text': True,
            'multiple_spaces': True
        }
        
        # 游戏文本特殊处理模式
        self.game_text_patterns = {
            'hyperlink_tags': r'\[\s*<a id="[^"]*" HyperLinkStr="[^"]*"[^>]*>([^<]*)<\/>\s*\]',  # 游戏超链接
            'closed_html_tags': r'<[a-zA-Z0-9_]+>([^<]*)<\/>',  # 闭合HTML标签
            'unclosed_html_tags': r'<[a-zA-Z0-9_]+ [^>]*>([^<]*)<\/>',  # 未闭合HTML标签
            'game_hyperlinks': r'a\s+idTMHyperlink\s+HyperLinkStr\s+styleAchievementLink(\w+)',  # 游戏超链接
            'color_marks': r'\^[0-9A-Fa-f]{6}',  # 颜色标记 ^RRGGBB
            'color_format': r'<(color|colou?r)=[\'"]?#?[0-9A-Fa-f]{3,6}[\'"]?>(.*?)</\1>',  # 颜色格式标记
        }
        
        # 颜色词列表 - 按长度排序
        self.color_words = [
            'orange', 'green', 'purple', 'magenta', 'yellow', 'lavender', 'maroon', 'coral', 
            'beige', 'olive', 'white', 'black', 'brown', 'navy', 'grey', 'gray', 'pink', 
            'teal', 'lime', 'mint', 'blue', 'cyan', 'gold', 'red'
        ]
        
        # 特殊游戏文本组合
        self.special_game_text = [
            (r'(?i)orange混沌日冕', r'混沌日冕'),
            (r'(?i)orange血魔契约', r'血魔契约'),
            (r'(?i)orange神堂傲视', r'神堂傲视'),
            (r'(?i)orange臂章珍宝箱', r'臂章珍宝箱'),
            (r'(?i)orange臂章', r'臂章'),
            (r'(?i)orange蓝钻', r'蓝钻'),
            (r'(?i)green的', r'的')
        ]
        
        logger.info("数据预处理器初始化完成")
        
    def preprocess_excel(self, input_file: str, output_file: str, id_col: Optional[str] = None, 
                         apply_deduplication: bool = False) -> str:
        """
        预处理Excel文件
        
        Args:
            input_file: 输入Excel文件路径
            output_file: 输出Excel文件路径
            id_col: ID列名，默认为None（自动识别）
            apply_deduplication: 是否应用消消乐式去重，默认为False
            
        Returns:
            输出Excel文件路径
        """
        try:
            logger.info(f"开始预处理Excel文件: {input_file}")
            
            # 标准化路径，确保使用一致的斜杠格式
            input_file = os.path.normpath(input_file)
            output_file = os.path.normpath(output_file)
            
            # 确保输出目录存在
            output_dir = os.path.dirname(output_file)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir, exist_ok=True)
                logger.info(f"创建输出目录: {output_dir}")
            
            # 读取Excel文件
            df = self.read_excel(input_file)
            if df.empty:
                logger.error("Excel文件为空或读取失败")
                return ""
            
            # 识别列（ID列和需要处理的文本列）
            id_column, text_columns = self.identify_columns(df)
            logger.info(f"识别到ID列: {id_column}, 文本列: {text_columns}")
            
            # 检测单列Excel文件
            is_single_column = len(df.columns) == 1
            if is_single_column:
                logger.info("检测到单列Excel文件，将使用特殊处理")
                # 对于单列文件，我们将ID列也作为文本进行处理
                # text_columns将为空，clean_text_columns方法将特殊处理
            
            # 记录原始行数和文本长度
            original_row_count = len(df)
            
            # 计算原始文本长度
            if is_single_column:
                # 单列文件：使用ID列作为文本计算长度
                original_text_length = df[id_column].astype(str).str.len().sum()
            else:
                # 双列文件：使用文本列计算长度
                original_text_length = sum(df[col].astype(str).str.len().sum() for col in text_columns)
            
            # 清洗文本列
            df = self.clean_text_columns(df, text_columns, id_column)
            
            # 记录清洗后的文本长度
            if is_single_column:
                # 单列文件：查找清洗后的列
                if f"cleaned_{id_column}" in df.columns:
                    cleaned_text_length = df[f"cleaned_{id_column}"].astype(str).str.len().sum()
                elif "cleaned_text" in df.columns:
                    cleaned_text_length = df["cleaned_text"].astype(str).str.len().sum()
                else:
                    cleaned_text_length = df[id_column].astype(str).str.len().sum()
            else:
                # 双列文件：使用原文本列
                cleaned_text_length = sum(df[col].astype(str).str.len().sum() for col in text_columns)
            
            text_reduction = original_text_length - cleaned_text_length
            text_reduction_percent = (text_reduction / original_text_length * 100) if original_text_length > 0 else 0
            logger.info(f"文本清洗减少了 {text_reduction} 个字符 ({text_reduction_percent:.2f}%)")
            
            # 存储处理统计信息
            stats = {
                "original_rows": original_row_count,
                "original_text_length": original_text_length,
                "cleaned_text_length": cleaned_text_length,
                "text_reduction": text_reduction,
                "text_reduction_percent": text_reduction_percent,
                "is_single_column": is_single_column
            }
            
            removed_ids = []  # 存储被删除的ID
            
            # 应用消消乐式去重（如果需要）
            if apply_deduplication:
                logger.info("开始应用消消乐式去重")
                try:
                    # 记录去重前的状态
                    pre_dedup_row_count = len(df)
                    
                    # 计算去重前的文本长度
                    if is_single_column:
                        # 单列文件：查找清洗后的列
                        if f"cleaned_{id_column}" in df.columns:
                            pre_dedup_text_length = df[f"cleaned_{id_column}"].astype(str).str.len().sum()
                        elif "cleaned_text" in df.columns:
                            pre_dedup_text_length = df["cleaned_text"].astype(str).str.len().sum()
                        else:
                            pre_dedup_text_length = df[id_column].astype(str).str.len().sum()
                            
                        # 为单列文件适配去重所需的文本列
                        if f"cleaned_{id_column}" in df.columns:
                            dedup_text_columns = [f"cleaned_{id_column}"]
                        elif "cleaned_text" in df.columns:
                            dedup_text_columns = ["cleaned_text"]
                        else:
                            dedup_text_columns = [id_column]
                    else:
                        # 双列文件：使用原文本列
                        pre_dedup_text_length = sum(df[col].astype(str).str.len().sum() for col in text_columns)
                        dedup_text_columns = text_columns
                    
                    # 应用消消乐去重
                    df = self.apply_progressive_deduplication(df, id_column, dedup_text_columns)
                    logger.info("消消乐式去重完成")
                    
                    # 记录去重后的状态
                    if is_single_column:
                        # 单列文件
                        if f"cleaned_{id_column}" in df.columns:
                            post_dedup_text_length = df[f"cleaned_{id_column}"].astype(str).str.len().sum()
                        elif "cleaned_text" in df.columns:
                            post_dedup_text_length = df["cleaned_text"].astype(str).str.len().sum()
                        else:
                            post_dedup_text_length = df[id_column].astype(str).str.len().sum()
                    else:
                        # 双列文件
                        post_dedup_text_length = sum(df[col].astype(str).str.len().sum() for col in text_columns)
                    
                    dedup_text_reduction = pre_dedup_text_length - post_dedup_text_length
                    dedup_text_percent = (dedup_text_reduction / pre_dedup_text_length * 100) if pre_dedup_text_length > 0 else 0
                    logger.info(f"消消乐去重减少了 {dedup_text_reduction} 个字符 ({dedup_text_percent:.2f}%)")
                    
                    # 添加去重统计
                    stats.update({
                        "pre_dedup_text_length": pre_dedup_text_length,
                        "post_dedup_text_length": post_dedup_text_length,
                        "dedup_text_reduction": dedup_text_reduction,
                        "dedup_text_reduction_percent": dedup_text_percent
                    })
                    
                    # 删除完全相同的条目，只保留首条
                    logger.info("开始删除重复内容条目")
                    duplicates_df = self._remove_duplicate_entries(df, id_column, dedup_text_columns)
                    
                    # 记录被删除的ID
                    duplicate_count = 0
                    if not duplicates_df.empty:
                        duplicate_count = len(duplicates_df)
                        removed_ids.extend(duplicates_df[id_column].tolist())
                        logger.info(f"删除了 {duplicate_count} 条重复内容的条目")
                    
                    # 删除空内容的条目
                    logger.info("开始删除空内容条目")
                    empty_df = self._remove_empty_entries(df, id_column, dedup_text_columns)
                    
                    # 记录被删除的ID
                    empty_count = 0
                    if not empty_df.empty:
                        empty_count = len(empty_df)
                        removed_ids.extend(empty_df[id_column].tolist())
                        logger.info(f"删除了 {empty_count} 条空内容的条目")
                    
                    # 更新统计信息
                    stats.update({
                        "duplicate_entries_removed": duplicate_count,
                        "empty_entries_removed": empty_count,
                        "total_entries_removed": duplicate_count + empty_count
                    })
                    
                except Exception as e:
                    logger.error(f"消消乐式去重或条目删除失败: {str(e)}")
                    import traceback
                    logger.error(traceback.format_exc())
                    # 继续处理，不中断
                
                # 输出处理结果统计
                final_row_count = len(df)
                rows_removed = original_row_count - final_row_count
                rows_removed_percent = (rows_removed / original_row_count * 100) if original_row_count > 0 else 0
                
                logger.info(f"共删除 {rows_removed} 条记录 ({rows_removed_percent:.2f}%)，"
                           f"保留 {final_row_count} 条记录")
                
                # 最终文本长度
                if is_single_column:
                    # 单列文件
                    if f"cleaned_{id_column}" in df.columns:
                        final_text_length = df[f"cleaned_{id_column}"].astype(str).str.len().sum()
                    elif "cleaned_text" in df.columns:
                        final_text_length = df["cleaned_text"].astype(str).str.len().sum()
                    else:
                        final_text_length = df[id_column].astype(str).str.len().sum()
                else:
                    # 双列文件
                    final_text_length = sum(df[col].astype(str).str.len().sum() for col in text_columns)
                
                total_text_reduction = original_text_length - final_text_length
                total_text_reduction_percent = (total_text_reduction / original_text_length * 100) if original_text_length > 0 else 0
                
                logger.info(f"总计减少 {total_text_reduction} 个字符 ({total_text_reduction_percent:.2f}%)")
                
                # 更新最终统计信息
                stats.update({
                    "final_rows": final_row_count,
                    "rows_removed": rows_removed,
                    "rows_removed_percent": rows_removed_percent,
                    "final_text_length": final_text_length,
                    "total_text_reduction": total_text_reduction,
                    "total_text_reduction_percent": total_text_reduction_percent
                })
                
                # 如果有删除的ID，保存为单独的Excel
                if removed_ids:
                    removed_ids_file = self._save_removed_ids(removed_ids, id_column, output_file)
                    stats["removed_ids_file"] = removed_ids_file
            
            # 导出为Excel
            output_path = self.export_to_excel(df, output_file)
            
            # 如果成功导出，也保存统计信息
            if output_path:
                stats_file = self._save_stats(stats, output_file)
                logger.info(f"处理统计信息已保存至: {stats_file}")
            
            logger.info(f"预处理完成，结果已保存至: {output_path}")
            
            return output_path
        except Exception as e:
            logger.error(f"预处理过程中发生错误: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return ""
    
    def read_excel(self, file_path: str) -> pd.DataFrame:
        """
        读取Excel文件
        
        Args:
            file_path: Excel文件路径
            
        Returns:
            pandas DataFrame对象
        """
        errors = []
        
        # 尝试所有可能的引擎和组合
        engines_to_try = [
            {'engine': 'openpyxl', 'description': 'openpyxl引擎(推荐用于.xlsx)'},
            {'engine': 'xlrd', 'description': 'xlrd引擎(用于.xls)'},
            {'engine': None, 'description': 'pandas默认引擎'},
        ]
        
        for engine_info in engines_to_try:
            engine = engine_info['engine']
            description = engine_info['description']
            
            try:
                if engine:
                    logger.info(f"尝试使用{description}读取文件: {file_path}")
                    df = pd.read_excel(file_path, engine=engine)
                else:
                    logger.info(f"尝试使用{description}读取文件: {file_path}")
                    df = pd.read_excel(file_path)
                    
                # 如果成功读取
                if not df.empty:
                    logger.info(f"✓ 使用{description}成功读取: {len(df)}行 × {len(df.columns)}列")
                    return df
                else:
                    logger.warning(f"使用{description}读取成功但文件为空")
                    errors.append(f"{description}读取成功但文件为空")
            except Exception as e:
                error_msg = f"使用{description}读取失败: {str(e)}"
                logger.warning(error_msg)
                errors.append(error_msg)
        
        # 如果所有尝试都失败
        error_details = "\n".join(errors)
        logger.error(f"所有引擎都无法读取Excel文件: {file_path}\n错误详情:\n{error_details}")
        raise Exception(f"无法读取Excel文件，请确保文件格式正确且未被损坏。错误详情:\n{error_details}")
    
    def identify_columns(self, df: pd.DataFrame) -> Tuple[Optional[str], List[str]]:
        """
        自动识别ID列和文本内容列
        
        Args:
            df: 输入数据DataFrame
            
        Returns:
            Tuple[Optional[str], List[str]]: (ID列名, 文本列名列表)
        """
        # 如果DataFrame为空，返回空结果
        if df.empty:
            return None, []
            
        # 特殊处理：单列Excel文件
        if len(df.columns) == 1:
            logger.info("检测到单列Excel文件")
            # 对于单列文件，创建一个虚拟ID列，将唯一列作为文本列
            column_name = df.columns[0]
            logger.info(f"单列文件：将唯一列 {column_name} 作为文本列处理")
            # 返回一个虚拟ID和实际的文本列
            return "virtual_id", [column_name]
            
        # 尝试识别ID列
        id_column = self._identify_id_column(df)
        
        # 尝试识别文本列
        text_columns = self._identify_text_columns(df)
        
        # 如果找不到文本列，尝试更宽松的方式
        if not text_columns:
            text_columns = self._identify_text_columns_relaxed(df)
        
        # 确保ID列不在文本列中
        if id_column and id_column in text_columns:
            text_columns.remove(id_column)
            
        return id_column, text_columns
        
    def _identify_id_column(self, df: pd.DataFrame) -> Optional[str]:
        """识别ID列"""
        # 常见的ID列名
        id_column_patterns = [
            r'^id$', r'^编号$', r'^序号$', r'^index$', r'^key$', 
            r'^\s*id\s*$', r'^\s*编号\s*$', r'^\s*序号\s*$',
            r'^id[_.][a-z0-9]+$', r'^id[0-9]+$', r'^.*[_.]id$',
            r'^row[_.]id$', r'^row[_.]key$', r'^row$', r'^source[_.]id$'
        ]
        
        # 尝试匹配ID列名
        for column in df.columns:
            col_name = str(column).lower()
            # 精确匹配ID列名
            for pattern in id_column_patterns:
                if re.match(pattern, col_name, re.IGNORECASE):
                    return column
        
        # 如果没有找到匹配的列名，检查是否有数值型且唯一值的列
        for column in df.columns:
            # 检查是否为数值型
            if pd.api.types.is_numeric_dtype(df[column]):
                # 检查唯一值比例
                unique_ratio = df[column].nunique() / len(df)
                # 如果唯一值比例高于0.9，认为是ID列
                if unique_ratio > 0.9:
                    return column
        
        # 如果找不到ID列，返回第一列
        try:
            first_col = df.columns[0]
            # 检查第一列是否合适作为ID
            if pd.api.types.is_numeric_dtype(df[first_col]) or df[first_col].nunique() / len(df) > 0.7:
                return first_col
        except:
            pass
            
        return None
        
    def _identify_text_columns(self, df: pd.DataFrame) -> List[str]:
        """识别文本内容列"""
        text_columns = []
        
        # 常见的文本列名
        text_column_patterns = [
            r'^text$', r'^content$', r'^description$', r'^desc$', 
            r'^文本$', r'^内容$', r'^描述$', r'^sentence$', r'^document$',
            r'^.*text$', r'^.*content$', r'^.*description$',
            r'^source[_.](text|content)$', r'^text[_.](content|body)$'
        ]
        
        # 尝试匹配文本列名
        pattern_matched = False
        for column in df.columns:
            col_name = str(column).lower()
            # 精确匹配文本列名
            for pattern in text_column_patterns:
                if re.match(pattern, col_name, re.IGNORECASE):
                    text_columns.append(column)
                    pattern_matched = True
                    break
        
        # 如果没有找到匹配的列名，检查内容
        if not pattern_matched:
            for column in df.columns:
                # 跳过纯数值列
                if pd.api.types.is_numeric_dtype(df[column]):
                    continue
                    
                # 检查是否为可能的文本列
                if self._is_text_column(df, column):
                    text_columns.append(column)
        
        return text_columns
    
    def _identify_text_columns_relaxed(self, df: pd.DataFrame) -> List[str]:
        """使用更宽松的条件识别文本列"""
        text_columns = []
        
        for column in df.columns:
            # 跳过纯数值列
            if pd.api.types.is_numeric_dtype(df[column]):
                continue
                
            # 检查是否为可能的文本列（宽松版）
            if self._is_text_column_relaxed(df, column):
                text_columns.append(column)
        
        # 如果仍然没有找到文本列，选择最长的非数值列
        if not text_columns:
            non_numeric_cols = [col for col in df.columns if not pd.api.types.is_numeric_dtype(df[col])]
            if non_numeric_cols:
                # 计算每列的平均文本长度
                col_lengths = {}
                for col in non_numeric_cols:
                    try:
                        col_lengths[col] = df[col].astype(str).apply(len).mean()
                    except:
                        col_lengths[col] = 0
                
                # 选择平均长度最长的列
                if col_lengths:
                    longest_col = max(col_lengths.items(), key=lambda x: x[1])[0]
                    text_columns.append(longest_col)
        
        return text_columns
    
    def _is_text_column(self, df: pd.DataFrame, column: str) -> bool:
        """检查是否为文本列（标准版）"""
        try:
            # 获取样本值
            sample = df[column].dropna().astype(str).head(100)
            if len(sample) == 0:
                return False
            
            # 计算平均字符长度
            avg_length = sum(len(str(val)) for val in sample) / len(sample)
            
            # 检查是否包含较长文本
            has_long_text = any(len(str(val)) > 15 for val in sample)
            
            # 判断是否为文本列
            return avg_length > 8 or has_long_text
        except:
            return False
    
    def _is_text_column_relaxed(self, df: pd.DataFrame, column: str) -> bool:
        """检查是否为文本列（宽松版）"""
        try:
            # 获取样本值
            sample = df[column].dropna().astype(str).head(100)
            if len(sample) == 0:
                return False
            
            # 计算平均字符长度
            avg_length = sum(len(str(val)) for val in sample) / len(sample)
            
            # 检查是否包含中等长度文本
            has_medium_text = any(len(str(val)) > 8 for val in sample)
            
            # 检查是否包含空格（可能是句子）
            has_spaces = any(' ' in str(val) for val in sample)
            
            # 判断是否为文本列（宽松条件）
            return avg_length > 5 or has_medium_text or has_spaces
        except:
            return False

    def clean_text_columns(self, df: pd.DataFrame, text_columns: List[str], 
                          id_column: Optional[str] = None, keep_original: bool = False) -> pd.DataFrame:
        """
        清洗文本列
        
        Args:
            df: 输入数据DataFrame
            text_columns: 文本列名列表
            id_column: ID列名（可选）
            keep_original: 是否保留原始列
            
        Returns:
            清洗后的DataFrame
        """
        # 处理虚拟ID的情况
        is_virtual_id = id_column == "virtual_id"
        
        # 如果是虚拟ID但DataFrame中没有此列，添加索引作为虚拟ID
        if is_virtual_id and "virtual_id" not in df.columns:
            df = df.copy()
            df["virtual_id"] = df.index.astype(str)
        
        # 创建新的DataFrame
        if keep_original:
            cleaned_df = df.copy()
        else:
            # 只保留ID列和文本列
            columns_to_keep = []
            if id_column and id_column in df.columns:
                columns_to_keep.append(id_column)
            columns_to_keep.extend(text_columns)
            
            # 单列Excel文件特殊处理
            if len(df.columns) == 1 or (is_virtual_id and len(df.columns) == 2):
                cleaned_df = df.copy()
            elif columns_to_keep:
                cleaned_df = df[columns_to_keep].copy()
            else:
                cleaned_df = df.copy()
        
        # 清洗文本列
        for col in text_columns:
            if col in cleaned_df.columns:
                # 创建新的清洗列名
                clean_col_name = f"cleaned_{col}" if keep_original else col
                
                # 清洗文本
                logger.info(f"清洗列: {col}")
                cleaned_df[clean_col_name] = cleaned_df[col].apply(
                    lambda x: self.clean_text(x) if pd.notna(x) else ""
                )
        
        # 对于单列文件时，如果没有text_columns，做特殊处理
        if not text_columns and id_column and id_column != "virtual_id" and id_column in cleaned_df.columns:
            # 清洗ID列，将其作为文本
            clean_col_name = f"cleaned_{id_column}" if keep_original else "cleaned_text"
            logger.info(f"单列Excel文件：清洗ID列作为文本: {id_column}")
            cleaned_df[clean_col_name] = cleaned_df[id_column].apply(
                lambda x: self.clean_text(x) if pd.notna(x) else ""
            )
        
        return cleaned_df
    
    def clean_text(self, text: Any, **options) -> str:
        """
        清洗文本
        
        Args:
            text: 输入文本
            options: 清洗选项
                - numbers: 是否清除数字
                - punctuation: 是否清除标点符号
                - html_tags: 是否清除HTML标签
                - hyperlinks: 是否清除超链接
                - markdown_links: 是否清除Markdown链接
                - email: 是否清除电子邮件
                - placeholders: 是否清除占位符
                - game_text: 是否清除游戏文本标记
                - multiple_spaces: 是否清除多余空格
                
        Returns:
            清洗后的文本
        """
        # 如果输入为空或None，返回空字符串
        if text is None or (isinstance(text, str) and not text.strip()):
            return ""
            
        # 如果输入不是字符串，转换为字符串
        if not isinstance(text, str):
            try:
                text = str(text)
            except:
                return ""
        
        # 合并选项，使用默认选项和传入的选项
        clean_options = dict(self.default_options)
        clean_options.update(options)
        
        # 如果文本太短，可能不需要清洗
        if len(text) <= 3:
            return text
            
        # 处理换行符，确保\n整体被删除
        text = re.sub(r'\\n', ' ', text)
            
        # 游戏文本特殊处理
        if clean_options.get('game_text', True):
            text = self.clean_game_text(text)
            
        # 清除HTML标签
        if clean_options.get('html_tags', True):
            # 保留标签内的文本内容
            # 处理常见的HTML标签
            text = re.sub(r'<[^>]*>', '', text)
            
            # 清除特殊颜色标记
            text = re.sub(r'<(green|red|blue|yellow|white|gray|black)>(.*?)</(green|red|blue|yellow|white|gray|black)>', r'\2', text)
            text = re.sub(r'<(green|red|blue|yellow|white|gray|black)>(.*?)</>', r'\2', text)
            text = re.sub(r'<col=(#[0-9a-fA-F]{6})>(.*?)</col>', r'\2', text)
        
        # 清除超链接
        if clean_options.get('hyperlinks', True):
            text = re.sub(r'https?://\S+|www\.\S+', '', text)
        
        # 清除Markdown链接
        if clean_options.get('markdown_links', True):
            text = re.sub(r'\[([^\]]+)\]\(([^)]+)\)', r'\1', text)
        
        # 清除电子邮件
        if clean_options.get('email', True):
            text = re.sub(r'\S+@\S+', '', text)
        
        # 清除占位符
        if clean_options.get('placeholders', True):
            text = re.sub(r'\{\{.*?\}\}|\{\%.*?\%\}|___+', '', text)
        
        # 清除数字
        if clean_options.get('numbers', True):
            text = re.sub(r'\d+', '', text)
        
        # 清除标点符号 - 但保留中文标点
        if clean_options.get('punctuation', True):
            # 这个正则表达式会移除英文标点但保留中文标点
            text = re.sub(r'[!"#$%&\'()*+,-./:;<=>?@[\\\]^_`{|}~]', '', text)
        
        # 清除多余空格
        if clean_options.get('multiple_spaces', True):
            text = re.sub(r'\s+', ' ', text)
            text = text.strip()
        
        return text
    
    def clean_game_text(self, text: str) -> str:
        """
        清洗游戏文本特有的格式
        
        Args:
            text: 游戏文本
            
        Returns:
            清洗后的文本
        """
        if not isinstance(text, str) or not text:
            return text if isinstance(text, str) else ""
            
        # 保护括号内的内容
        protected_count = 0
        protected_texts = {}
        
        def protect_match(match):
            nonlocal protected_count
            placeholder = f"__PROTECTED_{protected_count}__"
            protected_texts[placeholder] = match.group(0)
            protected_count += 1
            return placeholder
        
        # 处理中文括号，保留内部文本但移除括号本身
        # 处理【】格式的括号，保留内部文本
        text = re.sub(r'【(.*?)】', r'\1', text)
        # 处理［］格式的括号，保留内部文本
        text = re.sub(r'［(.*?)］', r'\1', text)
        # 处理〔〕格式的括号，保留内部文本
        text = re.sub(r'〔(.*?)〕', r'\1', text)
        # 处理〈〉格式的括号，保留内部文本
        text = re.sub(r'〈(.*?)〉', r'\1', text)
        # 处理《》格式的括号，保留内部文本
        text = re.sub(r'《(.*?)》', r'\1', text)
        # 处理「」格式的括号，保留内部文本
        text = re.sub(r'「(.*?)」', r'\1', text)
        # 处理『』格式的括号，保留内部文本 
        text = re.sub(r'『(.*?)』', r'\1', text)
        
        # 保护小括号内的内容 - 避免误删重要信息
        text = re.sub(r'\([^)]*\)', protect_match, text)
        
        # 处理特殊游戏文本组合
        for pattern, replacement in self.special_game_text:
            text = re.sub(pattern, replacement, text)
            
        # 处理各种特殊文本格式
        for pattern_name, pattern in self.game_text_patterns.items():
            if pattern_name in ['hyperlink_tags', 'closed_html_tags', 'unclosed_html_tags', 'game_hyperlinks']:
                text = re.sub(pattern, r'\1', text)
            else:
                text = re.sub(pattern, '', text)
                
        # 处理颜色词 - 移除前缀颜色
        for color in self.color_words:
            # 处理各种大小写形式
            text = re.sub(rf'\b{color}(?=\S)', '', text, flags=re.IGNORECASE)
        
        # 清除其他HTML/类HTML标签
        text = re.sub(r'<[^>]*>', '', text)
        
        # 恢复被保护的文本
        for placeholder, original in protected_texts.items():
            text = text.replace(placeholder, original)
            
        # 最后的清理，移除多余空格
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
    
    def export_to_excel(self, df: pd.DataFrame, output_file: str) -> str:
        """
        导出数据框为Excel文件
        
        Args:
            df: 要导出的数据框
            output_file: 输出文件路径
            
        Returns:
            输出文件的绝对路径
        """
        # 标准化路径
        output_file = os.path.normpath(output_file)
        logger.info(f"准备导出到: {output_file}")
        
        try:
            # 确保输出目录存在
            output_dir = os.path.dirname(output_file)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir, exist_ok=True)
                logger.info(f"创建输出目录: {output_dir}")
            
            # 尝试删除可能存在的老文件，避免权限问题
            if os.path.exists(output_file):
                try:
                    os.remove(output_file)
                    logger.info(f"已删除已存在的输出文件: {output_file}")
                except Exception as e:
                    logger.warning(f"无法删除已存在的输出文件: {str(e)}")
            
            # 保存为Excel
            logger.info(f"开始导出数据到Excel: {output_file}")
            df.to_excel(output_file, index=False)
            
            # 检查文件是否成功创建
            if os.path.exists(output_file):
                logger.info(f"成功导出Excel文件: {output_file}")
                # 返回绝对路径
                return os.path.abspath(output_file)
            else:
                logger.error(f"文件导出失败: 文件未创建 {output_file}")
                # 尝试导出为CSV作为备选方案
                return self._export_as_csv(df, output_file)
        except PermissionError as e:
            logger.error(f"导出Excel文件失败 - 权限错误: {str(e)}")
            # 尝试使用临时文件夹
            try:
                temp_dir = os.path.join(os.path.expanduser("~"), "AppData", "Local", "Temp", "term_extractor")
                os.makedirs(temp_dir, exist_ok=True)
                
                # 获取原文件名
                file_name = os.path.basename(output_file)
                temp_output = os.path.join(temp_dir, file_name)
                
                logger.info(f"尝试导出到临时位置: {temp_output}")
                df.to_excel(temp_output, index=False)
                
                logger.info(f"成功导出到临时位置: {temp_output}")
                return os.path.abspath(temp_output)
            except Exception as temp_error:
                logger.error(f"导出到临时位置也失败: {str(temp_error)}")
                # 尝试导出为CSV
                return self._export_as_csv(df, output_file)
        except Exception as e:
            logger.error(f"导出Excel文件失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            # 尝试导出为CSV作为备选方案
            return self._export_as_csv(df, output_file)
        
    def _export_as_csv(self, df: pd.DataFrame, excel_path: str) -> str:
        """
        当Excel导出失败时，尝试导出为CSV
        
        Args:
            df: 要导出的数据框
            excel_path: 原Excel路径
            
        Returns:
            CSV文件路径或空字符串
        """
        try:
            # 创建CSV路径
            csv_file = excel_path.replace('.xlsx', '.csv').replace('.xls', '.csv')
            if csv_file == excel_path:  # 如果文件名没有改变
                csv_file = excel_path + '.csv'
            
            # 尝试导出为CSV
            logger.info(f"尝试导出为CSV: {csv_file}")
            df.to_csv(csv_file, index=False)
            
            # 检查CSV文件是否创建成功
            if os.path.exists(csv_file):
                logger.info(f"已将数据导出为CSV文件: {csv_file}")
                return os.path.abspath(csv_file)
            else:
                logger.error(f"CSV文件导出失败: 文件未创建 {csv_file}")
                return ""
        except Exception as csv_error:
            logger.error(f"导出CSV文件也失败: {str(csv_error)}")
            import traceback
            logger.error(traceback.format_exc())
            return ""
    
    def apply_progressive_deduplication(self, df: pd.DataFrame, id_column: str, text_columns: List[str]) -> pd.DataFrame:
        """
        应用增强版消消乐式去重：保留首行完整内容，后续行删除与先前行重复的内容（包括前缀、中间部分和后缀）
        
        Args:
            df: 数据框
            id_column: ID列名
            text_columns: 文本列名列表
            
        Returns:
            去重后的数据框
        """
        # 复制数据框以避免修改原始数据
        deduplicated_df = df.copy()
        
        try:
            # 对每个文本列应用去重
            for col in text_columns:
                if col not in deduplicated_df.columns:
                    logger.warning(f"列 '{col}' 不存在于数据框中，跳过")
                    continue
                    
                logger.info(f"开始对列 '{col}' 应用全方位消消乐式去重")
                
                # 存储所有已处理的文本
                all_processed_texts = []
                all_processed_words = set()  # 用于快速检查单词是否已出现过
                
                # 确保ID列存在
                if id_column not in deduplicated_df.columns:
                    logger.warning(f"ID列 '{id_column}' 不存在，使用索引作为ID")
                    deduplicated_df[id_column] = deduplicated_df.index
                
                # 处理每一行
                for idx in range(len(deduplicated_df)):
                    try:
                        # 获取当前行的文本和ID
                        current_id = str(deduplicated_df.iloc[idx][id_column])
                        current_text = str(deduplicated_df.iloc[idx][col]).strip()
                        
                        # 如果是第一行或文本为空，保持不变并添加到处理记录
                        if idx == 0 or not current_text:
                            all_processed_texts.append(current_text)
                            if current_text:
                                # 添加此行所有单词到集合
                                for word in current_text.split():
                                    all_processed_words.add(word)
                            continue
                        
                        # 拆分当前文本为单词
                        current_words = current_text.split()
                        if not current_words:
                            all_processed_texts.append(current_text)
                            continue
                        
                        # 第一步：处理连续的前缀和后缀去重
                        modified_words = self._remove_continuous_duplicates(current_words, all_processed_texts)
                        
                        # 第二步：处理中间部分的重复单词（用于长文本中间有大量重复内容的情况）
                        if len(modified_words) > 3:  # 只处理足够长的文本，避免过度去重
                            unique_words = []
                            for word in modified_words:
                                # 如果单词已经在之前出现过，并且不是常见连接词或标点，则跳过
                                if word in all_processed_words and not self._is_common_word(word):
                                    continue
                                unique_words.append(word)
                                all_processed_words.add(word)
                            
                            # 如果去重后的内容至少有一个单词，则使用它
                            if unique_words:
                                modified_words = unique_words
                        
                        # 第三步：专门处理单位词重复问题（如"秒", "s", "分", "m"等）
                        modified_words = self._remove_repeated_unit_words(modified_words)
                        
                        # 更新当前行的文本
                        new_text = " ".join(modified_words) if modified_words else ""
                        
                        # 特殊情况：如果去重后文本为空，保留第一个单词或一些识别信息
                        if not new_text and current_words:
                            new_text = current_words[0]  # 保留至少一个单词，避免完全空白
                        
                        deduplicated_df.iloc[idx, deduplicated_df.columns.get_loc(col)] = new_text
                        
                        # 将原始文本添加到已处理列表
                        all_processed_texts.append(current_text)
                        
                    except Exception as e:
                        logger.error(f"处理行 {idx} 时出错: {str(e)}")
                        import traceback
                        logger.error(traceback.format_exc())
                        # 继续处理下一行
                
                logger.info(f"列 '{col}' 应用全方位消消乐式去重完成")
                
                # 最后一步：对整个列应用额外的单位词清理
                deduplicated_df[col] = deduplicated_df[col].apply(self._clean_unit_words)
                logger.info(f"列 '{col}' 应用单位词清理完成")
            
            return deduplicated_df
        except Exception as e:
            logger.error(f"消消乐式去重过程中发生错误: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            # 出错时返回原始数据框
            return df
    
    def _remove_continuous_duplicates(self, current_words: List[str], previous_texts: List[str]) -> List[str]:
        """
        移除连续的重复前缀和后缀
        
        Args:
            current_words: 当前行的单词列表
            previous_texts: 之前处理过的文本列表
        
        Returns:
            处理后的单词列表
        """
        result_words = current_words.copy()
        
        for prev_text in previous_texts:
            if not prev_text:
                continue
            
            prev_words = prev_text.split()
            if not prev_words:
                continue
            
            # 1. 处理最长公共前缀
            prefix_len = 0
            min_len = min(len(result_words), len(prev_words))
            
            for i in range(min_len):
                if i >= len(result_words):
                    break
                if result_words[i] == prev_words[i]:
                    prefix_len += 1
                else:
                    break
            
            # 如果存在较长的公共前缀，移除它 
            if prefix_len > 1 and prefix_len < len(result_words):  # 至少2个单词才算前缀
                result_words = result_words[prefix_len:]
            
            # 2. 处理最长公共后缀
            if result_words and len(result_words) < len(current_words):  # 只有在前缀处理有效时才处理后缀
                suffix_len = 0
                current_len = len(result_words)
                prev_len = len(prev_words)
                
                for i in range(1, min(current_len, prev_len) + 1):
                    if result_words[-i] == prev_words[-i]:
                        suffix_len += 1
                    else:
                        break
                
                # 如果存在较长的公共后缀，移除它
                if suffix_len > 1 and suffix_len < current_len:  # 至少2个单词才算后缀
                    result_words = result_words[:-suffix_len]
        
        return result_words
    
    def _remove_repeated_unit_words(self, words: List[str]) -> List[str]:
        """
        删除文本中重复的单位词
        
        Args:
            words: 单词列表
            
        Returns:
            处理后的单词列表
        """
        if not words or len(words) <= 1:
            return words
        
        # 定义需要特殊处理的单位词
        unit_words = {'秒', 's', '分', 'm', '小时', 'h', '天', 'd', 
                     '周', 'w', '月', '年', 'y', '次', '个', '条',
                     '项', '元', '分钟', '千米', 'km', '米', '厘米', 'cm'}
        
        # 先检查是否存在单位词
        has_unit_word = False
        for word in words:
            if word in unit_words:
                has_unit_word = True
                break
        
        # 如果没有单位词，无需处理
        if not has_unit_word:
            return words
        
        # 统计每个单位词出现的次数
        unit_count = {}
        for word in words:
            if word in unit_words:
                unit_count[word] = unit_count.get(word, 0) + 1
        
        # 如果有重复的单位词，进行去重
        if any(count > 1 for count in unit_count.values()):
            result = []
            unit_seen = set()
            
            # 保留每个单位词的第一次出现
            for word in words:
                if word in unit_words:
                    if word not in unit_seen:
                        result.append(word)
                        unit_seen.add(word)
                else:
                    result.append(word)
            
            return result
        
        # 如果没有重复的单位词，返回原列表
        return words
    
    def _clean_unit_words(self, text: str) -> str:
        """
        清理文本中重复的单位词
        
        Args:
            text: 要处理的文本
            
        Returns:
            处理后的文本
        """
        if not text or not isinstance(text, str):
            return text
        
        # 定义需要清理的模式
        patterns = [
            (r'秒\s+秒+', '秒'),  # 处理连续的"秒"
            (r's\s+s+', 's'),    # 处理连续的"s"
            (r'分\s+分+', '分'),  # 处理连续的"分"
            (r'm\s+m+', 'm'),    # 处理连续的"m"
            (r'小时\s+小时+', '小时'),
            (r'h\s+h+', 'h'),
            (r'天\s+天+', '天'),
            (r'd\s+d+', 'd'),
            (r'次\s+次+', '次'),
            (r'个\s+个+', '个'),
            # 处理单位词的常见组合
            (r'秒\s+s', '秒'),
            (r's\s+秒', '秒'),
            (r'分\s+m', '分'),
            (r'm\s+分', '分'),
            (r'小时\s+h', '小时'),
            (r'h\s+小时', '小时'),
            (r'天\s+d', '天'),
            (r'd\s+天', '天')
        ]
        
        # 应用模式
        for pattern, replacement in patterns:
            text = re.sub(pattern, replacement, text)
        
        return text
    
    def _is_common_word(self, word: str) -> bool:
        """
        检查一个单词是否是常见连接词或无意义词，这些词不应该被去重
        
        Args:
            word: 要检查的单词
        
        Returns:
            是否是常见词
        """
        common_words = {
            '的', '了', '和', '与', '或', '及', '等', '中', '在', 
            '是', '有', '个', '为', '以', '之', '于', '上', '下',
            '需要', '可以', '使用', '包括', '如果', '则', '当',
            '组件', '升级', '激活', '用途', '获得', '可用'
        }
        
        # 添加常见单位词到常见词列表
        unit_words = {
            '秒', 's', '分', 'm', '小时', 'h', '天', 'd', 
            '周', 'w', '月', '年', 'y', '次', '个', '条',
            '项', '元', '分钟', '千米', 'km', '米', '厘米', 'cm'
        }
        
        common_words.update(unit_words)
        
        return word in common_words or len(word) <= 1

    def _remove_duplicate_entries(self, df: pd.DataFrame, id_column: str, text_columns: List[str]) -> pd.DataFrame:
        """
        删除文本内容完全相同的条目，只保留首条
        
        Args:
            df: 数据框
            id_column: ID列名
            text_columns: 文本列名列表
            
        Returns:
            被删除条目的数据框
        """
        # 复制数据框以避免修改原始数据
        original_df = df.copy()
        
        # 为每个文本列创建一个内容字典，用于检测重复
        content_dict = {}
        duplicate_indices = []
        
        # 遍历每一行
        for idx, row in df.iterrows():
            # 对每个文本列，检查内容是否已经存在
            for col in text_columns:
                content = str(row[col]).strip()
                if content in content_dict:
                    # 如果内容已存在，标记为重复
                    duplicate_indices.append(idx)
                    break
                else:
                    # 否则添加到内容字典
                    content_dict[content] = idx
        
        # 删除重复行
        if duplicate_indices:
            df.drop(duplicate_indices, inplace=True)
            
            # 返回被删除的行
            return original_df.loc[duplicate_indices]
        
        return pd.DataFrame()  # 如果没有删除任何行，返回空数据框

    def _remove_empty_entries(self, df: pd.DataFrame, id_column: str, text_columns: List[str]) -> pd.DataFrame:
        """
        删除文本内容为空的条目
        
        Args:
            df: 数据框
            id_column: ID列名
            text_columns: 文本列名列表
            
        Returns:
            被删除条目的数据框
        """
        # 复制数据框以避免修改原始数据
        original_df = df.copy()
        
        # 检查每行，如果所有文本列内容为空，则标记为删除
        empty_indices = []
        
        for idx, row in df.iterrows():
            # 检查所有文本列是否都为空
            is_empty = True
            for col in text_columns:
                content = str(row[col]).strip()
                if content:  # 如果有内容，不是空的
                    is_empty = False
                    break
            
            if is_empty:
                empty_indices.append(idx)
        
        # 删除空内容行
        if empty_indices:
            df.drop(empty_indices, inplace=True)
            
            # 返回被删除的行
            return original_df.loc[empty_indices]
        
        return pd.DataFrame()  # 如果没有删除任何行，返回空数据框

    def _save_removed_ids(self, removed_ids: List, id_column: str, original_output_file: str) -> str:
        """
        将被删除条目的ID保存为单独的Excel文件
        
        Args:
            removed_ids: 被删除条目的ID列表
            id_column: ID列名
            original_output_file: 原始输出文件路径
            
        Returns:
            保存的文件路径
        """
        try:
            # 创建包含被删除ID的数据框
            removed_df = pd.DataFrame({id_column: removed_ids})
            
            # 生成删除条目ID的输出文件名
            output_dir = os.path.dirname(original_output_file)
            base_name = os.path.basename(original_output_file)
            name_parts = os.path.splitext(base_name)
            removed_file = os.path.join(output_dir, f"{name_parts[0]}_removed_ids{name_parts[1]}")
            
            # 保存为Excel
            removed_df.to_excel(removed_file, index=False)
            logger.info(f"已将 {len(removed_ids)} 个被删除条目的ID保存至: {removed_file}")
            return removed_file
        except Exception as e:
            logger.error(f"保存被删除ID失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return ""

    def _save_stats(self, stats: Dict[str, Any], original_output_file: str) -> str:
        """
        将处理统计信息保存为JSON文件
        
        Args:
            stats: 统计信息字典
            original_output_file: 原始输出文件路径
            
        Returns:
            保存的文件路径
        """
        try:
            # 生成统计信息文件名
            output_dir = os.path.dirname(original_output_file)
            base_name = os.path.basename(original_output_file)
            name_parts = os.path.splitext(base_name)
            stats_file = os.path.join(output_dir, f"{name_parts[0]}_stats.json")
            
            # 保存为JSON
            import json
            with open(stats_file, 'w', encoding='utf-8') as f:
                json.dump(stats, f, ensure_ascii=False, indent=2)
            
            return stats_file
        except Exception as e:
            logger.error(f"保存统计信息失败: {str(e)}")
            return ""

def main():
    """模块主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description="数据预处理工具 - 清洗Excel中的文本数据")
    parser.add_argument("--input", "-i", required=True, help="输入Excel文件路径")
    parser.add_argument("--output", "-o", help="输出Excel文件路径，默认为'cleaned_数据文件名'")
    parser.add_argument("--id-column", help="ID列名称，默认自动识别")
    parser.add_argument("--deduplication", "-d", action="store_true", 
                        help="启用消消乐式去重功能，保留首行完整内容，后续行自动删除与前面重复的内容")
    
    args = parser.parse_args()
    
    # 如果未指定输出文件，则生成默认名称
    if not args.output:
        input_path = Path(args.input)
        args.output = str(input_path.with_name(f"cleaned_{input_path.name}"))
    
    # 创建预处理器并处理数据
    preprocessor = DataPreprocessor()
    output_path = preprocessor.preprocess_excel(args.input, args.output, args.id_column, args.deduplication)
    
    if output_path:
        print(f"预处理完成！结果已保存至: {output_path}")
    else:
        print("预处理失败！请检查日志获取详细信息。")

if __name__ == "__main__":
    main() 