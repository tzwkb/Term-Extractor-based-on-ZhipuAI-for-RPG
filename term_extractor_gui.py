"""
术语抽取工具 - GUI界面
基于智谱AI的专业术语抽取工具图形界面
"""

import os
import sys
import time
import logging
import threading
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext, simpledialog
import pandas as pd
import traceback
import requests  # 添加requests模块导入
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Any, Tuple
import shutil
import re
import subprocess

# 导入自定义模块
from data_preprocessor import DataPreprocessor
from term_extractor import TermExtractor
from check_dependencies import check_and_install_dependencies, fix_excel_dependencies, test_excel_reading_capability

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("term_extractor_gui")

# 检查可选依赖
try:
    from zhipuai import ZhipuAI
    HAS_ZHIPUAI = True
except ImportError:
    HAS_ZHIPUAI = False
    logger.info("未安装zhipuai库，无法使用术语提取功能")


class TermExtractorGUI:
    """术语抽取工具的图形用户界面"""

    def __init__(self, root):
        """初始化GUI界面"""
        self.root = root
        self.version = "1.2"  # 添加版本号
        root.title("术语提取工具")
        root.geometry("1200x800")
        root.minsize(1000, 700)
        
        # 先检查并修复Excel相关依赖
        self._check_excel_dependencies()
        
        # 设置窗口图标
        try:
            # 尝试设置图标
            icon_path = os.path.join(os.path.dirname(__file__), "assets", "icon.ico")
            if os.path.exists(icon_path):
                root.iconbitmap(icon_path)
        except:
            pass
            
        # 停止事件
        self.stop_event = threading.Event()
        
        # 状态变量
        self.status_var = tk.StringVar(value="就绪")
        self.progress_var = tk.DoubleVar(value=0.0)
        self.api_key_var = tk.StringVar()
        self.api_url_var = tk.StringVar(value="https://open.bigmodel.cn/api/paas/v4")
        self.model_var = tk.StringVar(value="glm-4-flash")
        self.min_term_length_var = tk.StringVar(value="2")
        self.max_retries_var = tk.StringVar(value="3")
        self.extract_progress_var = tk.DoubleVar(value=0.0)
        
        # 文件和目录变量
        self.input_file_var = tk.StringVar()
        self.output_dir_var = tk.StringVar(value=os.path.join(os.getcwd(), "output"))
        self.output_filename_var = tk.StringVar(value="术语提取结果.xlsx")
        self.cleaned_file_var = tk.StringVar()
        self.preprocessed_file_var = tk.StringVar()  # 用于存储预处理文件路径
        self.last_output_file = None  # 用于存储最后处理的输出文件路径
        
        # 提取状态标志
        self.is_extracting = False
        self.is_cleaning = False
        self.extraction_active = False
        
        # 数据清洗选项
        self.clean_numbers_var = tk.BooleanVar(value=False)
        self.clean_urls_var = tk.BooleanVar(value=True)
        self.clean_html_tags_var = tk.BooleanVar(value=True)
        self.clean_punctuation_var = tk.BooleanVar(value=False)
        self.clean_placeholders_var = tk.BooleanVar(value=True)
        
        # 创建菜单栏
        self._create_menu()
        
        # 创建主框架
        main_frame = ttk.Frame(root, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # 创建单页面设计的主界面
        self._setup_integrated_interface(main_frame)
        
        # 创建日志区域
        self._setup_log_area(main_frame)
        
        # 检查和显示环境信息
        self._check_environment()
        
        # 绑定关闭事件
        root.protocol("WM_DELETE_WINDOW", self._on_closing)
        
        # 设置状态
        self.update_status("准备就绪")
        self.log("术语提取工具 v{} 已启动".format(self.version))
        
        # 闪烁状态指示灯
        self.indicator_state = False
        # ID和状态指示灯
        self.process_animation_id = None
    
    def _set_default_values(self):
        # 设置窗口图标
        try:
            # 尝试设置图标（如果有）
            pass
        except:
            pass
        
        # 状态变量
        self.progress_var = tk.DoubleVar(value=0.0)
        self.api_key_var = tk.StringVar()
        self.api_url_var = tk.StringVar(value="https://open.bigmodel.cn/api/paas/v4/chat/completions")
        self.model_var = tk.StringVar(value="glm-4-flash")
        self.min_term_length_var = tk.StringVar(value="2")
        self.max_retries_var = tk.StringVar(value="3")
        
        # 文件相关变量
        self.input_file_var = tk.StringVar()
        self.output_dir_var = tk.StringVar(value=os.getcwd())
        self.output_file_var = tk.StringVar(value="提取的术语.xlsx")
        self.cleaned_file_var = tk.StringVar()
        self.preprocessed_file_var = tk.StringVar()  # 用于存储预处理文件路径
        
        # 数据清洗选项
        self.clean_numbers_var = tk.BooleanVar(value=True)
        self.clean_punctuation_var = tk.BooleanVar(value=True)
        self.clean_urls_var = tk.BooleanVar(value=True)
        self.clean_html_tags_var = tk.BooleanVar(value=True)
        self.clean_placeholders_var = tk.BooleanVar(value=True)
        
        # 设置UI主题
        style = ttk.Style()
        style.theme_use('clam')  # 使用内置主题
        
        # 创建菜单栏
        self._create_menu()
        
        # 创建日志区域
        self._setup_log_area(self.main_frame)
        
        # 检查和显示环境信息
        self._check_environment()
        
        # 绑定关闭事件
        self.root.protocol("WM_DELETE_WINDOW", self._on_closing)
        
        # 设置状态
        self.running = False
    
    def _check_excel_dependencies(self):
        """检查并修复Excel相关依赖"""
        # 创建临时日志框架
        temp_frame = ttk.Frame(self.root, padding=10)
        temp_frame.pack(fill=tk.BOTH, expand=True)
        
        temp_log = scrolledtext.ScrolledText(temp_frame, height=10)
        temp_log.pack(fill=tk.BOTH, expand=True)
        temp_log.insert(tk.END, "正在检查Excel相关依赖...\n")
        
        # 定向print输出到临时日志
        original_stdout = sys.stdout
        
        class LogRedirector:
            def __init__(self, text_widget):
                self.text_widget = text_widget
            
            def write(self, string):
                self.text_widget.insert(tk.END, string)
                self.text_widget.see(tk.END)
                self.text_widget.update()
            
            def flush(self):
                pass
        
        sys.stdout = LogRedirector(temp_log)
        
        try:
            # 运行Excel依赖检查和修复
            fixed = fix_excel_dependencies()
            if fixed:
                temp_log.insert(tk.END, "✓ Excel依赖已自动修复\n")
            else:
                temp_log.insert(tk.END, "! Excel依赖修复失败，可能会导致Excel文件读取问题\n")
            
            # 测试Excel读取能力
            success, message = test_excel_reading_capability()
            if success:
                temp_log.insert(tk.END, "✓ Excel读取能力测试通过\n")
            else:
                temp_log.insert(tk.END, f"! Excel读取能力测试失败: {message}\n")
                messagebox.showwarning(
                    "Excel读取问题", 
                    "Excel读取能力测试失败，可能会导致Excel文件无法读取。\n"
                    "建议手动运行以下命令：\n"
                    "pip install xlrd==1.2.0 openpyxl pandas"
                )
        except Exception as e:
            temp_log.insert(tk.END, f"! 依赖检查时出错: {str(e)}\n")
        finally:
            # 恢复原始stdout
            sys.stdout = original_stdout
            
            # 显示完成信息并移除临时框架
            temp_log.insert(tk.END, "依赖检查完成，正在启动主程序...\n")
            self.root.after(3000, lambda: temp_frame.destroy())
    
    def _create_menu(self):
        """创建菜单栏"""
        menubar = tk.Menu(self.root)
        
        # 文件菜单
        file_menu = tk.Menu(menubar, tearoff=0)
        file_menu.add_command(label="选择Excel文件", command=self._browse_file)
        file_menu.add_command(label="选择输出目录", command=self._browse_output_dir)
        file_menu.add_separator()
        file_menu.add_command(label="打开术语提取结果", command=self._open_result_file)
        file_menu.add_command(label="清理临时文件", command=self._clean_temp_files)
        file_menu.add_separator()
        file_menu.add_command(label="退出", command=self._on_closing)
        menubar.add_cascade(label="文件", menu=file_menu)
        
        # 工具菜单
        tools_menu = tk.Menu(menubar, tearoff=0)
        tools_menu.add_command(label="测试API连接", command=self._test_api)
        tools_menu.add_command(label="检查依赖", command=self._check_dependencies)
        tools_menu.add_command(label="修复Excel依赖", command=self._fix_excel_dependencies)
        tools_menu.add_separator()
        tools_menu.add_command(label="清空日志", command=self._clear_log)
        menubar.add_cascade(label="工具", menu=tools_menu)
        
        # 帮助菜单
        help_menu = tk.Menu(menubar, tearoff=0)
        help_menu.add_command(label="使用帮助", command=self._show_help)
        help_menu.add_command(label="关于", command=self._show_about)
        menubar.add_cascade(label="帮助", menu=help_menu)
        
        self.root.config(menu=menubar)
        
        # 快捷键绑定
        self.root.bind("<Control-o>", lambda e: self._browse_file())
        self.root.bind("<Control-p>", lambda e: self._preview_excel())
    
    def _setup_integrated_interface(self, parent):
        """设置界面"""
        # 文件设置区域
        file_frame = ttk.LabelFrame(parent, text="数据文件设置", padding="5")
        file_frame.pack(fill=tk.X, expand=False, pady=3)
        
        # 输入文件选择
        ttk.Label(file_frame, text="Excel输入文件:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=2)
        self.input_file_var = tk.StringVar()
        ttk.Entry(file_frame, textvariable=self.input_file_var, width=50).grid(row=0, column=1, padx=5, pady=2)
        ttk.Button(file_frame, text="浏览...", command=self._browse_file).grid(row=0, column=2, padx=5, pady=2)
        ttk.Button(file_frame, text="预览", command=self._preview_excel).grid(row=0, column=3, padx=5, pady=2)
        ttk.Label(file_frame, text="(选择包含待提取术语的Excel文件)").grid(row=0, column=4, sticky=tk.W, padx=5, pady=2)
        
        # 输出目录选择
        ttk.Label(file_frame, text="结果保存目录:").grid(row=1, column=0, sticky=tk.W, padx=5, pady=2)
        self.output_dir_var = tk.StringVar(value=os.path.join(os.getcwd(), "output"))
        ttk.Entry(file_frame, textvariable=self.output_dir_var, width=50).grid(row=1, column=1, padx=5, pady=2)
        ttk.Button(file_frame, text="浏览...", command=self._browse_output_dir).grid(row=1, column=2, padx=5, pady=2)
        ttk.Label(file_frame, text="(程序将在此目录保存所有结果文件)").grid(row=1, column=4, sticky=tk.W, padx=5, pady=2)
        
        # 输出文件名
        ttk.Label(file_frame, text="结果文件名称:").grid(row=2, column=0, sticky=tk.W, padx=5, pady=2)
        self.output_filename_var = tk.StringVar(value="术语提取结果.xlsx")
        ttk.Entry(file_frame, textvariable=self.output_filename_var, width=50).grid(row=2, column=1, padx=5, pady=2)
        ttk.Label(file_frame, text="(提取的术语将保存到此Excel文件)").grid(row=2, column=4, sticky=tk.W, padx=5, pady=2)
        
        # 创建一个水平分隔线
        separator = ttk.Separator(parent, orient='horizontal')
        separator.pack(fill=tk.X, padx=10, pady=3)
        
        # 数据清洗面板
        clean_panel = ttk.LabelFrame(parent, text="数据清洗设置", padding="5")
        clean_panel.pack(fill=tk.X, expand=False, padx=3, pady=3)
        
        # 设置数据清洗面板内容
        self._setup_clean_panel(clean_panel)
        
        # 术语抽取面板
        extract_panel = ttk.LabelFrame(parent, text="术语提取设置", padding="5")
        extract_panel.pack(fill=tk.BOTH, expand=True, padx=3, pady=3)
        
        # 设置术语抽取面板内容
        self._setup_extract_panel(extract_panel)
        
        # 进度条框架
        progress_frame = ttk.Frame(parent, padding="3")
        progress_frame.pack(fill=tk.X, expand=False, pady=2)
        
        # 术语抽取进度条
        ttk.Label(progress_frame, text="处理进度:").pack(side=tk.LEFT, padx=5)
        self.extract_progress_var = tk.DoubleVar(value=0.0)
        self.extract_progress_bar = ttk.Progressbar(
            progress_frame, 
            orient=tk.HORIZONTAL, 
            length=300, 
            mode='determinate',
            variable=self.extract_progress_var
        )
        self.extract_progress_bar.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)
        
        # 状态标签
        ttk.Label(progress_frame, text="状态:").pack(side=tk.LEFT, padx=(10, 5))
        self.status_label = ttk.Label(progress_frame, textvariable=self.status_var)
        self.status_label.pack(side=tk.LEFT)
        
        # 处理指示器
        self.process_indicator_var = tk.StringVar(value="")
        self.process_indicator_label = ttk.Label(progress_frame, textvariable=self.process_indicator_var, font=("Arial", 10, "bold"))
        self.process_indicator_label.pack(side=tk.LEFT, padx=10)
    
    def _setup_clean_panel(self, parent):
        """设置数据清洗面板"""
        # 创建左右两列布局
        left_frame = ttk.Frame(parent)
        left_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        right_frame = ttk.Frame(parent)
        right_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # 左侧 - 清洗选项
        options_frame = ttk.LabelFrame(left_frame, text="清洗选项", padding="5")
        options_frame.pack(fill=tk.BOTH, expand=True)
        
        # 添加清洗选项
        ttk.Checkbutton(options_frame, text="清除数字", 
                       variable=self.clean_numbers_var).grid(row=0, column=0, sticky=tk.W, padx=5, pady=2)
        ttk.Label(options_frame, text="(删除文本中的阿拉伯数字)").grid(row=0, column=1, sticky=tk.W, padx=5, pady=2)
        
        ttk.Checkbutton(options_frame, text="清除标点符号", 
                       variable=self.clean_punctuation_var).grid(row=1, column=0, sticky=tk.W, padx=5, pady=2)
        ttk.Label(options_frame, text="(删除文本中的标点符号)").grid(row=1, column=1, sticky=tk.W, padx=5, pady=2)
        
        ttk.Checkbutton(options_frame, text="清除超链接", 
                       variable=self.clean_urls_var).grid(row=2, column=0, sticky=tk.W, padx=5, pady=2)
        ttk.Label(options_frame, text="(删除URL和网址)").grid(row=2, column=1, sticky=tk.W, padx=5, pady=2)
        
        ttk.Checkbutton(options_frame, text="清除HTML标签", 
                       variable=self.clean_html_tags_var).grid(row=3, column=0, sticky=tk.W, padx=5, pady=2)
        ttk.Label(options_frame, text="(保留标签内的文本，如<green>20:30</>)").grid(row=3, column=1, sticky=tk.W, padx=5, pady=2)
        
        ttk.Checkbutton(options_frame, text="清除占位符", 
                       variable=self.clean_placeholders_var).grid(row=4, column=0, sticky=tk.W, padx=5, pady=2)
        ttk.Label(options_frame, text="(删除___等占位符)").grid(row=4, column=1, sticky=tk.W, padx=5, pady=2)
        
        # 右侧 - 操作按钮
        buttons_frame = ttk.Frame(right_frame, padding="5")
        buttons_frame.pack(pady=10)
        
        # 数据清洗文件路径
        ttk.Label(right_frame, text="清洗后的文件:").pack(anchor=tk.W, padx=5, pady=2)
        ttk.Entry(right_frame, textvariable=self.cleaned_file_var, width=45, state="readonly").pack(fill=tk.X, padx=5, pady=2)
        ttk.Label(right_frame, text="(清洗后的数据将用于术语提取)").pack(anchor=tk.W, padx=5, pady=2)
        
        # 清洗按钮
        self.clean_button = ttk.Button(buttons_frame, text="清洗数据", 
                                     command=self._clean_data_thread, width=15)
        self.clean_button.pack(side=tk.LEFT, padx=5, pady=5)
    
    def _setup_extract_panel(self, parent):
        """设置术语抽取面板"""
        # API设置 
        api_frame = ttk.LabelFrame(parent, text="智谱AI接口设置", padding="5")
        api_frame.pack(fill=tk.X, expand=False, padx=5, pady=5)
        
        # API Key 
        ttk.Label(api_frame, text="API密钥:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=2)
        api_key_entry = ttk.Entry(api_frame, textvariable=self.api_key_var, width=40, show="*")
        api_key_entry.grid(row=0, column=1, padx=5, pady=2, sticky=tk.W+tk.E)
        
        # 密钥显示开关 
        self.show_key_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(api_frame, text="显示密钥", 
                        command=lambda: api_key_entry.config(show="" if self.show_key_var.get() else "*"),
                        variable=self.show_key_var).grid(row=0, column=2, padx=5, pady=2)
        
        # 测试API按钮 
        ttk.Button(api_frame, text="测试连接", command=self._test_api).grid(row=0, column=3, padx=5, pady=2)
        
        # API URL
        ttk.Label(api_frame, text="服务地址:").grid(row=1, column=0, sticky=tk.W, padx=5, pady=2)
        ttk.Entry(api_frame, textvariable=self.api_url_var, width=40).grid(row=1, column=1, columnspan=2, padx=5, pady=2, sticky=tk.W+tk.E)
        
        # 模型选择
        ttk.Label(api_frame, text="AI模型:").grid(row=2, column=0, sticky=tk.W, padx=5, pady=2) 
        model_combobox = ttk.Combobox(api_frame, textvariable=self.model_var, 
                                       values=["glm-4-flash", "glm-4", "glm-3-turbo"], width=15)
        model_combobox.grid(row=2, column=1, padx=5, pady=2, sticky=tk.W)
        model_combobox.current(0)  # 默认选中GLM-4-Flash
        
        # 参数设置帧
        params_frame = ttk.LabelFrame(parent, text="术语提取设置", padding="5")
        params_frame.pack(fill=tk.X, expand=False, padx=5, pady=5)
        
        # 术语最小长度
        ttk.Label(params_frame, text="术语最小长度:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=2)
        ttk.Spinbox(params_frame, from_=1, to=10, textvariable=self.min_term_length_var, 
                   width=5).grid(row=0, column=1, padx=5, pady=2, sticky=tk.W)
        ttk.Label(params_frame, text="(较小的值会提取更多短术语)").grid(row=0, column=2, sticky=tk.W, padx=5, pady=2)
        
        # 最大重试次数
        ttk.Label(params_frame, text="网络重试次数:").grid(row=1, column=0, sticky=tk.W, padx=5, pady=2)
        ttk.Spinbox(params_frame, from_=1, to=5, textvariable=self.max_retries_var, 
                   width=5).grid(row=1, column=1, padx=5, pady=2, sticky=tk.W)
        ttk.Label(params_frame, text="(如网络不稳定可增加重试次数)").grid(row=1, column=2, sticky=tk.W, padx=5, pady=2)
        
        # 依赖检查
        if not HAS_ZHIPUAI:
            warning_frame = ttk.Frame(parent, padding="5")
            warning_frame.pack(fill=tk.X, expand=False, padx=5, pady=5)
            
            warning_label = ttk.Label(warning_frame, 
                                     text="⚠️ 未安装智谱AI接口库，请点击下方按钮安装后使用",
                                     foreground="red")
            warning_label.pack(fill=tk.X, padx=5, pady=5)
            
            ttk.Button(warning_frame, text="安装必要组件", 
                      command=lambda: self._install_package("zhipuai")).pack(padx=5, pady=5)
        
        # 操作按钮区域
        buttons_frame = ttk.Frame(parent, padding="5")
        buttons_frame.pack(fill=tk.X, expand=False, padx=5, pady=10)
        
        # 运行按钮
        self.extract_button = ttk.Button(buttons_frame, text="开始提取术语", 
                                        command=self._check_and_start_extraction)
        self.extract_button.pack(side=tk.LEFT, padx=5, pady=5)
        
        # 停止按钮
        self.stop_button = ttk.Button(buttons_frame, text="停止提取", 
                                     command=self._stop_extraction, state=tk.DISABLED)
        self.stop_button.pack(side=tk.LEFT, padx=5, pady=5)
        
        # 打开结果按钮
        self.open_result_button = ttk.Button(buttons_frame, text="打开结果文件", 
                                           command=self._open_result_file)
        self.open_result_button.pack(side=tk.LEFT, padx=5, pady=5)
        
        # 清理文件按钮
        ttk.Button(buttons_frame, text="清理临时文件", 
                  command=self._clean_temp_files).pack(side=tk.RIGHT, padx=5, pady=5)
    
    def _setup_log_area(self, parent):
        """设置日志区域"""
        log_frame = ttk.LabelFrame(parent, text="处理日志", padding="5")
        log_frame.pack(fill=tk.BOTH, expand=True, pady=3)
        
        # 创建文本区域和滚动条
        self.log_text = scrolledtext.ScrolledText(
            log_frame, 
            wrap=tk.WORD, 
            height=25,
            width=100,
            font=("Consolas", 10)  # 使用等宽字体
        )
        self.log_text.pack(fill=tk.BOTH, expand=True)
        self.log_text.config(state=tk.DISABLED)
        
        # 初始化日志
        self.log("欢迎使用术语提取工具")
        self.log('请先选择Excel文件，然后点击"开始提取术语"按钮')
        self.log("程序将自动从文本中识别并提取专业术语")
    
    def _check_environment(self):
        """检查运行环境，显示版本信息"""
        self.log(f"Python版本: {sys.version.split()[0]}")
        
        # 检查是否安装了智谱AI库
        try:
            import zhipuai
            self.log(f"✓ 已安装智谱AI库 (zhipuai)")
        except ImportError:
            self.log("⚠️ 未安装智谱AI库，使用直接HTTP请求模式")
            
        # 检查pandas库
        try:
            import pandas as pd
            self.log(f"✓ 已安装pandas {pd.__version__}")
        except ImportError:
            self.log("⚠️ 未安装pandas库，可能导致Excel处理失败")
            
        # 检查json_repair库
        try:
            import json_repair
            self.log("✓ 已安装json_repair库")
        except ImportError:
            self.log("⚠️ 未安装json_repair库，建议安装以提高JSON解析成功率")
            
        self.log("--------------------------------------")
        
    def log(self, message):
        """向日志文本框添加消息"""
        self.log_text.config(state=tk.NORMAL)
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_text.insert(tk.END, f"[{timestamp}] {message}\n")
        self.log_text.see(tk.END)
        self.log_text.config(state=tk.DISABLED)
        
        # 更新GUI
        self.root.update_idletasks()
        
    def _check_text_column(self, df, col_name) -> bool:
        """检查列是否可能是文本列"""
        try:
            # 获取非空值的样本
            sample = df[col_name].dropna().astype(str)
            if len(sample) == 0:
                return False
                
            # 计算平均长度
            avg_length = sum(len(str(x)) for x in sample) / len(sample)
            
            # 检查是否至少有一个稍长的值(超过10个字符)
            has_long_text = any(len(str(x)) > 10 for x in sample)
            
            # 判断是否可能是文本列:
            # 1. 平均长度大于5个字符
            # 2. 或者有至少一个长度超过10个字符的值
            return avg_length > 5 or has_long_text
        except:
            return False
            
    def _browse_file(self):
        """浏览文件对话框"""
        filename = filedialog.askopenfilename(
            title="选择Excel文件",
            filetypes=[("Excel文件", "*.xlsx *.xls"), ("所有文件", "*.*")]
        )
        if filename:
            self.input_file_var.set(filename)
            
            # 预览列信息
            try:
                # 首先尝试使用openpyxl引擎
                try:
                    df = pd.read_excel(filename, engine='openpyxl')
                    self.log("✓ 成功读取Excel文件")
                except Exception as e1:
                    self.log(f"! openpyxl引擎读取失败，尝试使用默认引擎")
                    try:
                        # 尝试使用pandas默认引擎
                        df = pd.read_excel(filename)
                        self.log("✓ 使用默认引擎成功读取文件")
                    except Exception as e2:
                        self.log(f"❌ 所有引擎都无法读取文件")
                        raise Exception(f"无法读取Excel文件，请确保文件格式正确且未被损坏。\n错误详情：{str(e2)}")
                
                # 检测单列Excel文件
                is_single_column = len(df.columns) == 1
                if is_single_column:
                    self.log("检测到单列Excel文件")
                    self.log(f"单列名: {df.columns[0]}")
                    self.log("单列文件将被作为特殊情况处理")
                else:
                    # 显示列信息
                    column_info = "\n".join([f"- {col}" for col in df.columns])
                    self.log(f"文件包含以下列:\n{column_info}")
                
                # 自动设置输出文件名
                base_name = os.path.splitext(os.path.basename(filename))[0]
                self.output_filename_var.set(f"{base_name}_术语提取结果.xlsx")
                
                # 自动设置预处理结果文件名
                output_dir = self.output_dir_var.get()
                preprocessed_filename = f"preprocessed_{os.path.basename(filename)}"
                preprocessed_path = os.path.join(output_dir, preprocessed_filename)
                self.preprocessed_file_var.set(preprocessed_path)
                
                # 清空已清洗文件的路径
                self.cleaned_file_var.set("")
            except Exception as e:
                self.log(f"❌ 预览文件时出错: {str(e)}")
                messagebox.showerror("错误", f"无法读取或预览文件: {str(e)}")
                traceback.print_exc()
        
    def _preview_excel_file(self, file_path):
        """预览Excel文件内容"""
        try:
            # 首先尝试使用openpyxl引擎
            try:
                df = pd.read_excel(file_path, engine='openpyxl')
                self.log("✓ 成功预览Excel文件")
            except Exception as e1:
                self.log(f"! openpyxl引擎预览失败，尝试使用默认引擎")
                try:
                    # 尝试使用pandas默认引擎
                    df = pd.read_excel(file_path)
                    self.log("✓ 使用默认引擎成功预览文件")
                except Exception as e2:
                    self.log(f"❌ 所有引擎都无法预览文件")
                    raise Exception(f"无法预览Excel文件，请确保文件格式正确且未被损坏。\n错误详情：{str(e2)}")
            
            preview_text = f"文件预览:\n\n"
            preview_text += f"总行数: {len(df)}\n"
            preview_text += f"列名: {', '.join(df.columns)}\n\n"
            preview_text += "前5行数据:\n"
            preview_text += df.head().to_string()
            self.preview_text.delete(1.0, tk.END)
            self.preview_text.insert(tk.END, preview_text)
        except Exception as e:
            self.log(f"❌ 预览Excel文件失败: {str(e)}")
            self.preview_text.delete(1.0, tk.END)
            self.preview_text.insert(tk.END, f"预览失败: {str(e)}")
    
    def _preview_excel(self):
        """预览选中的Excel文件"""
        file_path = self.input_file_var.get()
        if not file_path:
            messagebox.showwarning("警告", "请先选择Excel文件")
            return
        
        self._preview_excel_file(file_path)
    
    def _browse_output_dir(self):
        """浏览并选择输出目录"""
        output_dir = filedialog.askdirectory()
        if output_dir:
            # 验证目录权限
            try:
                # 尝试创建一个临时文件测试写入权限
                test_file = os.path.join(output_dir, f"test_write_{int(time.time())}.tmp")
                with open(test_file, 'w') as f:
                    f.write("test write permission")
                os.remove(test_file)
                # 权限验证成功
                self.output_dir_var.set(output_dir)
                self.log(f"已选择输出目录: {output_dir}")
            except PermissionError:
                self.log(f"⚠️ 没有对所选目录的写入权限: {output_dir}")
                messagebox.showerror(
                    "权限错误", 
                    f"您没有对所选目录的写入权限: {output_dir}\n\n请选择其他目录或以管理员身份运行程序。"
                )
            except Exception as e:
                self.log(f"⚠️ 验证目录权限时出错: {str(e)}")
                messagebox.showwarning(
                    "警告", 
                    f"验证目录权限时出错: {str(e)}\n\n可能存在权限问题，请确保程序有权写入该目录。"
                )

    def _extraction_thread(self):
        """术语抽取线程"""
        # 设置提取状态
        self.is_extracting = True
        
        # 创建单独的线程运行实际处理，防止GUI线程阻塞
        extraction_worker = threading.Thread(target=self._extraction_worker)
        extraction_worker.daemon = True
        extraction_worker.start()
        
    def _extraction_worker(self):
        """术语抽取实际工作函数"""
        try:
            input_file = self.input_file_var.get()
            output_dir = self.output_dir_var.get()
            
            if not input_file or not output_dir:
                self.log("请选择输入文件和输出目录")
                return
            
            self.log(f"正在读取Excel文件: {input_file}")
            # 根据文件扩展名选择引擎
            engine = 'openpyxl' if input_file.lower().endswith('.xlsx') else 'xlrd'
            df = pd.read_excel(input_file, engine=engine)
            
            # 检测单列Excel文件
            if len(df.columns) == 1:
                self.log("检测到单列Excel文件，将使用特殊处理")
            
            # 解析数值参数
            try:
                min_term_length = int(self.min_term_length_var.get())
                max_retries = int(self.max_retries_var.get())
            except ValueError:
                min_term_length = 2
                max_retries = 3
                self.log("⚠️ 参数解析错误，使用默认值")
            
            # 基本检查
            if not input_file or not os.path.exists(input_file):
                self.log("❌ 输入文件不存在")
                messagebox.showerror("错误", "请选择有效的输入Excel文件！")
                return
                
            if not self.api_key_var.get():
                self.log("❌ 未提供API密钥")
                messagebox.showerror("错误", "请输入有效的API密钥！")
                return
                
            if not HAS_ZHIPUAI:
                self.log("❌ 未安装zhipuai库，无法使用批处理功能")
                messagebox.showerror("错误", "未安装zhipuai库，无法使用批处理功能！请安装后再试。")
                return
            
            # 创建必要的目录
            os.makedirs(output_dir, exist_ok=True)
            chunks_dir = os.path.join(output_dir, "chunks")
            os.makedirs(chunks_dir, exist_ok=True)
            
            # 记录参数设置
            self.log(f"🔍 开始提取术语:")
            self.log(f"📁 输入文件: {input_file}")
            self.log(f"📂 输出目录: {output_dir}")
            
            # 运行提取任务
            self._run_extraction_with_params(
                input_file, chunks_dir, output_dir, self.api_key_var.get(), self.api_url_var.get(), self.model_var.get(),
                min_term_length, max_retries)
        
        except Exception as e:
            self.log(f"❌ 术语抽取过程出错: {str(e)}")
            messagebox.showerror("错误", f"术语抽取过程出错:\n{str(e)}")
            traceback.print_exc()
        finally:
            self._reset_ui_after_extraction()
    
    def _run_extraction_with_params(self, input_file: str, chunks_dir: str, output_dir: str,
                               api_key: str, api_url: str, model: str,
                               min_term_length: int, max_retries: int):
        """执行术语提取任务"""
        try:
            # 更新UI状态并初始化提取器
            self.update_ui_state("extracting")
            self.update_status("正在初始化术语提取器...")
            self.root.update_idletasks()  # 确保UI立即更新
            
            # 创建术语提取器并配置
            extractor = TermExtractor(api_key=api_key)
            extractor.model = model
            extractor.min_term_length = min_term_length
            extractor.max_retries = max_retries
            extractor.stop_event = self.stop_event
            
            # 设置回调函数
            extractor.set_callbacks(
                status_callback=self.update_status,
                progress_callback=self.update_extract_progress,
                complete_callback=self.on_extraction_complete
            )
            
            # 确保输出目录存在
            self.update_status("准备输出目录...")
            self.root.update_idletasks()
            os.makedirs(os.path.dirname(output_dir), exist_ok=True)
            
            # 生成输出文件名
            base_name = os.path.splitext(os.path.basename(input_file))[0]
            actual_output = os.path.join(output_dir, f"extracted_terms_{base_name}.xlsx")
            
            # 开始处理
            self.update_status("开始处理数据...")
            self.root.update_idletasks()
            
            # 定期执行UI更新的监视线程
            self.extraction_active = True
            ui_update_thread = threading.Thread(target=self._keep_ui_alive)
            ui_update_thread.daemon = True
            ui_update_thread.start()
            
            # 执行提取
            try:
                extractor.process_data(
                    excel_file=input_file,
                    chunks_dir=chunks_dir,
                    output_file=actual_output
                )
                self.last_output_file = actual_output
            except Exception as process_error:
                # 捕获处理过程中的错误，特别是与单列文件相关的错误
                if "window \"J_querystring\" was deleted" in str(process_error):
                    # 特殊处理此错误
                    self.log("⚠️ 检测到UI元素错误，尝试使用备用方法处理...")
                    # 强制更新UI
                    self.root.update_idletasks()
                    
                    # 读取Excel文件并识别列
                    df = pd.read_excel(input_file)
                    if len(df.columns) == 1:
                        self.log("确认检测到单列Excel文件，重新尝试处理...")
                        # 重新创建提取器，避免UI元素问题
                        new_extractor = TermExtractor(api_key=api_key)
                        new_extractor.model = model
                        new_extractor.min_term_length = min_term_length
                        new_extractor.max_retries = max_retries
                        new_extractor.stop_event = self.stop_event
                        # 不设置UI回调以避免J_querystring错误
                        new_extractor.process_data(
                            excel_file=input_file,
                            chunks_dir=chunks_dir,
                            output_file=actual_output
                        )
                        self.last_output_file = actual_output
                    else:
                        # 如果不是单列文件，重新抛出原始错误
                        raise process_error
                else:
                    # 其他类型的错误
                    raise process_error
            
            # 停止UI更新线程
            self.extraction_active = False
            
            # 检查是否被用户取消
            if self.stop_event.is_set():
                self.update_status("用户已取消操作")
                return
            
            # 完成处理
            self.update_status(f"术语提取完成！结果已保存到: {actual_output}")
            self.show_complete_message(actual_output)
            
        except Exception as e:
            logger.error(f"术语提取失败: {str(e)}")
            self.update_status(f"错误: {str(e)}")
            messagebox.showerror("错误", f"术语提取失败: {str(e)}")
            
        finally:
            # 停止UI更新线程
            self.extraction_active = False
            # 重置UI状态
            self.update_ui_state("ready")
            self.update_extract_progress(0)
    
    def _keep_ui_alive(self):
        """保持UI响应"""
        while getattr(self, 'extraction_active', False):
            try:
                self.root.update_idletasks()
            except:
                pass
            time.sleep(0.1)  # 每100ms刷新一次UI
    
    def _stop_extraction(self):
        """停止正在进行的处理，并保存已处理的数据"""
        # 确认用户确实想要停止
        if messagebox.askyesno("确认", "确定要停止当前处理吗？\n已处理的数据将被保存。"):
            self.log("⚠️ 用户请求停止处理")
            self.update_status("正在停止...")
            
            # 设置停止事件
            if hasattr(self, 'stop_event'):
                self.stop_event.set()
                
            # 更新UI
            self.extract_button.config(state=tk.NORMAL)
            self.stop_button.config(state=tk.DISABLED)
            self.is_extracting = False
    
    def _on_closing(self):
        """处理窗口关闭事件"""
        self.log("⚠️ 正在停止处理...")
        self.stop_event.set()
        self.root.destroy()

    def _open_result_file(self):
        """打开结果文件"""
        output_file = self.last_output_file
        if not output_file or not os.path.exists(output_file):
            messagebox.showwarning("警告", "结果文件不存在")
            return
        
        try:
            # 使用系统默认的文件管理器打开文件
            os.startfile(output_file)
        except:
            try:
                # 尝试使用其他方法（适用于不同操作系统）
                if sys.platform == 'darwin':  # macOS
                    subprocess.Popen(['open', output_file])
                elif sys.platform == 'linux':  # Linux
                    subprocess.Popen(['xdg-open', output_file])
                else:
                    messagebox.showwarning("警告", "无法打开结果文件")
            except:
                messagebox.showwarning("警告", "无法打开结果文件")

    def _clean_temp_files(self):
        """清理临时文件"""
        output_dir = self.output_dir_var.get()
        chunks_dir = os.path.join(output_dir, "chunks")
        
        if not os.path.exists(chunks_dir):
            self.log("没有找到临时文件目录")
            return
            
        # 确认对话框
        confirm = messagebox.askyesno(
            "确认",
            "确定要清理所有临时文件吗？\n这将删除所有中间JSONL文件和批处理结果文件。",
            icon="warning"
        )
        
        if not confirm:
            return
            
        # 删除所有JSONL和批处理结果文件
        deleted_count = 0
        try:
            for filename in os.listdir(chunks_dir):
                if filename.endswith(".jsonl"):
                    file_path = os.path.join(chunks_dir, filename)
                    os.remove(file_path)
                    deleted_count += 1
                    
            # 如果chunks_dir为空，可以删除目录
            if len(os.listdir(chunks_dir)) == 0:
                os.rmdir(chunks_dir)
                self.log(f"已删除空的临时文件目录: {chunks_dir}")
                
            self.log(f"清理完成，共删除{deleted_count}个临时文件")
        except Exception as e:
            self.log(f"清理临时文件时出错: {str(e)}")
            messagebox.showerror("错误", f"清理临时文件失败:\n{str(e)}")

    def _check_dependencies(self):
        """检查依赖库"""
        check_and_install_dependencies(self.root)

    def _clear_log(self):
        """清空日志"""
        self.log_text.config(state=tk.NORMAL)
        self.log_text.delete(1.0, tk.END)
        self.log_text.config(state=tk.DISABLED)
        self.log("日志已清空")

    def _show_help(self):
        """显示使用说明"""
        help_text = """
使用流程:

1. 准备数据文件:
   - 准备一个包含文本的Excel文件
   - 程序会自动识别出包含ID的列和文本内容列

2. 数据清洗(可选):
   - 勾选需要的清洗选项(清除数字、标点符号等)
   - "清除HTML标签"选项会保留标签内的文本内容，如<green>20:30</> 将变为 20:30
   - 程序会自动识别和清除特殊游戏格式标记（如颜色标记）
   - 点击"清洗数据"按钮
   - 清洗完成后，会自动将清洗结果设为输入文件

3. 设置参数:
   - 填写智谱AI的API密钥（必填项）
   - 选择合适的AI模型（默认已选择最快的模型）
   - 调整术语最小长度（默认为2个字符）

4. 开始提取:
   - 点击"开始提取术语"按钮
   - 等待处理完成
   - 查看并使用提取的术语结果文件

提示:
- 为获得最佳结果，请确保Excel文件中文本内容清晰明确
- 提取结果会按原文本分组保存到结果Excel文件中
- 如果处理时间过长，可以点击"停止提取"按钮
- 第一次使用时，请先测试API连接以确保设置正确
"""
        messagebox.showinfo("使用说明", help_text)

    def _show_about(self):
        """显示关于对话框"""
        about_text = f"""术语提取工具 v{self.version}

一个用于从游戏文本中提取专业术语的工具。

功能特点：
• 支持多种文本格式清洗
• 智能识别专业术语
• 支持批量处理
• 多语言支持
• 导出Excel格式

作者：刘家劭
版权所有 © 2024

本工具仅供学习和研究使用。
"""
        messagebox.showinfo("关于", about_text)

    def update_extract_progress(self, progress_value: float):
        """更新进度条并刷新UI"""
        self.extract_progress_var.set(progress_value)
        # 强制UI更新
        self.root.update_idletasks()
    
    def extraction_complete(self):
        """术语提取完成回调"""
        self.log("✅ 术语提取任务已完成")
        self.root.after(0, lambda: self.extract_progress_bar.stop())
        self.root.after(0, lambda: self.extract_button.config(state=tk.NORMAL))
        self.root.after(0, lambda: self.stop_button.config(state=tk.DISABLED))

    def _reset_ui_after_extraction(self):
        """重置UI状态"""
        self.is_extracting = False
        self.extract_button.config(state=tk.NORMAL)
        self.stop_button.config(state=tk.DISABLED)
        self.extract_progress_bar.stop()
        self.extract_progress_var.set(0)
        self.status_var.set("就绪")

    def _test_api(self):
        """测试API连接"""
        api_key = self.api_key_var.get().strip()
        
        if not api_key:
            messagebox.showerror("错误", "请输入API密钥")
            return
            
        # 检查zhipuai库
        if not HAS_ZHIPUAI:
            messagebox.showerror("错误", "未安装zhipuai库，无法测试API")
            return
            
        self.log("正在测试API连接...")
        
        try:
            # 创建测试实例
            extractor = TermExtractor(api_key=api_key)
            result = extractor.test_api_key()
            
            if result:
                self.log("✅ API测试成功！")
                messagebox.showinfo("成功", "API连接测试成功")
            else:
                self.log("❌ API测试失败！")
                messagebox.showerror("错误", "API连接测试失败，请检查API密钥")
        except Exception as e:
            self.log(f"❌ API测试出错: {str(e)}")
            messagebox.showerror("错误", f"API测试出错: {str(e)}")

    def _clean_data_thread(self):
        """开始数据清洗线程"""
        cleaning_thread = threading.Thread(target=self._run_data_cleaning)
        cleaning_thread.daemon = True
        cleaning_thread.start()
        
    def _run_data_cleaning(self):
        """执行数据清洗功能"""
        try:
            # 更新UI状态
            self.update_ui_state("cleaning")
            
            # 获取并验证输入文件
            input_file = self.input_file_var.get()
            if not input_file or not os.path.exists(input_file):
                messagebox.showerror("错误", f"输入文件不存在或未选择: {input_file}")
                self._reset_ui_after_cleaning(False)
                return
                
            # 获取输出目录并确保它存在
            output_dir = self.output_dir_var.get()
            try:
                os.makedirs(output_dir, exist_ok=True)
            except Exception as e:
                messagebox.showerror("错误", f"无法创建输出目录: {str(e)}")
                self._reset_ui_after_cleaning(False)
                return
            
            # 创建清洗后的文件名
            base_name = os.path.splitext(os.path.basename(input_file))[0]
            cleaned_file = os.path.join(output_dir, f"cleaned_{base_name}.xlsx")
            
            # 记录开始时间并读取Excel文件
            start_time = time.time()
            self.log(f"正在读取Excel文件: {input_file}")
            
            # 使用try/except捕获pandas可能的读取错误
            try:
                # 根据文件扩展名选择引擎
                engine = 'openpyxl' if input_file.lower().endswith('.xlsx') else 'xlrd'
                df = pd.read_excel(input_file, engine=engine)
                self.update_status(f"已读取 {len(df)} 行数据")
            except Exception as e:
                self.log(f"❌ 无法读取Excel文件: {str(e)}")
                messagebox.showerror("错误", f"无法读取Excel文件: {str(e)}")
                self._reset_ui_after_cleaning(False)
                return
            
            # 创建数据预处理器并识别列结构
            preprocessor = DataPreprocessor()
            id_column, text_columns = preprocessor.identify_columns(df)
            
            # 检测单列Excel文件
            is_single_column = len(df.columns) == 1
            if is_single_column:
                self.log("检测到单列Excel文件，将使用特殊处理")
                if id_column and not text_columns:
                    # 对于单列文件，我们将ID列同时作为文本列进行处理
                    self.log(f"单列文件：将唯一列 {id_column} 既作为ID列也作为文本列")
                    id_col_name = id_column
                else:
                    id_column = df.columns[0]
                    self.log(f"单列文件：使用唯一列 {id_column} 作为ID列")
            elif not text_columns:
                # 只有在不是单列文件且没有找到文本列时才询问用户
                self.log("未找到合适的文本列进行清洗")
                self.update_status("未找到合适的文本列")
                # 提示用户哪些列可用
                all_columns = df.columns.tolist()
                column_info = "\n".join([f"- {col}" for col in all_columns])
                self.log(f"文件包含以下列:\n{column_info}")
                self.log("请确保文件中至少有一列包含足够长的文本内容")
                
                # 先强制更新UI，防止对话框显示前元素被删除
                self.root.update_idletasks()
                
                # 询问用户是否要手动选择文本列
                if messagebox.askyesno("未找到文本列", 
                                      f"未找到合适的文本列进行清洗。\n\n"
                                      f"文件包含以下列:\n{column_info}\n\n"
                                      "您想手动指定一个文本列吗?"):
                    # 强制更新UI，防止对话框显示前元素被删除
                    self.root.update_idletasks()
                    
                    # 让用户选择列
                    selected_column = simpledialog.askstring(
                        "选择文本列", 
                        "请输入您要用作文本列的列名:",
                        initialvalue=all_columns[0] if all_columns else ""
                    )
                    
                    if selected_column and selected_column in all_columns:
                        text_columns = [selected_column]
                        self.log(f"已手动选择文本列: {selected_column}")
                    else:
                        messagebox.showerror("错误", "无效的列名或已取消选择")
                        self._reset_ui_after_cleaning(False)
                        return
                else:
                    # 用户选择不手动指定
                    messagebox.showerror("错误", "未找到合适的文本列进行清洗")
                    self._reset_ui_after_cleaning(False)
                    return
            
            # 记录识别的列信息
            if is_single_column:
                self.log(f"单列文件：列 {id_column} 将被清洗")
            else:
                self.log(f"找到的文本列: {', '.join(text_columns)}")
                self.log(f"找到的ID列: {id_column or '无'} {'' if not id_column else '(ID列不会被清洗)'}")
            
            # 设置清洗选项
            clean_options = {
                'numbers': self.clean_numbers_var.get(),
                'html_tags': self.clean_html_tags_var.get(),
                'hyperlinks': self.clean_urls_var.get(),
                'punctuation': self.clean_punctuation_var.get(),
                'placeholders': self.clean_placeholders_var.get(),
                'markdown_links': True,
                'email': False,
                'game_text': self.clean_html_tags_var.get(),
                'multiple_spaces': True
            }
            
            # 统计清洗信息 - 每处理一部分更新UI
            self.update_status("正在分析数据内容...")
            
            # 对于单列文件特殊处理
            if is_single_column:
                # 创建一个虚拟的统计信息
                stats = {
                    "total_rows": len(df),
                    "text_columns": 1,
                    "numbers_cleaned": 0,
                    "punctuation_cleaned": 0,
                    "urls_cleaned": 0,
                    "html_tags_cleaned": 0,
                    "placeholders_cleaned": 0,
                    "special_formats_cleaned": 0
                }
            else:
                stats = self._calculate_cleaning_stats(df, text_columns, clean_options)
            
            # 定期更新UI以防止界面卡死
            self.root.update_idletasks()
            
            # 执行数据清洗
            self.log("\n正在应用清洗规则...")
            self.update_status("正在应用清洗规则...")
            
            # 执行清洗
            cleaned_df = preprocessor.clean_text_columns(df, text_columns, id_column, keep_original=False)
            
            # 去重处理
            self.log("\n正在进行去重处理...")
            self.update_status("正在去除重复数据...")
            self.root.update_idletasks()
            
            original_rows = len(cleaned_df)
            
            if id_column and id_column in cleaned_df.columns:
                # 保留ID列，对其他列进行去重
                text_cols_for_dedup = [col for col in cleaned_df.columns if col != id_column]
                if text_cols_for_dedup:  # 确保有列可以去重
                    cleaned_df = cleaned_df.drop_duplicates(subset=text_cols_for_dedup, keep='first')
                else:
                    # 对于单列文件，如果没有其他列可以去重，则对整个数据框去重
                    cleaned_df = cleaned_df.drop_duplicates(keep='first')
            else:
                # 对所有列进行去重
                cleaned_df = cleaned_df.drop_duplicates(keep='first')
            
            # 计算去重后的统计信息
            deduped_rows = len(cleaned_df)
            removed_rows = original_rows - deduped_rows
            
            self.log(f"去重前总行数: {original_rows}")
            self.log(f"去重后总行数: {deduped_rows}")
            self.log(f"删除重复行数: {removed_rows}")
            
            # 检查输出权限
            self.update_status("检查输出权限...")
            self._check_output_permissions(output_dir)
            
            # 处理文件冲突
            self.update_status("准备保存文件...")
            cleaned_file = self._handle_file_conflict(cleaned_file)
            
            # 保存Excel文件
            try:
                self.update_status("正在保存清洗后的数据...")
                self.root.update_idletasks()
                cleaned_df.to_excel(cleaned_file, index=False)
                self.cleaned_file_var.set(cleaned_file)
                
                # 显示列信息
                self.log("\n已生成清洗后的文件，包含以下列：")
                for col in cleaned_df.columns:
                    if is_single_column:
                        column_type = "单列数据（既是ID列也是文本列）"
                    else:
                        column_type = "ID列" if col == id_column else "文本列"
                    self.log(f"- {col} ({column_type})")
                    
            except (PermissionError, Exception) as e:
                self._handle_save_error(e, cleaned_file)
                return
            
            # 输出统计信息
            self._display_cleaning_summary(input_file, cleaned_file, stats, time.time() - start_time)
            
            # 更新界面和文件设置
            self.input_file_var.set(cleaned_file)
            self.log("已自动将清洗后的文件设为输入文件")
            
            # 设置输出文件名
            base_name = os.path.splitext(os.path.basename(cleaned_file))[0]
            self.output_filename_var.set(f"{base_name}_术语提取结果.xlsx")
            
            # 重置UI状态
            self._reset_ui_after_cleaning(True)
            
        except Exception as e:
            self.log(f"数据清洗出错: {str(e)}")
            messagebox.showerror("错误", f"数据清洗出错:\n{str(e)}")
            traceback.print_exc()

    def _calculate_cleaning_stats(self, df, text_columns, clean_options):
        """计算清洗统计信息"""
        stats = {
            "total_rows": len(df),
            "text_columns": len(text_columns),
            "numbers_cleaned": 0,
            "punctuation_cleaned": 0,
            "urls_cleaned": 0,
            "html_tags_cleaned": 0,
            "placeholders_cleaned": 0,
            "special_formats_cleaned": 0
        }
        
        for col in text_columns:
            if not isinstance(df[col].iloc[0], str):
                continue  # 跳过非文本列
            
            # 计算需要清洗的模式数量
            if clean_options['numbers']:
                self.log(f"扫描列 {col}: 清除数字")
                stats["numbers_cleaned"] += sum(1 for x in df[col] if isinstance(x, str) and re.search(r'\d', x))
                
            if clean_options['punctuation']:
                self.log(f"扫描列 {col}: 清除标点符号")
                stats["punctuation_cleaned"] += sum(1 for x in df[col] if isinstance(x, str) and re.search(r'[^\w\s]', x))
                
            if clean_options['hyperlinks']:
                self.log(f"扫描列 {col}: 清除超链接")
                stats["urls_cleaned"] += sum(1 for x in df[col] if isinstance(x, str) and re.search(r'https?://\S+|www\.\S+', x))
                
            if clean_options['html_tags']:
                self.log(f"扫描列 {col}: 清除HTML标签")
                html_tag_count = sum(1 for x in df[col] if isinstance(x, str) and re.search(r'<[^>]*>', x))
                if html_tag_count > 0:
                    self.log(f"检测到 {html_tag_count} 行包含HTML标签")
                stats["html_tags_cleaned"] += html_tag_count
                
            if clean_options['placeholders']:
                self.log(f"扫描列 {col}: 清除占位符")
                stats["placeholders_cleaned"] += sum(1 for x in df[col] if isinstance(x, str) and re.search(r'\{\{.*?\}\}|\{\%.*?\%\}', x))
        
        return stats
        
    def _check_output_permissions(self, output_dir):
        """检查输出目录权限"""
        try:
            temp_file = os.path.join(output_dir, f"temp_{int(time.time())}.txt")
            with open(temp_file, 'w') as f:
                f.write("test")
            os.remove(temp_file)
        except Exception as e:
            self.log(f"⚠️ 输出目录权限检查失败: {str(e)}")
            raise PermissionError(f"无法写入到输出目录 {output_dir}，请检查目录权限")
            
    def _handle_file_conflict(self, file_path):
        """处理文件冲突"""
        if os.path.exists(file_path):
            try:
                # 尝试以写入模式打开文件
                with open(file_path, 'a'):
                    pass
                return file_path
            except PermissionError:
                # 文件可能被其他程序打开，创建新文件名
                timestamp = time.strftime("%Y%m%d_%H%M%S")
                base, ext = os.path.splitext(file_path)
                new_path = f"{base}_{timestamp}{ext}"
                self.log(f"⚠️ 文件 {file_path} 可能被其他程序打开")
                self.log(f"将使用新文件名: {new_path}")
                return new_path
        return file_path
        
    def _handle_save_error(self, error, file_path):
        """处理保存错误"""
        if isinstance(error, PermissionError):
            self.log(f"❌ 文件访问权限错误: {str(error)}")
            error_msg = (
                f"无法保存到文件 {file_path}。可能的原因:\n"
                "1. 文件正在被其他程序（如Excel）打开\n"
                "2. 您没有对该目录的写入权限\n"
                "3. 文件被设置为只读\n\n"
                "请关闭可能打开此文件的程序，或选择不同的输出目录。"
            )
            messagebox.showerror("权限错误", error_msg)
        else:
            self.log(f"❌ 保存文件时出错: {str(error)}")
            messagebox.showerror("保存错误", f"保存文件时出错:\n{str(error)}")
        self._reset_ui_after_cleaning(False)
        
    def _display_cleaning_summary(self, input_file, output_file, stats, elapsed_time):
        """显示清洗总结信息"""
        self.log("\n===== 数据清洗完成 =====")
        self.log(f"处理文件: {input_file}")
        self.log(f"输出文件: {output_file}")
        self.log(f"总行数: {stats['total_rows']}")
        self.log(f"处理文本列数: {stats['text_columns']}")
        self.log(f"清除数字: {'是' if self.clean_numbers_var.get() else '否'} ({stats['numbers_cleaned']}处)")
        self.log(f"清除标点: {'是' if self.clean_punctuation_var.get() else '否'} ({stats['punctuation_cleaned']}处)")
        self.log(f"清除URL: {'是' if self.clean_urls_var.get() else '否'} ({stats['urls_cleaned']}处)")
        self.log(f"清除HTML标签: {'是' if self.clean_html_tags_var.get() else '否'} ({stats['html_tags_cleaned']}处)")
        self.log(f"清除占位符: {'是' if self.clean_placeholders_var.get() else '否'} ({stats['placeholders_cleaned']}处)")
        self.log(f"清除特殊格式: {'是' if self.clean_html_tags_var.get() else '否'} ({stats['special_formats_cleaned']}处)")
        self.log(f"处理耗时: {elapsed_time:.2f}秒")
        self.log(f"文件中不包含原始数据，仅保留ID列和清洗后的内容，适合直接用于大模型训练")
        
        # 显示成功消息
        messagebox.showinfo("完成", f"数据清洗完成，结果已保存至\n{output_file}")

    def _reset_ui_after_cleaning(self, success):
        """重置UI状态"""
        # 确保停止进度条动画
        try:
            self.extract_progress_bar.stop()
        except:
            pass
            
        # 完全重置UI状态
        self.update_ui_state("ready")
        
        # 更新状态文本
        if success:
            self.status_var.set("数据清洗完成")
        else:
            self.status_var.set("数据清洗失败")
            
        # 强制UI更新
        self.root.update_idletasks()

    def update_ui_state(self, state: str):
        """更新UI状态"""
        if state == "extracting":
            self.is_extracting = True
            self.extract_button.config(state=tk.DISABLED)
            self.clean_button.config(state=tk.DISABLED)
            self.stop_button.config(state=tk.NORMAL)
            self.extract_progress_bar.config(mode='determinate')
            self.extract_progress_var.set(0)
            self.show_process_indicator(True)
        elif state == "cleaning":
            self.extract_button.config(state=tk.DISABLED)
            self.clean_button.config(state=tk.DISABLED)
            self.stop_button.config(state=tk.NORMAL)
            self.extract_progress_bar.config(mode='indeterminate')
            self.extract_progress_bar.start(10)
            self.show_process_indicator(True)
        elif state == "ready":
            self.is_extracting = False
            self.extract_button.config(state=tk.NORMAL)
            self.clean_button.config(state=tk.NORMAL)
            self.stop_button.config(state=tk.DISABLED)
            self.extract_progress_bar.config(mode='determinate')
            self.extract_progress_bar.stop()
            self.extract_progress_var.set(0)
            self.show_process_indicator(False)
            
    def update_status(self, status_text: str):
        """更新状态文本并刷新UI"""
        self.status_var.set(status_text)
        # 强制UI更新
        self.root.update_idletasks()
        
    def show_complete_message(self, output_file: str):
        """显示完成消息"""
        self.last_output_file = output_file
        message = f"术语提取已完成！\n\n结果已保存到：\n{output_file}"
        messagebox.showinfo("完成", message)
        
    def on_extraction_complete(self):
        """提取完成的回调"""
        self.log("✅ 术语提取完成")
        self.status_var.set("完成")
        # 确保GUI更新
        self.root.update_idletasks()

    def show_process_indicator(self, show: bool):
        """显示或隐藏处理指示器"""
        if show:
            self._start_process_indicator()
        else:
            self._stop_process_indicator()
    
    def _start_process_indicator(self):
        """启动处理指示器动画"""
        self.process_indicator_running = True
        self._update_process_indicator()
    
    def _stop_process_indicator(self):
        """停止处理指示器动画"""
        self.process_indicator_running = False
        self.process_indicator_var.set("")
    
    def _update_process_indicator(self):
        """更新处理指示器动画"""
        if not hasattr(self, 'process_indicator_running') or not self.process_indicator_running:
            return
            
        indicators = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
        if not hasattr(self, 'indicator_index'):
            self.indicator_index = 0
        
        self.process_indicator_var.set(f"{indicators[self.indicator_index]} 处理中...")
        self.indicator_index = (self.indicator_index + 1) % len(indicators)
        
        # 每100ms更新一次
        self.root.after(100, self._update_process_indicator)

    def _check_and_start_extraction(self):
        """检查是否可以开始提取，如果可以则启动提取线程"""
        if self.is_extracting:
            messagebox.showinfo("提示", "术语提取已在进行中，请等待当前任务完成")
            return
        self._extraction_thread()

    def _fix_excel_dependencies(self):
        """修复Excel依赖问题"""
        try:
            # 检查修复脚本是否存在
            fix_script = "fix_excel_dependencies.py"
            if not os.path.exists(fix_script):
                self.log("❌ 未找到Excel依赖修复工具，无法修复")
                messagebox.showerror("错误", "未找到Excel依赖修复工具，无法修复")
                return
            
            self.log("正在启动Excel依赖修复工具...")
            
            # 运行修复脚本
            python_exe = sys.executable
            subprocess.Popen([python_exe, fix_script])
            
            self.log("✓ Excel依赖修复工具已启动，请按照提示操作")
            self.log("⚠️ 完成修复后，请重启术语提取工具以应用更改")
            
        except Exception as e:
            self.log(f"❌ 启动Excel依赖修复工具失败: {str(e)}")
            messagebox.showerror("错误", f"启动Excel依赖修复工具失败:\n{str(e)}")


class TermExtractorWrapper(TermExtractor):
    """术语抽取器的包装类，用于在GUI中使用"""
    
    def __init__(self, api_key=None, api_url=None, 
                min_term_length=2, max_retries=3):
        """
        初始化术语抽取器包装类
        
        Args:
            api_key: API密钥
            api_url: API URL地址
            min_term_length: 术语最小长度
            max_retries: 最大重试次数
        """
        super().__init__(api_key=api_key, api_url=api_url)
        
        # 设置参数
        self.min_term_length = min_term_length
        self.max_retries = max_retries
        
        # 设置停止事件
        self.stop_event = threading.Event()


def run_diagnostic_mode():
    """运行诊断模式，测试文件导出功能"""
    print("🔍 运行诊断模式...")
    
    # 设置日志
    import logging
    logging.basicConfig(level=logging.DEBUG, 
                        format='[%(asctime)s] %(levelname)s - %(message)s',
                        datefmt='%H:%M:%S')
    
    # 创建测试数据
    test_data = [
        {"source_id": "test_1", "source_text": "这是第一个测试句子", "term": "测试句子", "context": "这是第一个测试句子"},
        {"source_id": "test_1", "source_text": "这是第一个测试句子", "term": "第一个", "context": "这是第一个测试句子"},
        {"source_id": "test_2", "source_text": "这是RPG游戏的专有名词", "term": "RPG游戏", "context": "这是RPG游戏的专有名词"},
        {"source_id": "test_2", "source_text": "这是RPG游戏的专有名词", "term": "专有名词", "context": "这是RPG游戏的专有名词"}
    ]
    
    print(f"📊 创建了 {len(test_data)} 条测试数据")
    
    # 创建TermExtractor
    try:
        from term_extractor import TermExtractor
        extractor = TermExtractor()
        print("✅ 成功创建TermExtractor实例")
    except Exception as e:
        print(f"❌ 创建TermExtractor实例失败: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # 测试文件夹权限
    import os
    output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')
    os.makedirs(output_dir, exist_ok=True)
    test_file = os.path.join(output_dir, 'test_write_permission.txt')
    
    try:
        with open(test_file, 'w') as f:
            f.write("Test write permission")
        print(f"✅ 成功写入测试文件: {test_file}")
        os.remove(test_file)
        print(f"✅ 成功删除测试文件")
    except Exception as e:
        print(f"❌ 文件系统权限测试失败: {e}")
        return
    
    # 测试导出Excel
    output_file = os.path.join(output_dir, f'diagnostic_test_{time.strftime("%Y%m%d_%H%M%S")}.xlsx')
    print(f"📝 开始导出到: {output_file}")
    
    try:
        result = extractor.export_to_excel(test_data, output_file)
        if result.get("success", False):
            print(f"✅ 导出成功: {result.get('message', '')}")
            print(f"📄 输出文件: {result.get('output_file', output_file)}")
        else:
            print(f"❌ 导出失败: {result.get('message', '未知错误')}")
    except Exception as e:
        print(f"❌ 导出时发生异常: {e}")
        import traceback
        traceback.print_exc()
    
    print("🔍 诊断完成")


def main():
    """程序入口函数"""
    root = tk.Tk()
    
    # 检查并安装必要依赖
    if not check_and_install_dependencies(root):
        # 如果依赖检查失败，显示警告但继续启动
        messagebox.showwarning("警告", "部分依赖库未安装，程序可能无法正常工作。请从菜单中选择'工具'->'检查依赖库'。")
    
    # 检查是否有诊断模式参数
    if len(sys.argv) > 1 and sys.argv[1] == "--diagnostic":
        run_diagnostic_mode()
    else:
        app = TermExtractorGUI(root)
        root.mainloop()


if __name__ == "__main__":
    main() 