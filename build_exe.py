"""
构建术语提取工具可执行文件
此脚本使用PyInstaller将术语提取工具打包为Windows可执行文件
"""

import PyInstaller.__main__
import shutil
import os
from pathlib import Path

print("开始构建术语提取工具可执行文件...")

# 确保输出目录存在
dist_dir = Path("dist")
if not dist_dir.exists():
    dist_dir.mkdir()

# 应用图标路径（如果有的话）
# icon_path = Path("icon.ico") 
# icon_param = f"--icon={icon_path}" if icon_path.exists() else ""

# 构建命令
PyInstaller.__main__.run([
    'term_extractor_gui.py',  # 主程序文件
    '--name=术语提取工具',      # 可执行文件名称
    '--onefile',              # 生成单个可执行文件
    '--windowed',             # 不显示控制台窗口
    '--clean',                # 清理临时文件
    '--add-data=config.json;.',  # 添加配置文件
    # icon_param,             # 图标参数（如果有）
])

# 复制必要的文件到dist目录
additional_files = [
    "requirements.txt", 
    "config.json"
]

for file in additional_files:
    if os.path.exists(file):
        shutil.copy(file, os.path.join("dist", file))
        print(f"已复制 {file} 到dist目录")

# 创建output目录
output_dir = os.path.join("dist", "output")
if not os.path.exists(output_dir):
    os.makedirs(output_dir)
    print("已创建output目录")

print("构建完成！可执行文件位于dist目录") 