"""
术语提取工具高级打包脚本
此脚本使用PyInstaller进行高级打包配置，确保所有依赖被正确包含
"""

import os
import sys
import shutil
import subprocess
from pathlib import Path

def main():
    print("===== 术语提取工具高级打包脚本 =====")
    print("正在准备打包环境...")
    
    # 确保安装了所有依赖
    print("正在确认所有依赖已安装...")
    subprocess.run([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
    
    # 确保安装了PyInstaller
    subprocess.run([sys.executable, "-m", "pip", "install", "pyinstaller"])
    
    # 创建spec文件
    spec_content = """# -*- mode: python ; coding: utf-8 -*-


block_cipher = None


a = Analysis(
    ['term_extractor_gui.py'],
    pathex=[],
    binaries=[],
    datas=[
        ('config.json', '.'),
        ('term_extractor.py', '.'),
        ('data_preprocessor.py', '.'),
        ('json_utils.py', '.'),
        ('check_dependencies.py', '.'),
        ('fix_excel_dependencies.py', '.'),
    ],
    hiddenimports=[
        'pandas', 
        'numpy', 
        'openpyxl', 
        'xlrd', 
        'xlsxwriter', 
        'requests', 
        'zhipuai', 
        'json_repair', 
        'tqdm',
        'tkinter',
        'tkinter.ttk',
        'tkinter.filedialog',
        'tkinter.messagebox',
        'tkinter.scrolledtext',
        'tkinter.simpledialog',
        'threading',
        'queue',
        'logging',
        'datetime',
        'pathlib',
        'shutil',
        're',
        'subprocess',
        'traceback',
        'json',
        'time',
        'sys',
        'os',
        'PIL',
        'PIL.Image',
        'PIL.ImageTk',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)
pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='术语提取工具',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=None,  # 如有图标，请指定路径
)
"""
    
    # 写入spec文件
    with open("term_extractor.spec", "w", encoding="utf-8") as f:
        f.write(spec_content)
    
    print("已创建PyInstaller配置文件...")
    
    # 运行PyInstaller
    print("开始打包，这可能需要几分钟时间...")
    subprocess.run(["pyinstaller", "term_extractor.spec", "--clean"])
    
    # 在dist目录中创建一个output文件夹
    output_dir = os.path.join("dist", "output")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # 创建使用说明文件
    readme_content = """术语提取工具 使用说明
====================

一、软件简介
-----------
本软件是基于智谱AI的专业术语提取工具，可以从各种文本中自动提取专业术语。
无需安装Python，可在Windows系统上直接运行。

二、使用前准备
-----------
1. 您需要准备好包含待提取术语的Excel文件，文件中应包含文本内容列。
2. 如使用智谱AI功能，需准备好API密钥（在软件内设置）。
3. 请确保您的计算机能够访问互联网（如需使用在线API）。

三、软件使用方法
-----------
1. 双击运行"术语提取工具.exe"启动软件。
2. 在软件界面中选择Excel输入文件。
3. 设置输出目录（默认为软件目录下的output文件夹）。
4. 设置API密钥（如使用智谱AI功能）。
5. 点击"数据清洗"对数据进行预处理（可选）。
6. 点击"开始提取"开始术语提取过程。
7. 等待提取完成，结果将保存在指定的输出目录中。

四、注意事项
-----------
1. 首次运行时，可能会被杀毒软件拦截，请将软件添加到杀毒软件的信任列表中。
2. 如遇到"找不到某某DLL"的错误，可尝试安装最新的Visual C++ Redistributable。
3. 请勿删除软件目录下的任何文件，以免影响软件正常运行。
4. 处理大文件时，软件可能需要较长时间，请耐心等待。

五、故障排除
-----------
1. 如软件无法启动，请尝试以管理员身份运行。
2. 如提取过程中断，请检查网络连接和API密钥是否正确。
3. 如输出结果不完整，请检查Excel文件格式是否正确。

如有任何问题，请联系技术支持。

祝您使用愉快！
"""
    
    # 写入使用说明文件
    with open(os.path.join("dist", "使用说明.txt"), "w", encoding="utf-8") as f:
        f.write(readme_content)
    
    # 复制任何额外需要的文件
    for file in ["requirements.txt", "config.json"]:
        if os.path.exists(file):
            shutil.copy(file, os.path.join("dist", file))
    
    print("\n===== 打包完成 =====")
    print("可执行文件位于dist目录")
    print("请将整个dist目录复制到目标机器使用。")
    print("提示: 在目标机器上首次运行时可能会被杀毒软件拦截，请添加信任或例外。")

if __name__ == "__main__":
    main() 