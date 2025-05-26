"""
依赖检查模块 - 检查必要的Python库是否已安装
提供自动安装缺失库的功能
"""

import importlib
import subprocess
import sys
import tkinter as tk
from tkinter import messagebox

# 必要库及其版本
REQUIRED_LIBRARIES = {
    "pandas": "1.0.0",
    "xlrd": "1.2.0",  # 明确要求xlrd 1.2.0版本，这是支持.xlsx的最后一个版本
    "openpyxl": "3.0.0",
    "xlsxwriter": "1.0.0",
    "requests": "2.0.0"
}

# 可选但推荐的库
OPTIONAL_LIBRARIES = {
    "json_repair": "0.1.0",
    "zhipuai": "1.0.0"
}

def check_library(library, min_version=None):
    """
    检查库是否已安装，及其版本是否满足要求
    
    Args:
        library: 库名称
        min_version: 最低版本要求，如 "1.0.0"
        
    Returns:
        (installed, version): 是否已安装及其版本
    """
    try:
        # 尝试导入库
        module = importlib.import_module(library)
        
        # 获取版本 - 增强版本检测逻辑
        try:
            # 尝试多种方式获取版本
            if hasattr(module, "__version__"):
                version = module.__version__
            elif hasattr(module, "version"):
                version = module.version
            elif hasattr(module, "VERSION"):
                version = module.VERSION
            # 针对pandas等特殊库的处理
            elif library == "pandas" and importlib.util.find_spec("pandas") is not None:
                version = "已安装"
            elif library == "xlrd" and importlib.util.find_spec("xlrd") is not None:
                version = "已安装"
            elif library == "openpyxl" and importlib.util.find_spec("openpyxl") is not None:
                version = "已安装"
            else:
                # 使用pip命令获取版本
                try:
                    import pkg_resources
                    version = pkg_resources.get_distribution(library).version
                except:
                    version = "已安装，版本未知"
        except:
            version = "已安装，版本未知"
        
        # 如果无法检测版本但确实已安装，视为通过
        if version in ["已安装", "已安装，版本未知"] and min_version:
            print(f"{library}已安装，但无法检测版本。假定其满足要求。")
            return True, version
        
        # 检查版本要求（如果有）
        if min_version and version != "未知" and version not in ["已安装", "已安装，版本未知"]:
            try:
                # 提取版本号的数字部分
                ver_parts = version.split(".")
                min_ver_parts = min_version.split(".")
                
                # 确保只比较数字部分
                ver_nums = []
                for part in ver_parts:
                    try:
                        # 删除版本号中的字母部分
                        num_part = ''.join(c for c in part if c.isdigit() or c == '.')
                        ver_nums.append(int(num_part) if num_part else 0)
                    except ValueError:
                        ver_nums.append(0)
                
                min_ver_nums = []
                for part in min_ver_parts:
                    try:
                        num_part = ''.join(c for c in part if c.isdigit() or c == '.')
                        min_ver_nums.append(int(num_part) if num_part else 0)
                    except ValueError:
                        min_ver_nums.append(0)
                
                # 补齐长度不足的版本号
                while len(ver_nums) < len(min_ver_nums):
                    ver_nums.append(0)
                while len(min_ver_nums) < len(ver_nums):
                    min_ver_nums.append(0)
                
                # 逐位比较
                for i in range(len(ver_nums)):
                    if ver_nums[i] < min_ver_nums[i]:
                        return False, version
                    elif ver_nums[i] > min_ver_nums[i]:
                        break  # 当前版本更高
            except Exception as e:
                print(f"版本比较出错: {e}")
                # 出错时保守处理，认为版本不符
                return False, version
        
        return True, version
    except (ImportError, ModuleNotFoundError):
        return False, None

def install_library(library, version=None):
    """
    安装指定的库
    
    Args:
        library: 库名称
        version: 指定版本（可选）
        
    Returns:
        success: 安装是否成功
    """
    try:
        package = library if not version else f"{library}=={version}"
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
        return True
    except subprocess.CalledProcessError:
        return False

def fix_excel_dependencies():
    """
    专门修复Excel相关依赖，确保能够正确读取Excel文件
    
    Returns:
        success: 修复是否成功
    """
    print("检查Excel相关依赖...")
    
    # 特别检查xlrd版本，确保是1.2.0（支持.xlsx的最后一个版本）
    xlrd_installed, xlrd_version = check_library("xlrd")
    if not xlrd_installed or xlrd_version != "1.2.0":
        print(f"xlrd版本不是1.2.0 (当前: {xlrd_version})，正在修复...")
        try:
            # 卸载现有xlrd（如果有）
            subprocess.check_call([sys.executable, "-m", "pip", "uninstall", "-y", "xlrd"])
            # 安装1.2.0版本
            success = install_library("xlrd", "1.2.0")
            if success:
                print("✓ xlrd 1.2.0 安装成功")
            else:
                print("✗ xlrd 1.2.0 安装失败")
                return False
        except Exception as e:
            print(f"修复xlrd失败: {str(e)}")
            return False
    
    # 确保openpyxl已安装
    openpyxl_installed, openpyxl_version = check_library("openpyxl")
    if not openpyxl_installed:
        print("openpyxl未安装，正在安装...")
        success = install_library("openpyxl")
        if not success:
            print("✗ openpyxl安装失败")
            return False
    
    # 确保pandas已安装
    pandas_installed, pandas_version = check_library("pandas")
    if not pandas_installed:
        print("pandas未安装，正在安装...")
        success = install_library("pandas")
        if not success:
            print("✗ pandas安装失败")
            return False
    
    print("Excel相关依赖检查完成！")
    return True

def test_excel_reading_capability(path=None):
    """
    测试Excel文件读取能力
    
    Args:
        path: 测试文件路径，如果为None则仅检查库的兼容性
        
    Returns:
        (success, error_message): 是否成功及错误信息
    """
    print("测试Excel文件读取能力...")
    
    try:
        import pandas as pd
        print("✓ pandas 导入成功")
    except ImportError:
        return False, "pandas库导入失败"
    
    try:
        import openpyxl
        print("✓ openpyxl 导入成功")
    except ImportError:
        return False, "openpyxl库导入失败"
    
    try:
        import xlrd
        print(f"✓ xlrd 导入成功 (版本: {xlrd.__version__})")
        
        # 确认xlrd版本是否为1.2.0（支持.xlsx的最后一个版本）
        if hasattr(xlrd, "__version__") and xlrd.__version__ != "1.2.0":
            print(f"! xlrd版本不是1.2.0 (当前: {xlrd.__version__})，可能无法读取.xlsx文件")
    except ImportError:
        return False, "xlrd库导入失败"
    
    # 如果提供了测试文件，尝试读取
    if path:
        try:
            print(f"尝试读取Excel文件: {path}")
            # 首先尝试使用openpyxl读取
            try:
                df_openpyxl = pd.read_excel(path, engine='openpyxl')
                print(f"✓ 使用openpyxl引擎成功读取: {len(df_openpyxl)}行 × {len(df_openpyxl.columns)}列")
            except Exception as e1:
                print(f"! openpyxl引擎读取失败: {str(e1)}")
                
                # 尝试使用xlrd引擎
                try:
                    df_xlrd = pd.read_excel(path, engine='xlrd')
                    print(f"✓ 使用xlrd引擎成功读取: {len(df_xlrd)}行 × {len(df_xlrd.columns)}列")
                except Exception as e2:
                    print(f"! xlrd引擎读取也失败: {str(e2)}")
                    
                    # 最后尝试使用默认引擎
                    try:
                        df_default = pd.read_excel(path)
                        print(f"✓ 使用默认引擎成功读取: {len(df_default)}行 × {len(df_default.columns)}列")
                    except Exception as e3:
                        print(f"! 所有引擎都无法读取文件: {str(e3)}")
                        return False, f"所有引擎都无法读取Excel文件:\nopenpyxl错误: {str(e1)}\nxlrd错误: {str(e2)}\n默认引擎错误: {str(e3)}"
        except Exception as e:
            return False, f"Excel文件读取测试失败: {str(e)}"
    
    print("Excel读取能力测试完成！")
    return True, "Excel文件读取能力正常"

def check_and_install_dependencies(parent_window=None, ask_optional=True, fix_excel=True):
    """
    检查所有依赖并提供安装选项
    
    Args:
        parent_window: 父窗口，用于显示消息框
        ask_optional: 是否询问安装可选库
        fix_excel: 是否自动修复Excel相关依赖
        
    Returns:
        all_required_installed: 所有必要库是否已安装
    """
    print("开始检查依赖库...")
    
    # 如果需要自动修复Excel依赖
    if fix_excel:
        excel_fixed = fix_excel_dependencies()
        if excel_fixed:
            print("Excel依赖已自动修复")
        else:
            print("Excel依赖修复失败，请手动安装xlrd==1.2.0")
            if parent_window:
                messagebox.showwarning(
                    "Excel依赖修复失败", 
                    "无法自动修复Excel相关依赖，可能导致Excel文件读取失败。\n"
                    "请手动运行以下命令：\n"
                    "pip install xlrd==1.2.0",
                    parent=parent_window
                )
    
    missing_required = []
    outdated_required = []
    installed_required = []  # 记录已安装的库
    
    # 检查必要库
    for library, min_version in REQUIRED_LIBRARIES.items():
        print(f"检查库 {library}...")
        installed, version = check_library(library, min_version)
        if installed:
            print(f"✓ {library} 已安装 (版本: {version})")
            installed_required.append((library, version))
        else:
            if version:  # 已安装但版本过低
                print(f"⚠ {library} 版本过低 (当前: {version}, 需要: {min_version})")
                outdated_required.append((library, version, min_version))
            else:  # 未安装
                print(f"✗ {library} 未安装")
                missing_required.append(library)
    
    # 检查可选库
    missing_optional = []
    installed_optional = []
    if ask_optional:
        for library, min_version in OPTIONAL_LIBRARIES.items():
            print(f"检查可选库 {library}...")
            installed, version = check_library(library)
            if installed:
                print(f"✓ {library} 已安装 (版本: {version})")
                installed_optional.append((library, version))
            else:
                print(f"? {library} 未安装 (可选)")
                missing_optional.append(library)
    
    # 输出检查结果摘要
    print("\n依赖检查摘要:")
    if installed_required:
        print("已安装的必要库:")
        for lib, ver in installed_required:
            print(f"- {lib}: {ver}")
    
    if installed_optional:
        print("\n已安装的可选库:")
        for lib, ver in installed_optional:
            print(f"- {lib}: {ver}")
    
    # 只有在真正缺少必要库时才提示
    if missing_required or outdated_required:
        message = "检测到缺少或过时的必要依赖库:\n\n"
        
        if missing_required:
            message += "缺少的库:\n"
            for lib in missing_required:
                message += f"• {lib}\n"
            message += "\n"
            
        if outdated_required:
            message += "版本过低的库:\n"
            for lib, current, required in outdated_required:
                message += f"• {lib} (当前: {current}, 需要: {required})\n"
            message += "\n"
            
        message += "是否自动安装/更新这些库？"
        
        if parent_window:
            install = messagebox.askyesno("缺少必要依赖", message, parent=parent_window)
        else:
            install = messagebox.askyesno("缺少必要依赖", message)
            
        if install:
            # 安装缺失的必要库
            failed = []
            for library in missing_required:
                version = REQUIRED_LIBRARIES[library]
                print(f"安装 {library}...")
                if not install_library(library, version):
                    failed.append(library)
                    print(f"✗ 安装 {library} 失败")
                else:
                    print(f"✓ 安装 {library} 成功")
            
            # 更新版本过低的库
            for library, _, min_version in outdated_required:
                print(f"更新 {library}...")
                if not install_library(library, min_version):
                    failed.append(library)
                    print(f"✗ 更新 {library} 失败")
                else:
                    print(f"✓ 更新 {library} 成功")
                    
            if failed:
                error_msg = "以下库安装失败，请手动安装:\n"
                for lib in failed:
                    error_msg += f"• {lib}\n"
                    
                if parent_window:
                    messagebox.showerror("安装失败", error_msg, parent=parent_window)
                else:
                    messagebox.showerror("安装失败", error_msg)
                return False
            else:
                success_msg = "所有必要库已成功安装/更新！程序将重新启动以应用更改。"
                if parent_window:
                    messagebox.showinfo("安装成功", success_msg, parent=parent_window)
                else:
                    messagebox.showinfo("安装成功", success_msg)
                    
                # 重启程序
                python = sys.executable
                script = sys.argv[0]
                subprocess.Popen([python, script])
                sys.exit(0)
        else:
            warning_msg = "未安装必要的库，程序可能无法正常工作。"
            if parent_window:
                messagebox.showwarning("警告", warning_msg, parent=parent_window)
            else:
                messagebox.showwarning("警告", warning_msg)
            return False
    else:
        print("\n✓ 所有必要库已正确安装！")
    
    # 如果有缺失的可选库
    if missing_optional:
        message = "检测到缺少以下推荐的库:\n\n"
        for lib in missing_optional:
            message += f"• {lib}\n"
        message += "\n这些库可以提高程序的功能和性能。是否安装？"
        
        if parent_window:
            install = messagebox.askyesno("推荐安装", message, parent=parent_window)
        else:
            install = messagebox.askyesno("推荐安装", message)
            
        if install:
            # 安装缺失的可选库
            for library in missing_optional:
                print(f"安装可选库 {library}...")
                if install_library(library, OPTIONAL_LIBRARIES.get(library)):
                    print(f"✓ 安装 {library} 成功")
                else:
                    print(f"⚠ 安装 {library} 失败，但不影响核心功能")
    
    return True  # 所有必要库已正确安装

if __name__ == "__main__":
    # 如果直接运行此脚本，执行依赖检查
    root = tk.Tk()
    root.withdraw()  # 隐藏主窗口
    check_and_install_dependencies()
    root.destroy() 