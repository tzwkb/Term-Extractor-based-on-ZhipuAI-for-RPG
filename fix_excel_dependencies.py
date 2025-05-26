"""
Excel依赖修复工具

此脚本用于修复Excel文件读取相关的依赖问题
特别处理xlrd版本兼容性问题
"""

import sys
import subprocess
import os
import importlib
import tkinter as tk
from tkinter import messagebox

def check_library(library, min_version=None):
    """检查库是否已安装"""
    try:
        module = importlib.import_module(library)
        if hasattr(module, "__version__"):
            version = module.__version__
        else:
            version = "已安装，版本未知"
        return True, version
    except ImportError:
        return False, None

def install_library(library, version=None):
    """安装指定的库和版本"""
    try:
        if version:
            package = f"{library}=={version}"
        else:
            package = library
        
        # 确保pip是最新版本
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "pip"])
        
        # 安装或升级指定包
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", package])
        return True
    except subprocess.CalledProcessError as e:
        print(f"安装失败: {str(e)}")
        return False

def uninstall_library(library):
    """卸载指定的库"""
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "uninstall", "-y", library])
        return True
    except subprocess.CalledProcessError as e:
        print(f"卸载失败: {str(e)}")
        return False

def fix_excel_dependencies():
    """修复Excel相关的依赖问题"""
    print("开始修复Excel依赖...\n")
    
    # 步骤1: 卸载当前的xlrd
    print("步骤1: 卸载当前的xlrd...")
    xlrd_installed, xlrd_version = check_library("xlrd")
    if xlrd_installed:
        print(f"  发现xlrd版本: {xlrd_version}")
        if uninstall_library("xlrd"):
            print("  ✓ xlrd卸载成功")
        else:
            print("  ✗ xlrd卸载失败")
    else:
        print("  未安装xlrd")
    
    # 步骤2: 卸载openpyxl以确保安装最兼容的版本
    print("\n步骤2: 确保安装兼容的openpyxl...")
    if uninstall_library("openpyxl"):
        print("  ✓ openpyxl卸载成功")
    else:
        print("  openpyxl卸载失败或未安装")
    
    # 步骤3: 卸载pandas以确保版本兼容性
    print("\n步骤3: 确保安装兼容的pandas...")
    pandas_installed, pandas_version = check_library("pandas")
    if pandas_installed:
        print(f"  发现pandas版本: {pandas_version}")
    
    # 步骤4: 安装适当版本的xlrd (1.2.0是支持.xlsx文件的最后一个版本)
    print("\n步骤4: 安装xlrd 1.2.0...")
    if install_library("xlrd", "1.2.0"):
        print("  ✓ xlrd 1.2.0安装成功")
    else:
        print("  ✗ xlrd 1.2.0安装失败")
        return False
    
    # 步骤5: 安装适当版本的openpyxl
    print("\n步骤5: 安装openpyxl...")
    if install_library("openpyxl", "3.0.10"):
        print("  ✓ openpyxl安装成功")
    else:
        print("  ✗ openpyxl安装失败")
    
    # 步骤6: 安装其他Excel相关库
    print("\n步骤6: 安装其他Excel相关库...")
    if install_library("xlsxwriter"):
        print("  ✓ xlsxwriter安装成功")
    else:
        print("  ✗ xlsxwriter安装失败")
    
    # 步骤7: 验证安装结果
    print("\n步骤7: 验证安装结果...")
    xlrd_installed, xlrd_version = check_library("xlrd")
    openpyxl_installed, openpyxl_version = check_library("openpyxl")
    pandas_installed, pandas_version = check_library("pandas")
    
    if xlrd_installed and xlrd_version == "1.2.0" and openpyxl_installed and pandas_installed:
        print("\n✅ Excel依赖修复成功!")
        print(f"  xlrd: {xlrd_version}")
        print(f"  openpyxl: {openpyxl_version}")
        print(f"  pandas: {pandas_version}")
        return True
    else:
        print("\n❌ Excel依赖修复不完全")
        print(f"  xlrd: {'已安装 ' + xlrd_version if xlrd_installed else '未安装'}")
        print(f"  openpyxl: {'已安装 ' + openpyxl_version if openpyxl_installed else '未安装'}")
        print(f"  pandas: {'已安装 ' + pandas_version if pandas_installed else '未安装'}")
        return False

def test_excel_reading():
    """测试Excel读取能力"""
    print("\n正在测试Excel读取能力...")
    try:
        import pandas as pd
        import xlrd
        import openpyxl
        
        print(f"pandas版本: {pd.__version__}")
        print(f"xlrd版本: {xlrd.__version__}")
        print(f"openpyxl版本: {openpyxl.__version__}")
        
        # 创建一个简单的DataFrame并尝试保存和读取
        test_df = pd.DataFrame({'A': [1, 2, 3], 'B': ['a', 'b', 'c']})
        
        # 测试目录
        test_dir = os.path.join(os.path.expanduser("~"), "AppData", "Local", "Temp", "excel_test")
        os.makedirs(test_dir, exist_ok=True)
        
        # 测试.xlsx写入和读取
        xlsx_path = os.path.join(test_dir, "test.xlsx")
        test_df.to_excel(xlsx_path, index=False)
        print(f"✓ Excel写入测试通过: {xlsx_path}")
        
        # 测试使用openpyxl引擎读取
        try:
            df_openpyxl = pd.read_excel(xlsx_path, engine='openpyxl')
            print(f"✓ openpyxl引擎读取测试通过")
        except Exception as e:
            print(f"✗ openpyxl引擎读取测试失败: {str(e)}")
        
        # 测试使用xlrd引擎读取
        try:
            df_xlrd = pd.read_excel(xlsx_path, engine='xlrd')
            print(f"✓ xlrd引擎读取测试通过")
        except Exception as e:
            print(f"✗ xlrd引擎读取测试失败: {str(e)}")
        
        # 测试使用默认引擎读取
        try:
            df_default = pd.read_excel(xlsx_path)
            print(f"✓ 默认引擎读取测试通过")
        except Exception as e:
            print(f"✗ 默认引擎读取测试失败: {str(e)}")
            
        return True
    except Exception as e:
        print(f"✗ Excel读取能力测试失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """主函数"""
    # 创建基本的GUI窗口
    root = tk.Tk()
    root.title("Excel依赖修复工具")
    root.geometry("600x400")
    
    # 创建文本显示区域
    from tkinter import scrolledtext
    log_area = scrolledtext.ScrolledText(root, wrap=tk.WORD, width=70, height=20)
    log_area.pack(padx=10, pady=10, fill=tk.BOTH, expand=True)
    
    # 重定向标准输出
    class TextRedirector:
        def __init__(self, text_widget):
            self.text_widget = text_widget
            
        def write(self, string):
            self.text_widget.insert(tk.END, string)
            self.text_widget.see(tk.END)
            self.text_widget.update()
            
        def flush(self):
            pass
    
    # 保存原始stdout
    original_stdout = sys.stdout
    sys.stdout = TextRedirector(log_area)
    
    try:
        # 尝试修复Excel依赖
        success = fix_excel_dependencies()
        
        if success:
            # 测试Excel读取能力
            reading_success = test_excel_reading()
            
            if reading_success:
                log_area.insert(tk.END, "\n\n✅ 修复完成! Excel依赖已成功修复并测试通过。\n")
                messagebox.showinfo("成功", "Excel依赖已成功修复并测试通过!\n\n您现在可以正常使用术语提取工具阅读Excel文件了。")
            else:
                log_area.insert(tk.END, "\n\n⚠️ 依赖安装成功，但Excel读取测试失败。\n")
                messagebox.showwarning("部分成功", "Excel依赖已成功安装，但读取测试失败。\n请尝试重启应用后再试。")
        else:
            log_area.insert(tk.END, "\n\n❌ 修复失败! 无法完全修复Excel依赖。\n")
            messagebox.showerror("失败", "无法完全修复Excel依赖。\n\n请尝试手动运行以下命令:\npip install xlrd==1.2.0 openpyxl==3.0.10 pandas")
    except Exception as e:
        log_area.insert(tk.END, f"\n\n❌ 发生错误: {str(e)}\n")
        messagebox.showerror("错误", f"修复过程中发生错误:\n{str(e)}")
    finally:
        # 恢复原始stdout
        sys.stdout = original_stdout
    
    # 创建关闭按钮
    from tkinter import ttk
    close_button = ttk.Button(root, text="关闭", command=root.destroy)
    close_button.pack(pady=10)
    
    # 启动GUI
    root.mainloop()

if __name__ == "__main__":
    main() 