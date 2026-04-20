#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
codex noon爬虫-关键词
- 多行文本输入关键词（每行 1 个）
- 支持 CSV/TXT 文件导入
- Noon/Amazon 采集数量独立配置
- 实时日志显示
- 并发爬取 (Noon+Amazon 并行)
"""
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import subprocess
from pathlib import Path
import threading
import json

PROJECT_ROOT = Path(__file__).parent
NOON_TOOL_DIR = PROJECT_ROOT / 'noon-selection-tool'
VENV_PYTHON = PROJECT_ROOT / 'venv' / 'Scripts' / 'python.exe'


class KeywordCrawlerSelector:
    def __init__(self, root):
        self.root = root
        self.root.title('codex noon爬虫-关键词')
        self.root.geometry('1000x700')

        self.current_process = None
        self.is_crawling = False

        self.create_ui()

    def create_ui(self):
        main = ttk.Frame(self.root, padding='10')
        main.grid(row=0, column=0, sticky='nsew')
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)

        # 标题
        ttk.Label(main, text='codex noon爬虫-关键词', font=('Arial', 16, 'bold')).grid(row=0, column=0, pady=(0, 20))

        # 左侧：关键词输入
        kw_frame = ttk.LabelFrame(main, text='输入关键词 (每行一个)', padding='10')
        kw_frame.grid(row=1, column=0, sticky='nsew', pady=(0, 10))
        main.columnconfigure(0, weight=2)
        main.rowconfigure(1, weight=1)

        self.kw_text = tk.Text(kw_frame, height=20, width=40)
        self.kw_text.pack(side='left', fill='both', expand=True)

        kw_scroll = ttk.Scrollbar(kw_frame, orient='vertical', command=self.kw_text.yview)
        kw_scroll.pack(side='right', fill='y')
        self.kw_text.configure(yscrollcommand=kw_scroll.set)

        # 右侧：参数和日志
        right_frame = ttk.Frame(main)
        right_frame.grid(row=1, column=1, sticky='nsew', padx=(10, 0))
        main.columnconfigure(1, weight=1)

        # 参数区域
        parf = ttk.LabelFrame(right_frame, text='爬取参数', padding='10')
        parf.pack(fill='x', pady=(0, 10))

        ttk.Label(parf, text='Noon 每关键词数量:').grid(row=0, column=0, sticky='w')
        self.noon_count = tk.StringVar(value='100')
        ttk.Entry(parf, textvariable=self.noon_count, width=10).grid(row=0, column=1, padx=10)

        ttk.Label(parf, text='Amazon 每关键词数量:').grid(row=1, column=0, sticky='w', pady=(10, 0))
        self.amazon_count = tk.StringVar(value='100')
        ttk.Entry(parf, textvariable=self.amazon_count, width=10).grid(row=1, column=1, padx=10, pady=(10, 0))

        ttk.Label(parf, text='输出目录:').grid(row=2, column=0, sticky='w', pady=(10, 0))
        self.out_dir = tk.StringVar(value=str(NOON_TOOL_DIR / 'runtime_data' / 'keyword'))
        ttk.Entry(parf, textvariable=self.out_dir, width=35).grid(row=2, column=1, columnspan=2, padx=10, pady=(10, 0))
        ttk.Button(parf, text='浏览', command=self.browse_output).grid(row=2, column=3, pady=(10, 0))

        # 平台选择
        self.crawl_noon = tk.BooleanVar(value=True)
        ttk.Checkbutton(parf, text='爬取 Noon', variable=self.crawl_noon).grid(
            row=3, column=0, columnspan=2, sticky='w', pady=(10, 0))
        self.crawl_amazon = tk.BooleanVar(value=True)
        ttk.Checkbutton(parf, text='爬取 Amazon', variable=self.crawl_amazon).grid(
            row=3, column=2, columnspan=2, sticky='w', pady=(10, 0))

        # 已选显示
        self.sel_label = ttk.Label(parf, text='关键词数：0 个', font=('Arial', 10, 'bold'))
        self.sel_label.grid(row=4, column=0, columnspan=4, pady=(10, 0), sticky='w')

        # 导出 Excel 按钮
        self.export_btn = ttk.Button(parf, text='导出 Excel', command=self.export_to_excel, state='disabled')
        self.export_btn.grid(row=5, column=0, columnspan=4, sticky='w', pady=(5, 0))

        # 导入/导出关键词按钮
        btnf = ttk.Frame(parf)
        btnf.grid(row=6, column=0, columnspan=4, pady=(5, 0), sticky='w')
        ttk.Button(btnf, text='从文件导入', command=self.import_keywords).pack(side='left', padx=2)
        ttk.Button(btnf, text='清空', command=self.clear_keywords).pack(side='left', padx=2)
        ttk.Button(btnf, text='加载示例', command=self.load_example_keywords).pack(side='left', padx=2)

        # 日志区域
        logf = ttk.LabelFrame(right_frame, text='运行日志', padding='10')
        logf.pack(fill='both', expand=True)

        self.log_text = tk.Text(logf, height=12, width=50, state='disabled')
        self.log_text.pack(side='left', fill='both', expand=True)

        scb = ttk.Scrollbar(logf, orient='vertical', command=self.log_text.yview)
        scb.pack(side='right', fill='y')
        self.log_text.configure(yscrollcommand=scb.set)

        # 控制按钮
        ctrlf = ttk.Frame(main)
        ctrlf.grid(row=2, column=0, columnspan=2, pady=(10, 0))

        self.start_btn = ttk.Button(ctrlf, text='开始爬取', command=self.start_crawling)
        self.start_btn.pack(side='left', padx=5)

        self.stop_btn = ttk.Button(ctrlf, text='停止', command=self.stop_crawling, state='disabled')
        self.stop_btn.pack(side='left', padx=5)

        self.status_var = tk.StringVar(value='就绪')
        ttk.Label(main, textvariable=self.status_var, relief='sunken').grid(row=3, column=0, columnspan=2, sticky='ew')

        # 绑定关键词变化事件
        self.kw_text.bind('<KeyRelease>', self.update_count)

    def update_count(self, event=None):
        """更新关键词计数"""
        keywords = self.get_keywords()
        self.sel_label.configure(text=f'关键词数：{len(keywords)} 个')

    def get_keywords(self):
        """获取关键词列表"""
        text = self.kw_text.get('1.0', 'end')
        return [line.strip() for line in text.strip().split('\n') if line.strip()]

    def browse_output(self):
        """浏览输出目录"""
        d = filedialog.askdirectory(title='选择输出目录')
        if d:
            self.out_dir.set(d)

    def log(self, msg):
        """添加日志"""
        self.log_text.configure(state='normal')
        self.log_text.insert('end', msg + '\n')
        self.log_text.see('end')
        self.log_text.configure(state='disabled')

    def import_keywords(self):
        """从文件导入关键词"""
        filetypes = [('文本文件', '*.txt'), ('CSV 文件', '*.csv'), ('所有文件', '*.*')]
        f = filedialog.askopenfilename(title='导入关键词', filetypes=filetypes)
        if f:
            try:
                content = Path(f).read_text(encoding='utf-8')
                # 如果是 CSV，读取第一列
                if f.endswith('.csv'):
                    import csv
                    with open(f, 'r', encoding='utf-8') as csvfile:
                        reader = csv.reader(csvfile)
                        keywords = [row[0].strip() for row in reader if row and row[0].strip()]
                else:
                    keywords = [line.strip() for line in content.split('\n') if line.strip()]

                # 追加到现有关键词
                current = self.kw_text.get('1.0', 'end').strip()
                if current:
                    self.kw_text.insert('end', '\n' + '\n'.join(keywords))
                else:
                    self.kw_text.insert('1.0', '\n'.join(keywords))
                self.update_count()
                self.log(f'已导入 {len(keywords)} 个关键词')
            except Exception as e:
                messagebox.showerror('错误', f'导入失败：{e}')

    def clear_keywords(self):
        """清空关键词"""
        self.kw_text.delete('1.0', 'end')
        self.update_count()

    def load_example_keywords(self):
        """加载示例关键词"""
        examples = [
            'wireless earbuds',
            'phone case',
            'car charger',
            'bluetooth speaker',
            'smart watch',
            'laptop stand',
            'usb cable',
            'power bank',
            'screen protector',
            'gaming mouse',
        ]
        self.kw_text.insert('end', '\n'.join(examples))
        self.update_count()

    def start_crawling(self):
        """开始爬取"""
        keywords = self.get_keywords()
        if not keywords:
            messagebox.showwarning('警告', '请至少输入一个关键词')
            return

        if not self.crawl_noon.get() and not self.crawl_amazon.get():
            messagebox.showwarning('警告', '请至少选择一个平台')
            return

        try:
            noon_cnt = int(self.noon_count.get())
            if noon_cnt < 10 or noon_cnt > 1000:
                raise ValueError('Noon 数量必须在 10-1000 之间')
            amazon_cnt = int(self.amazon_count.get())
            if amazon_cnt < 10 or amazon_cnt > 1000:
                raise ValueError('Amazon 数量必须在 10-1000 之间')
        except ValueError as e:
            messagebox.showerror('错误', str(e))
            return

        if not messagebox.askyesno('确认',
                f'即将爬取 {len(keywords)} 个关键词\n'
                f'Noon: {noon_cnt} 条/词\n'
                f'Amazon: {amazon_cnt} 条/词\n\n确定？'):
            return

        self.start_btn.configure(state='disabled')
        self.stop_btn.configure(state='normal')
        self.is_crawling = True
        self.status_var.set('正在启动...')
        self.log('=' * 60)
        self.log(f'开始爬取 {len(keywords)} 个关键词')
        self.log(f'Noon: {noon_cnt}, Amazon: {amazon_cnt}')
        self.log('=' * 60)

        selected_platforms = []
        if self.crawl_noon.get():
            selected_platforms.append('noon')
        if self.crawl_amazon.get():
            selected_platforms.append('amazon')

        # 构建命令
        cmd = [
            str(VENV_PYTHON),
            str(NOON_TOOL_DIR / 'keyword_main.py'),
            '--step', 'scrape',
            '--noon-count', str(noon_cnt),
            '--amazon-count', str(amazon_cnt),
            '--persist',
            '--verbose',
            '--data-root', self.out_dir.get()
        ]
        if selected_platforms:
            cmd.extend(['--platforms', *selected_platforms])

        # 替换关键词文件
        kw_json = Path(self.out_dir.get()) / 'processed' / 'keywords.json'
        kw_json.parent.mkdir(parents=True, exist_ok=True)

        kw_json.write_text(json.dumps(keywords, ensure_ascii=False, indent=2), encoding='utf-8')
        self.log(f'关键词已保存：{kw_json}')
        cmd.extend(['--keywords-file', str(kw_json), '--tracking-mode', 'adhoc'])

        self.log(f'执行：{" ".join(cmd)}')

        try:
            p = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding='utf-8',
                errors='replace'
            )
            self.current_process = p

            def read_output():
                for line in p.stdout:
                    if line.strip():
                        self.root.after(0, lambda msg=line.strip(): self.log(msg))
                return p.wait()

            def on_done(returncode):
                self.log(f'完成!' if returncode == 0 else f'失败!')
                self.is_crawling = False
                if returncode == 0:
                    self.export_btn.configure(state='normal')
                self.start_btn.configure(state='normal')
                self.stop_btn.configure(state='disabled')

            threading.Thread(target=lambda: on_done(read_output()), daemon=True).start()

        except Exception as e:
            self.log(f'FAIL: {e}')
            self.is_crawling = False
            self.start_btn.configure(state='normal')
            self.stop_btn.configure(state='disabled')

    def stop_crawling(self):
        """停止爬取"""
        if messagebox.askyesno('确认', '确定停止？'):
            if self.current_process:
                self.current_process.kill()
                self.current_process = None
            self.log('用户取消')
            self.status_var.set('已取消')
            self.start_btn.configure(state='normal')
            self.stop_btn.configure(state='disabled')
            self.is_crawling = False

    def export_to_excel(self):
        """导出爬取数据为 Excel"""
        from output.crawl_data_exporter import export_crawl_data
        import subprocess

        # 获取快照目录
        data_dir = Path(self.out_dir.get())
        if not data_dir.exists():
            messagebox.showerror('错误', '数据目录不存在')
            return

        # 查找最新的 snapshot
        snap_dir = data_dir / 'snapshots'
        latest_dir = None

        if snap_dir.exists():
            subdirs = [d for d in snap_dir.iterdir() if d.is_dir() and (d / 'noon').exists()]
            if subdirs:
                latest_dir = max(subdirs, key=lambda d: d.stat().st_mtime)

        if not latest_dir:
            # 尝试旧路径
            if list(data_dir.glob('noon/*.json')):
                latest_dir = data_dir
            else:
                messagebox.showerror('错误', '没有找到爬取数据')
                return

        self.log(f'导出最新数据：{latest_dir}')

        try:
            noon_dir = latest_dir / 'noon'
            amazon_dir = latest_dir / 'amazon'

            dirs = []
            platforms = []
            if noon_dir.exists() and list(noon_dir.glob('*.json')):
                dirs.append(noon_dir)
                platforms.append('noon')
            if amazon_dir.exists() and list(amazon_dir.glob('*.json')):
                dirs.append(amazon_dir)
                platforms.append('amazon')

            if not dirs:
                messagebox.showerror('错误', '没有找到有效数据')
                return

            output_path = data_dir / 'exports' / f'{latest_dir.name}_products.xlsx'
            output_path.parent.mkdir(parents=True, exist_ok=True)
            export_crawl_data(dirs, output_path, platforms)
            self.log(f'Excel 已导出：{output_path}')

            # 用默认程序打开
            subprocess.run(['start', str(output_path)], shell=True)
        except Exception as e:
            self.log(f'导出失败：{e}')
            messagebox.showerror('错误', f'导出失败：{e}')


def main():
    root = tk.Tk()
    KeywordCrawlerSelector(root)
    root.mainloop()


if __name__ == '__main__':
    main()
