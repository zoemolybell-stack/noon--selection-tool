#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
产品趋势查看器 - 本地 SQLite 版
- 产品搜索 (product_id/标题)
- 价格历史曲线图
- 排名历史曲线图
- 导出 CSV/Excel
"""
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from pathlib import Path
import sqlite3
from datetime import datetime
import threading

# 图表库 (可选，如无安装则用文本显示)
try:
    import matplotlib.pyplot as plt
    from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False

# Excel 导出支持
try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

PROJECT_ROOT = Path(__file__).parent
DB_PATH = PROJECT_ROOT / 'noon-selection-tool' / 'data' / 'product_store.db'
LEGACY_DB_PATH = PROJECT_ROOT / 'product_store.db'


class ProductTrendViewer:
    def __init__(self, root):
        self.root = root
        self.root.title('NOON 产品趋势查看器 v1.0')
        self.root.geometry('1100x750')

        self.conn = None
        self.current_product = None
        self.search_results = []

        self._init_db()
        self.create_ui()

    def _init_db(self):
        """初始化 SQLite 数据库（创建表如果不存在）"""
        if not DB_PATH.exists() and LEGACY_DB_PATH.exists():
            db_path = LEGACY_DB_PATH
        else:
            DB_PATH.parent.mkdir(parents=True, exist_ok=True)
            db_path = DB_PATH

        self.conn = sqlite3.connect(str(db_path))
        self.conn.row_factory = sqlite3.Row
        cursor = self.conn.cursor()

        # 产品表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS products (
                product_id TEXT PRIMARY KEY,
                platform TEXT DEFAULT 'noon',
                title TEXT,
                brand TEXT,
                category_path TEXT,
                product_url TEXT,
                first_seen TEXT,
                last_seen TEXT,
                is_active INTEGER DEFAULT 1
            )
        ''')

        # 价格历史表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS price_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id TEXT,
                scraped_at TEXT,
                price REAL,
                original_price REAL,
                FOREIGN KEY (product_id) REFERENCES products(product_id)
            )
        ''')

        # 排名历史表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS rank_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id TEXT,
                keyword TEXT,
                search_rank INTEGER,
                bsr_rank INTEGER,
                scraped_at TEXT,
                FOREIGN KEY (product_id) REFERENCES products(product_id)
            )
        ''')

        # 销量快照表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sales_snapshot (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id TEXT,
                scraped_at TEXT,
                units_sold INTEGER,
                revenue REAL,
                review_count INTEGER,
                rating REAL,
                FOREIGN KEY (product_id) REFERENCES products(product_id)
            )
        ''')

        self.conn.commit()

    def create_ui(self):
        main = ttk.Frame(self.root, padding='10')
        main.grid(row=0, column=0, sticky='nsew')
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)

        # 标题
        ttk.Label(main, text='NOON 产品趋势查看器', font=('Arial', 16, 'bold')).grid(row=0, column=0, pady=(0, 15))

        # 搜索区
        search_frame = ttk.LabelFrame(main, text='产品搜索', padding='10')
        search_frame.grid(row=1, column=0, sticky='ew', pady=(0, 10))
        search_frame.columnconfigure(1, weight=1)

        ttk.Label(search_frame, text='关键词:').grid(row=0, column=0, sticky='w')
        self.search_entry = ttk.Entry(search_frame, width=60)
        self.search_entry.grid(row=0, column=1, padx=10, sticky='ew')
        self.search_entry.bind('<Return>', lambda e: self.search_products())

        self.search_btn = ttk.Button(search_frame, text='搜索', command=self.search_products)
        self.search_btn.grid(row=0, column=2, padx=5)

        ttk.Label(search_frame, text='(支持 product_id 或标题关键词)').grid(row=1, column=0, columnspan=3, sticky='w', pady=(5, 0))

        # 结果列表区
        result_frame = ttk.LabelFrame(main, text='搜索结果', padding='10')
        result_frame.grid(row=2, column=0, sticky='nsew', pady=(0, 10))
        main.rowconfigure(2, weight=1)
        result_frame.columnconfigure(0, weight=1)
        result_frame.rowconfigure(0, weight=1)

        # Treeview
        cols = ('product_id', 'platform', 'title', 'brand', 'last_price', 'last_bsr', 'last_seen')
        self.result_tree = ttk.Treeview(result_frame, columns=cols, show='headings', height=8)

        self.result_tree.heading('product_id', text='产品 ID')
        self.result_tree.column('product_id', width=120)
        self.result_tree.heading('platform', text='平台')
        self.result_tree.column('platform', width=60)
        self.result_tree.heading('title', text='标题')
        self.result_tree.column('title', width=400)
        self.result_tree.heading('brand', text='品牌')
        self.result_tree.column('brand', width=100)
        self.result_tree.heading('last_price', text='最新价格')
        self.result_tree.column('last_price', width=80)
        self.result_tree.heading('last_bsr', text='最新 BSR')
        self.result_tree.column('last_bsr', width=80)
        self.result_tree.heading('last_seen', text='最后更新')
        self.result_tree.column('last_seen', width=120)

        # 滚动条
        y_scroll = ttk.Scrollbar(result_frame, orient='vertical', command=self.result_tree.yview)
        self.result_tree.configure(yscrollcommand=y_scroll.set)

        self.result_tree.grid(row=0, column=0, sticky='nsew')
        y_scroll.grid(row=0, column=1, sticky='ns')

        self.result_tree.bind('<<TreeviewSelect>>', self.on_product_selected)

        # 详情区（带图表）
        detail_frame = ttk.LabelFrame(main, text='产品详情与趋势', padding='10')
        detail_frame.grid(row=3, column=0, sticky='nsew', pady=(0, 10))
        main.rowconfigure(3, weight=2)
        detail_frame.columnconfigure(0, weight=1)
        detail_frame.rowconfigure(1, weight=1)

        # 产品基本信息
        info_frame = ttk.Frame(detail_frame)
        info_frame.grid(row=0, column=0, sticky='ew', pady=(0, 10))
        info_frame.columnconfigure(1, weight=1)
        info_frame.columnconfigure(3, weight=1)

        self.lbl_title = ttk.Label(info_frame, text='标题：-', wraplength=400)
        self.lbl_title.grid(row=0, column=0, columnspan=4, sticky='w', pady=(0, 5))

        self.lbl_product_id = ttk.Label(info_frame, text='产品 ID: -')
        self.lbl_product_id.grid(row=1, column=0, sticky='w')
        self.lbl_platform = ttk.Label(info_frame, text='平台：-')
        self.lbl_platform.grid(row=1, column=1, sticky='w')
        self.lbl_brand = ttk.Label(info_frame, text='品牌：-')
        self.lbl_brand.grid(row=1, column=2, sticky='w')
        self.lbl_url = ttk.Label(info_frame, text='URL: -', foreground='blue', cursor='hand2')
        self.lbl_url.grid(row=1, column=3, sticky='w')
        self.lbl_url.bind('<Button-1>', lambda e: self._open_url())

        # 图表区
        self.chart_frame = ttk.Frame(detail_frame)
        self.chart_frame.grid(row=1, column=0, sticky='nsew')

        if HAS_MATPLOTLIB:
            self.figure, (self.ax1, self.ax2) = plt.subplots(2, 1, figsize=(10, 5), dpi=80)
            self.canvas = FigureCanvasTkAgg(self.figure, master=self.chart_frame)
            self.canvas.get_tk_widget().pack(fill='both', expand=True)
            self._clear_charts()
        else:
            ttk.Label(self.chart_frame, text='[图表区] 安装 matplotlib 可显示趋势图',
                     foreground='gray').pack(pady=20)
            self.chart_text = tk.Text(self.chart_frame, height=8, state='disabled')
            self.chart_text.pack(fill='both', expand=True, padx=10, pady=10)

        # 导出区
        export_frame = ttk.Frame(detail_frame)
        export_frame.grid(row=2, column=0, sticky='ew', pady=(10, 0))

        self.export_csv_btn = ttk.Button(export_frame, text='导出 CSV', command=self.export_csv, state='disabled')
        self.export_csv_btn.pack(side='left', padx=5)

        if HAS_PANDAS:
            self.export_excel_btn = ttk.Button(export_frame, text='导出 Excel', command=self.export_excel, state='disabled')
            self.export_excel_btn.pack(side='left', padx=5)

        # 状态栏
        self.status_var = tk.StringVar(value='就绪 - 请输入关键词搜索产品')
        ttk.Label(main, textvariable=self.status_var, relief='sunken').grid(row=4, column=0, sticky='ew')

    def _clear_charts(self):
        """清空图表"""
        if not HAS_MATPLOTLIB:
            return
        self.ax1.clear()
        self.ax2.clear()
        self.ax1.set_title('价格历史')
        self.ax1.set_xlabel('日期')
        self.ax1.set_ylabel('价格')
        self.ax2.set_title('BSR 排名历史')
        self.ax2.set_xlabel('日期')
        self.ax2.set_ylabel('排名')
        self.figure.tight_layout()

    def _open_url(self):
        """打开产品链接"""
        if self.current_product:
            import webbrowser
            webbrowser.open(self.current_product['product_url'])

    def search_products(self):
        """搜索产品"""
        keyword = self.search_entry.get().strip()
        if not keyword:
            messagebox.showwarning('警告', '请输入搜索关键词')
            return

        self.status_var.set(f'搜索：{keyword}...')

        # 后台线程执行查询
        thread = threading.Thread(target=self._do_search, args=(keyword,))
        thread.daemon = True
        thread.start()

    def _do_search(self, keyword):
        """后台执行搜索"""
        cursor = self.conn.cursor()

        # 搜索产品（匹配 product_id 或标题）
        query = '''
            SELECT
                p.product_id, p.platform, p.title, p.brand, p.product_url,
                p.first_seen, p.last_seen, p.is_active,
                (SELECT price FROM price_history ph WHERE ph.product_id = p.product_id ORDER BY ph.scraped_at DESC LIMIT 1) as last_price,
                (SELECT bsr_rank FROM rank_history rh WHERE rh.product_id = p.product_id AND rh.bsr_rank IS NOT NULL ORDER BY rh.scraped_at DESC LIMIT 1) as last_bsr,
                (SELECT COUNT(*) FROM price_history ph WHERE ph.product_id = p.product_id) as price_records,
                (SELECT COUNT(*) FROM rank_history rh WHERE rh.product_id = p.product_id) as rank_records
            FROM products p
            WHERE p.product_id LIKE ? OR p.title LIKE ?
            ORDER BY p.last_seen DESC
            LIMIT 100
        '''

        search_pattern = f'%{keyword}%'
        cursor.execute(query, (search_pattern, search_pattern))
        self.search_results = [dict(row) for row in cursor.fetchall()]

        # 更新 UI
        self.root.after(0, self._update_search_results)

    def _update_search_results(self):
        """更新搜索结果列表"""
        # 清空现有项
        for item in self.result_tree.get_children():
            self.result_tree.delete(item)

        # 填充结果
        for prod in self.search_results:
            self.result_tree.insert('', 'end', values=(
                prod['product_id'],
                prod['platform'],
                prod['title'][:50] + '...' if len(prod['title']) > 50 else prod['title'],
                prod['brand'] or '-',
                f"{prod['last_price']:.2f}" if prod['last_price'] else 'N/A',
                prod['last_bsr'] if prod['last_bsr'] else 'N/A',
                prod['last_seen'] or '-'
            ))

        count = len(self.search_results)
        self.status_var.set(f'找到 {count} 个产品')
        if count == 0:
            messagebox.showinfo('提示', '未找到匹配的产品')

    def on_product_selected(self, event):
        """产品被选中时加载详情"""
        selection = self.result_tree.selection()
        if not selection:
            return

        item = self.result_tree.item(selection[0])
        product_id = item['values'][0]

        # 查找对应产品
        for prod in self.search_results:
            if prod['product_id'] == product_id:
                self.current_product = prod
                self._load_product_detail(prod)
                self._load_charts(prod)

                # 启用导出按钮
                self.export_csv_btn.configure(state='normal')
                if HAS_PANDAS:
                    self.export_excel_btn.configure(state='normal')
                break

    def _load_product_detail(self, prod):
        """加载产品详情"""
        self.lbl_title.configure(text=f'标题：{prod["title"]}')
        self.lbl_product_id.configure(text=f'产品 ID: {prod["product_id"]}')
        self.lbl_platform.configure(text=f'平台：{prod["platform"]}')
        self.lbl_brand.configure(text=f'品牌：{prod["brand"] or "-"}')
        self.lbl_url.configure(text=f'URL: {prod["product_url"][:50]}...')

    def _load_charts(self, prod):
        """加载趋势图表"""
        cursor = self.conn.cursor()

        # 价格历史
        cursor.execute('''
            SELECT scraped_at, price, original_price
            FROM price_history
            WHERE product_id = ?
            ORDER BY scraped_at ASC
        ''', (prod['product_id'],))
        price_data = [dict(row) for row in cursor.fetchall()]

        # 排名历史
        cursor.execute('''
            SELECT scraped_at, search_rank, bsr_rank, keyword
            FROM rank_history
            WHERE product_id = ?
            ORDER BY scraped_at ASC
        ''', (prod['product_id'],))
        rank_data = [dict(row) for row in cursor.fetchall()]

        if HAS_MATPLOTLIB:
            self._draw_matplotlib_charts(price_data, rank_data)
        else:
            self._draw_text_charts(price_data, rank_data)

    def _draw_matplotlib_charts(self, price_data, rank_data):
        """绘制 matplotlib 图表"""
        self._clear_charts()

        # 价格图
        if price_data:
            dates = [d['scraped_at'][:16] for d in price_data]
            prices = [d['price'] for d in price_data]
            self.ax1.plot(dates, prices, 'b-o', label='价格', markersize=3)
            self.ax1.legend(loc='upper left')
            self.ax1.tick_params(axis='x', rotation=45)

        # 排名图（BSR 越低越好，所以用倒序）
        bsr_data = [(d['scraped_at'], d['bsr_rank']) for d in rank_data if d['bsr_rank']]
        if bsr_data:
            dates = [d[0][:16] for d in bsr_data]
            ranks = [d[1] for d in bsr_data]
            self.ax2.plot(dates, ranks, 'g-o', label='BSR', markersize=3)
            self.ax2.invert_yaxis()  # BSR 排名 inverted（越小越好）
            self.ax2.legend(loc='upper left')
            self.ax2.tick_params(axis='x', rotation=45)

        search_rank_data = [(d['scraped_at'], d['search_rank'], d['keyword'])
                           for d in rank_data if d['search_rank']]
        if search_rank_data:
            # 如果有多个关键词，分别绘制
            keywords = set(d[2] for d in search_rank_data if d[2])
            for kw in keywords:
                kw_dates = [d[0][:16] for d in search_rank_data if d[2] == kw]
                kw_ranks = [d[1] for d in search_rank_data if d[2] == kw]
                self.ax2.plot(kw_dates, kw_ranks, 'r--s', label=f'Search: {kw}', markersize=3, alpha=0.7)

        self.ax2.legend(loc='upper left')
        self.figure.tight_layout()
        self.canvas.draw()

    def _draw_text_charts(self, price_data, rank_data):
        """文本方式显示趋势（无 matplotlib 时）"""
        self.chart_text.configure(state='normal')
        self.chart_text.delete('1.0', 'end')

        self.chart_text.insert('end', '【价格历史】\n')
        if price_data:
            for d in price_data[-10:]:  # 显示最近 10 条
                self.chart_text.insert('end', f"  {d['scraped_at'][:16]}: ¥{d['price']:.2f}")
                if d['original_price']:
                    self.chart_text.insert('end', f" (原价：{d['original_price']:.2f})")
                self.chart_text.insert('end', '\n')
        else:
            self.chart_text.insert('end', '  暂无价格数据\n')

        self.chart_text.insert('end', '\n【排名历史】\n')
        bsr_data = [d for d in rank_data if d['bsr_rank']]
        if bsr_data:
            for d in bsr_data[-10:]:
                self.chart_text.insert('end', f"  {d['scraped_at'][:16]}: BSR={d['bsr_rank']}")
                if d['keyword']:
                    self.chart_text.insert('end', f" (关键词：{d['keyword']})")
                self.chart_text.insert('end', '\n')
        else:
            self.chart_text.insert('end', '  暂无排名数据\n')

        self.chart_text.configure(state='disabled')

    def export_csv(self):
        """导出 CSV"""
        if not self.current_product:
            return

        filepath = filedialog.asksaveasfilename(
            title='导出 CSV',
            defaultextension='.csv',
            filetypes=[('CSV 文件', '*.csv'), ('所有文件', '*.*')]
        )
        if not filepath:
            return

        cursor = self.conn.cursor()

        try:
            # 导出价格历史
            cursor.execute('''
                SELECT scraped_at, price, original_price
                FROM price_history
                WHERE product_id = ?
                ORDER BY scraped_at DESC
            ''', (self.current_product['product_id'],))

            with open(filepath, 'w', encoding='utf-8') as f:
                f.write('日期，价格，原价\n')
                for row in cursor.fetchall():
                    f.write(f'{row[0]},{row[1]},{row[2] or ""}\n')

            messagebox.showinfo('成功', f'已导出到：{filepath}')
        except Exception as e:
            messagebox.showerror('错误', f'导出失败：{str(e)}')

    def export_excel(self):
        """导出 Excel（包含多个 sheet）"""
        if not self.current_product or not HAS_PANDAS:
            return

        filepath = filedialog.asksaveasfilename(
            title='导出 Excel',
            defaultextension='.xlsx',
            filetypes=[('Excel 文件', '*.xlsx'), ('所有文件', '*.*')]
        )
        if not filepath:
            return

        try:
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                # 产品基本信息
                prod_df = pd.DataFrame([self.current_product])
                prod_df.to_excel(writer, sheet_name='产品信息', index=False)

                # 价格历史
                cursor = self.conn.cursor()
                cursor.execute('''
                    SELECT scraped_at, price, original_price
                    FROM price_history
                    WHERE product_id = ?
                    ORDER BY scraped_at DESC
                ''', (self.current_product['product_id'],))
                price_df = pd.DataFrame([dict(row) for row in cursor.fetchall()])
                price_df.to_excel(writer, sheet_name='价格历史', index=False)

                # 排名历史
                cursor.execute('''
                    SELECT scraped_at, keyword, search_rank, bsr_rank
                    FROM rank_history
                    WHERE product_id = ?
                    ORDER BY scraped_at DESC
                ''', (self.current_product['product_id'],))
                rank_df = pd.DataFrame([dict(row) for row in cursor.fetchall()])
                rank_df.to_excel(writer, sheet_name='排名历史', index=False)

            messagebox.showinfo('成功', f'已导出到：{filepath}')
        except Exception as e:
            messagebox.showerror('错误', f'导出失败：{str(e)}')

    def __del__(self):
        if self.conn:
            self.conn.close()


if __name__ == '__main__':
    root = tk.Tk()
    app = ProductTrendViewer(root)
    root.mainloop()
