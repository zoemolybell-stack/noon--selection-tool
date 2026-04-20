#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
类目映射配置工具 - 可视化配置 Noon 类目到 V6 品类的映射

功能:
- 显示 Noon 完整类目树
- 为每个 Noon 类目配置对应的 V6 品类
- 导出映射配置到 JSON
- 导入/更新映射配置

使用方式:
    python category_mapper_gui.py
"""
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import json
from pathlib import Path
from typing import Dict, List, Any

PROJECT_ROOT = Path(__file__).parent.parent
CATEGORY_TREE_PATH = PROJECT_ROOT / "config" / "category_tree.json"
MAPPING_CONFIG_PATH = PROJECT_ROOT / "config" / "category_mapping_config.json"

# V6 品类列表 (来自 v6_data.py)
V6_CATEGORIES = [
    "服装/鞋履 Apparel/Footwear",
    "旅行箱包 Bags:Luggage",
    "其他包袋 Bags:Other",
    "手表 Watches",
    "眼镜 Eyewear",
    "高级珠宝 Jewelry:Fine",
    "时尚饰品 Jewelry:Fashion",
    "手机/平板 Mobiles/Tablets",
    "SIM 卡 SIM Cards",
    "触控笔 Stylus Pens",
    "笔记本/台式机 Laptops",
    "内存卡 Memory Cards",
    "U 盘 USB Flash",
    "硬盘/SSD Storage",
    "网络配件 Network",
    "显示器/显卡 Monitors/GPU",
    "其他电脑硬件 PC Hardware",
    "软件 Software",
    "电脑包 Laptop Bags",
    "打印机/扫描仪 Printers",
    "其他办公电子 Office Electronics",
    "手机配件≤50 Phone Acc≤50",
    "耳机 Headphones",
    "AR/VR 眼镜",
    "智能手表/穿戴 Smartwatches",
    "相机/镜头 Cameras",
    "电视 TVs",
    "投影/流媒体 Projectors",
    "游戏主机 Consoles",
    "视频游戏 Video Games",
    "游戏配件 Gaming Acc",
    "大型家电 Large Appliances",
    "小型家电 Small Appliances",
    "香水 Fragrance",
    "彩妆 - 白牌 Makeup Generic",
    "个护 - 通用 Personal Care Generic",
    "健康营养品≤50 Health≤50",
    "健康营养品>50 Health>50",
    "厨具/床品/卫浴/装饰 Home",
    "家具 Furniture",
    "清洁卫浴 Cleaning",
    "运动户外 Sports",
    "玩具 Toys",
    "母婴 Baby Products",
    "图书 Books",
    "办公文具 Office Supplies",
    "汽配电子 Auto Electronics",
    "汽配零件 Auto Parts",
    "杂货非食品 Grocery Non-Food",
    "礼品卡 Gift Cards",
    "宠物用品 Pet Supplies",
    "其他通用 General/Other",
]


class CategoryMapperGUI:
    """类目映射配置 GUI"""

    def __init__(self, root):
        self.root = root
        self.root.title("Noon 类目映射配置工具 v1.0")
        self.root.geometry("1000x700")

        self.category_tree = {}
        self.mappings = {}

        self.load_category_tree()
        self.load_mappings()
        self.create_ui()

    def load_category_tree(self):
        """加载类目树"""
        if CATEGORY_TREE_PATH.exists():
            data = json.loads(CATEGORY_TREE_PATH.read_text(encoding="utf-8"))
            self.category_tree = data.get("categories", [])
        else:
            # 使用默认类目树
            self.category_tree = self._get_default_tree()

    def _get_default_tree(self) -> List[Dict]:
        """默认类目树（当 JSON 不存在时）"""
        return [
            {"id": "electronics", "name_en": "Electronics", "name_zh": "电子产品", "children": []},
            {"id": "fashion", "name_en": "Fashion", "name_zh": "时尚服装", "children": []},
            {"id": "beauty", "name_en": "Beauty", "name_zh": "美妆个护", "children": []},
            {"id": "home_kitchen", "name_en": "Home & Kitchen", "name_zh": "家居厨房", "children": []},
            {"id": "baby", "name_en": "Baby & Kids", "name_zh": "母婴儿童", "children": []},
            {"id": "sports", "name_en": "Sports", "name_zh": "运动健身", "children": []},
            {"id": "automotive", "name_en": "Automotive", "name_zh": "汽车摩托", "children": []},
            {"id": "grocery", "name_en": "Grocery", "name_zh": "食品杂货", "children": []},
            {"id": "pets", "name_en": "Pets", "name_zh": "宠物用品", "children": []},
            {"id": "office", "name_en": "Office", "name_zh": "办公文具", "children": []},
            {"id": "tools", "name_en": "Tools", "name_zh": "工具家装", "children": []},
            {"id": "garden", "name_en": "Garden", "name_zh": "花园户外", "children": []},
        ]

    def load_mappings(self):
        """加载已有映射配置"""
        if MAPPING_CONFIG_PATH.exists():
            self.mappings = json.loads(MAPPING_CONFIG_PATH.read_text(encoding="utf-8"))
        else:
            self.mappings = {}

    def create_ui(self):
        """创建界面"""
        main = ttk.Frame(self.root, padding="10")
        main.grid(row=0, column=0, sticky="nsew")
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)

        # 标题
        ttk.Label(main, text="Noon 类目映射配置工具", font=("Arial", 16, "bold")).grid(
            row=0, column=0, columnspan=3, pady=(0, 20)
        )

        # 左侧：类目树
        tree_frame = ttk.LabelFrame(main, text="Noon 类目树", padding="10")
        tree_frame.grid(row=1, column=0, sticky="nsew", padx=(0, 5))
        main.columnconfigure(0, weight=1)
        main.rowconfigure(1, weight=1)

        self.tree = ttk.Treeview(tree_frame, selectmode="browse")
        self.tree.pack(side="left", fill="both", expand=True)

        tree_scroll = ttk.Scrollbar(tree_frame, orient="vertical", command=self.tree.yview)
        tree_scroll.pack(side="right", fill="y")
        self.tree.configure(yscrollcommand=tree_scroll.set)

        # 填充类目树
        self._populate_tree()

        # 中间：映射配置
        map_frame = ttk.LabelFrame(main, text="V6 品类映射", padding="10")
        map_frame.grid(row=1, column=1, sticky="nsew", padx=5)
        main.columnconfigure(1, weight=1)

        ttk.Label(map_frame, text="选中类目:").grid(row=0, column=0, sticky="w")
        self.selected_cat_label = ttk.Label(map_frame, text="-", font=("Arial", 10, "bold"))
        self.selected_cat_label.grid(row=0, column=1, sticky="w", padx=10)

        ttk.Label(map_frame, text="映射到 V6 品类:").grid(row=1, column=0, sticky="w", pady=(20, 0))

        self.v6_combo = ttk.Combobox(map_frame, values=V6_CATEGORIES, state="readonly", width=40)
        self.v6_combo.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(5, 0))

        self.apply_btn = ttk.Button(map_frame, text="应用映射", command=self.apply_mapping)
        self.apply_btn.grid(row=3, column=0, columnspan=2, pady=(20, 0))

        # 右侧：已映射列表
        mapped_frame = ttk.LabelFrame(main, text="已配置映射", padding="10")
        mapped_frame.grid(row=1, column=2, sticky="nsew", padx=(5, 0))
        main.columnconfigure(2, weight=1)

        self.mapped_list = ttk.Treeview(mapped_frame, columns=("noon", "v6"), show="headings", height=20)
        self.mapped_list.heading("noon", text="Noon 类目")
        self.mapped_list.heading("v6", text="V6 品类")
        self.mapped_list.column("noon", width=150)
        self.mapped_list.column("v6", width=150)
        self.mapped_list.pack(side="left", fill="both", expand=True)

        mapped_scroll = ttk.Scrollbar(mapped_frame, orient="vertical", command=self.mapped_list.yview)
        mapped_scroll.pack(side="right", fill="y")
        self.mapped_list.configure(yscrollcommand=mapped_scroll.set)

        # 绑定选择事件
        self.tree.bind("<<TreeviewSelect>>", self.on_tree_select)

        # 底部按钮
        btn_frame = ttk.Frame(main)
        btn_frame.grid(row=2, column=0, columnspan=3, pady=(20, 0))

        ttk.Button(btn_frame, text="保存配置", command=self.save_mappings).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="导出 JSON", command=self.export_json).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="导入 JSON", command=self.import_json).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="刷新类目树", command=self.refresh_tree).pack(side="left", padx=5)

        # 状态栏
        self.status_var = tk.StringVar(value="就绪")
        ttk.Label(main, textvariable=self.status_var, relief="sunken").grid(
            row=3, column=0, columnspan=3, sticky="ew", pady=(10, 0)
        )

        # 加载已映射列表
        self._update_mapped_list()

    def _populate_tree(self):
        """填充类目树"""
        for item in self.tree.get_children():
            self.tree.delete(item)

        for cat in self.category_tree:
            cat_id = cat.get("id", cat.get("name_en", ""))
            name = cat.get("name_zh") or cat.get("name_en", "")
            node_id = self.tree.insert("", "end", text=name, values=(cat_id,))

            # 添加子类目
            for child in cat.get("children", []):
                child_id = child.get("id", child.get("name_en", ""))
                child_name = child.get("name_zh") or child.get("name_en", "")
                self.tree.insert(node_id, "end", text=child_name, values=(child_id,))

    def _update_mapped_list(self):
        """更新已映射列表"""
        for item in self.mapped_list.get_children():
            self.mapped_list.delete(item)

        for noon_cat, v6_cat in self.mappings.items():
            self.mapped_list.insert("", "end", values=(noon_cat, v6_cat))

    def on_tree_select(self, event):
        """类目树选择事件"""
        selection = self.tree.selection()
        if selection:
            item = self.tree.item(selection[0])
            values = item["values"]
            if values:
                cat_id = values[0]
                self.selected_cat_label.config(text=cat_id)

                # 设置当前映射值
                if cat_id in self.mappings:
                    self.v6_combo.set(self.mappings[cat_id])
                else:
                    self.v6_combo.set("")

    def apply_mapping(self):
        """应用映射"""
        selection = self.tree.selection()
        if not selection:
            messagebox.showwarning("警告", "请先选择要映射的类目")
            return

        item = self.tree.item(selection[0])
        values = item["values"]
        if not values:
            return

        cat_id = values[0]
        v6_cat = self.v6_combo.get()

        if not v6_cat:
            messagebox.showwarning("警告", "请选择 V6 品类")
            return

        self.mappings[cat_id] = v6_cat
        self._update_mapped_list()
        self.status_var.set(f"已映射：{cat_id} → {v6_cat}")

    def save_mappings(self):
        """保存映射配置"""
        MAPPING_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
        MAPPING_CONFIG_PATH.write_text(
            json.dumps(self.mappings, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )
        self.status_var.set("配置已保存")
        messagebox.showinfo("完成", f"映射配置已保存到:\n{MAPPING_CONFIG_PATH}")

    def export_json(self):
        """导出映射配置"""
        file_path = filedialog.asksaveasfilename(
            title="导出映射配置",
            defaultextension=".json",
            filetypes=[("JSON files", "*.json")]
        )
        if file_path:
            Path(file_path).write_text(
                json.dumps(self.mappings, ensure_ascii=False, indent=2),
                encoding="utf-8"
            )
            self.status_var.set(f"已导出：{file_path}")

    def import_json(self):
        """导入映射配置"""
        file_path = filedialog.askopenfilename(
            title="导入映射配置",
            defaultextension=".json",
            filetypes=[("JSON files", "*.json")]
        )
        if file_path:
            try:
                self.mappings = json.loads(Path(file_path).read_text(encoding="utf-8"))
                self._update_mapped_list()
                self.status_var.set(f"已导入：{file_path}")
            except Exception as e:
                messagebox.showerror("错误", f"导入失败：{e}")

    def refresh_tree(self):
        """刷新类目树"""
        self.load_category_tree()
        self._populate_tree()
        self.status_var.set("类目树已刷新")


def main():
    root = tk.Tk()
    app = CategoryMapperGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()
