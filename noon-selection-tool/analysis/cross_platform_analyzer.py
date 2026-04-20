"""
跨平台交叉分析器 - Noon vs Amazon 数据对比

分析维度:
1. 价格差机会：Amazon 价格 > Noon 价格 × 1.3 → 高溢价空间
2. 供给缺口：Amazon BSR<1000 且 Noon 无货/少货 → 蓝海机会
3. 广告密度：Noon 广告占比 > 30% → 竞争激烈预警
4. Listing 质量：Noon 平均图片数 < 3 → 优化机会
5. 评论壁垒：Amazon 头部评论数 > 1000 → 进入难度大

输入:
- noon_products.json / Excel Products_Raw sheet
- amazon_products.json / Excel Products_Raw sheet

输出:
- cross_analysis_report.json
- cross_analysis_report.xlsx (5 个分析 sheet)
"""
import json
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
from collections import defaultdict

import pandas as pd

logger = logging.getLogger(__name__)


class CrossPlatformAnalyzer:
    """跨平台交叉分析器"""

    def __init__(self, noon_data: List[Dict], amazon_data: List[Dict]):
        """
        Args:
            noon_data: Noon 产品列表
            amazon_data: Amazon 产品列表
        """
        self.noon_df = pd.DataFrame(noon_data)
        self.amazon_df = pd.DataFrame(amazon_data)
        self.analysis_results = {}

    @classmethod
    def load_from_json(cls, noon_json: Path, amazon_json: Path) -> 'CrossPlatformAnalyzer':
        """从 JSON 文件加载数据"""
        noon_data = []
        amazon_data = []

        # 加载 Noon 数据 (支持单文件或目录)
        if noon_json.is_file():
            data = json.loads(noon_json.read_text(encoding='utf-8'))
            if isinstance(data, list):
                noon_data = data
            elif 'products' in data:
                noon_data = data['products']
        elif noon_json.is_dir():
            for f in noon_json.glob('*.json'):
                try:
                    data = json.loads(f.read_text(encoding='utf-8'))
                    if isinstance(data, list):
                        noon_data.extend(data)
                    elif 'products' in data:
                        noon_data.extend(data['products'])
                except Exception as e:
                    logger.warning(f"加载 {f} 失败：{e}")

        # 加载 Amazon 数据
        if amazon_json.is_file():
            data = json.loads(amazon_json.read_text(encoding='utf-8'))
            if isinstance(data, list):
                amazon_data = data
            elif 'products' in data:
                amazon_data = data['products']
        elif amazon_json.is_dir():
            for f in amazon_json.glob('*.json'):
                try:
                    data = json.loads(f.read_text(encoding='utf-8'))
                    if isinstance(data, list):
                        amazon_data.extend(data)
                    elif 'products' in data:
                        amazon_data.extend(data['products'])
                except Exception as e:
                    logger.warning(f"加载 {f} 失败：{e}")

        logger.info(f"加载 Noon 产品：{len(noon_data)} 个")
        logger.info(f"加载 Amazon 产品：{len(amazon_data)} 个")

        return cls(noon_data, amazon_data)

    @classmethod
    def load_from_excel(cls, excel_path: Path) -> 'CrossPlatformAnalyzer':
        """从 Excel 文件加载数据 (Products_Raw sheet)"""
        df = pd.read_excel(excel_path, sheet_name='Products_Raw')

        noon_df = df[df['platform'] == 'noon'].copy()
        amazon_df = df[df['platform'] == 'amazon'].copy()

        logger.info(f"加载 Noon 产品：{len(noon_df)} 个")
        logger.info(f"加载 Amazon 产品：{len(amazon_df)} 个")

        return cls(noon_df.to_dict('records'), amazon_df.to_dict('records'))

    def analyze_price_gap(self, threshold: float = 1.3) -> Dict:
        """
        分析维度 1: 价格差机会

        逻辑:
        - 按 title 相似度匹配 Noon 和 Amazon 产品
        - Amazon 价格 > Noon 价格 × threshold → 高溢价空间
        - Noon 价格 > Amazon 价格 × threshold → 反向机会

        Args:
            threshold: 价格差阈值，默认 1.3 (30% 溢价)

        Returns:
            价格差分析结果
        """
        # 简化匹配：按标题关键词匹配 (生产环境可用 fuzzy matching)
        opportunities = []

        # 按价格区间分组统计
        price_gap_summary = {
            'high_premium': [],  # Amazon > Noon × threshold
            'reverse_opportunity': [],  # Noon > Amazon × threshold
            'similar_price': [],  # 价格相近
        }

        # 构建 Amazon 标题索引
        amazon_index = defaultdict(list)
        for _, row in self.amazon_df.iterrows():
            title = row.get('title', '').lower()
            # 提取核心词 (前 3 个单词)
            core_words = ' '.join(title.split()[:3])
            amazon_index[core_words].append(row)

        # 遍历 Noon 产品
        for _, noon_row in self.noon_df.iterrows():
            noon_title = noon_row.get('title', '').lower()
            noon_price = noon_row.get('price', 0) or 0
            noon_core = ' '.join(noon_title.split()[:3])

            # 查找匹配的 Amazon 产品
            matches = amazon_index.get(noon_core, [])

            for amazon_row in matches:
                amazon_price = amazon_row.get('price', 0) or 0

                if noon_price <= 0 or amazon_price <= 0:
                    continue

                ratio = amazon_price / noon_price

                if ratio >= threshold:
                    price_gap_summary['high_premium'].append({
                        'noon_product': {
                            'title': noon_row.get('title', ''),
                            'price': noon_price,
                            'product_id': noon_row.get('product_id', ''),
                        },
                        'amazon_product': {
                            'title': amazon_row.get('title', ''),
                            'price': amazon_price,
                            'product_id': amazon_row.get('product_id', ''),
                        },
                        'price_ratio': round(ratio, 2),
                        'premium_pct': round((ratio - 1) * 100, 1),
                    })
                elif ratio <= 1 / threshold:
                    price_gap_summary['reverse_opportunity'].append({
                        'noon_product': {'title': noon_row.get('title', ''), 'price': noon_price},
                        'amazon_product': {'title': amazon_row.get('title', ''), 'price': amazon_price},
                        'price_ratio': round(ratio, 2),
                    })
                else:
                    price_gap_summary['similar_price'].append({
                        'noon_product': {'title': noon_row.get('title', ''), 'price': noon_price},
                        'amazon_product': {'title': amazon_row.get('title', ''), 'price': amazon_price},
                        'price_ratio': round(ratio, 2),
                    })

        result = {
            'threshold': threshold,
            'high_premium_count': len(price_gap_summary['high_premium']),
            'reverse_opportunity_count': len(price_gap_summary['reverse_opportunity']),
            'similar_price_count': len(price_gap_summary['similar_price']),
            'high_premium_opportunities': price_gap_summary['high_premium'][:50],  # Top 50
            'summary': '价格差分析完成' if price_gap_summary['high_premium'] else '未发现显著价格差机会',
        }

        self.analysis_results['price_gap'] = result
        return result

    def analyze_supply_gap(self, bsr_threshold: int = 1000) -> Dict:
        """
        分析维度 2: 供给缺口

        逻辑:
        - Amazon BSR < 1000 (热销) 且 Noon 无相同/类似产品 → 蓝海机会
        - Noon 独家产品 (Amazon 无) → 本地优势

        Args:
            bsr_threshold: BSR 阈值，越小越热销

        Returns:
            供给缺口分析结果
        """
        # 当前实现简化版 (BSR 数据需要从 Amazon 详情页爬取)
        # 这里用 review_count 作为替代指标

        amazon_popular = self.amazon_df[
            (self.amazon_df.get('review_count', 0) > 100)
        ].copy() if 'review_count' in self.amazon_df.columns else pd.DataFrame()

        noon_titles = set(self.noon_df.get('title', '').str.lower().dropna())

        supply_gap = []
        for _, row in amazon_popular.iterrows():
            title = row.get('title', '').lower()
            # 检查 Noon 是否有类似产品
            if not any(title in noon_title for noon_title in noon_titles):
                supply_gap.append({
                    'amazon_product': {
                        'title': row.get('title', ''),
                        'price': row.get('price', 0),
                        'review_count': row.get('review_count', 0),
                        'product_id': row.get('product_id', ''),
                    },
                    'opportunity_type': 'noon_missing',
                })

        result = {
            'bsr_threshold': bsr_threshold,
            'amazon_popular_count': len(amazon_popular),
            'noon_missing_count': len(supply_gap),
            'opportunities': supply_gap[:50],  # Top 50
            'summary': f'发现 {len(supply_gap)} 个 Noon 缺失的热销产品' if supply_gap else '未检测到显著供给缺口',
        }

        self.analysis_results['supply_gap'] = result
        return result

    def analyze_ad_density(self, ad_threshold: float = 0.3) -> Dict:
        """
        分析维度 3: 广告密度

        逻辑:
        - Noon 广告占比 > 30% → 竞争激烈预警
        - Amazon 广告占比 > 50% → 红海市场

        Args:
            ad_threshold: 广告密度阈值

        Returns:
            广告密度分析结果
        """
        noon_ad_rate = 0
        amazon_ad_rate = 0

        if len(self.noon_df) > 0:
            noon_ads = self.noon_df[self.noon_df.get('is_ad', False) == True]
            noon_ad_rate = len(noon_ads) / len(self.noon_df)

        if len(self.amazon_df) > 0:
            amazon_ads = self.amazon_df[self.amazon_df.get('is_ad', False) == True]
            amazon_ad_rate = len(amazon_ads) / len(self.amazon_df)

        result = {
            'noon_ad_rate': round(noon_ad_rate, 3),
            'noon_ad_count': int(noon_ad_rate * len(self.noon_df)) if len(self.noon_df) > 0 else 0,
            'noon_total': len(self.noon_df),
            'amazon_ad_rate': round(amazon_ad_rate, 3),
            'amazon_ad_count': int(amazon_ad_rate * len(self.amazon_df)) if len(self.amazon_df) > 0 else 0,
            'amazon_total': len(self.amazon_df),
            'noon_competition_level': 'high' if noon_ad_rate > ad_threshold else 'normal',
            'amazon_competition_level': 'high' if amazon_ad_rate > ad_threshold else 'normal',
            'summary': self._get_ad_summary(noon_ad_rate, amazon_ad_rate, ad_threshold),
        }

        self.analysis_results['ad_density'] = result
        return result

    def _get_ad_summary(self, noon_rate: float, amazon_rate: float, threshold: float) -> str:
        """生成广告密度分析摘要"""
        if noon_rate > threshold and amazon_rate > threshold:
            return '双平台广告密度高 - 红海市场，谨慎进入'
        elif noon_rate > threshold:
            return 'Noon 广告密集 - 竞争激烈但 Amazon 有机会'
        elif amazon_rate > threshold:
            return 'Amazon 广告密集 - Noon 可能是蓝海'
        else:
            return '双平台广告密度正常 - 健康市场环境'

    def analyze_listing_quality(self) -> Dict:
        """
        分析维度 4: Listing 质量

        逻辑:
        - Noon 平均图片数 < 3 → 优化机会
        - Amazon 平均图片数 > 6 → 标杆对比
        - 标题长度、评分完整性对比

        Returns:
            Listing 质量分析结果
        """
        # 图片数量分析
        noon_img_avg = self.noon_df.get('image_count', 0).mean() if len(self.noon_df) > 0 else 0
        amazon_img_avg = self.amazon_df.get('image_count', 0).mean() if len(self.amazon_df) > 0 else 0

        # 标题长度分析
        noon_title_len = self.noon_df.get('title', '').str.len().mean() if len(self.noon_df) > 0 else 0
        amazon_title_len = self.amazon_df.get('title', '').str.len().mean() if len(self.amazon_df) > 0 else 0

        # 评分完整性
        noon_rating_rate = self.noon_df[self.noon_df.get('rating', 0) > 0].shape[0] / len(self.noon_df) if len(self.noon_df) > 0 else 0
        amazon_rating_rate = self.amazon_df[self.amazon_df.get('rating', 0) > 0].shape[0] / len(self.amazon_df) if len(self.amazon_df) > 0 else 0

        result = {
            'image_count': {
                'noon_avg': round(noon_img_avg, 1),
                'amazon_avg': round(amazon_img_avg, 1),
                'gap': round(amazon_img_avg - noon_img_avg, 1),
            },
            'title_length': {
                'noon_avg': round(noon_title_len, 0),
                'amazon_avg': round(amazon_title_len, 0),
                'gap': round(amazon_title_len - noon_title_len, 0),
            },
            'rating_completeness': {
                'noon_rate': round(noon_rating_rate * 100, 1),
                'amazon_rate': round(amazon_rating_rate * 100, 1),
            },
            'optimization_opportunity': noon_img_avg < 3,
            'summary': self._get_listing_summary(noon_img_avg, amazon_img_avg, noon_title_len),
        }

        self.analysis_results['listing_quality'] = result
        return result

    def _get_listing_summary(self, noon_img: float, amazon_img: float, noon_title: float) -> str:
        """生成 Listing 质量分析摘要"""
        if noon_img < 3:
            return f'Noon  Listing 质量待提升 (平均{noon_img:.1f}图) - 优化图片可提升转化'
        elif noon_img < amazon_img - 2:
            return f'Noon 图片数落后 Amazon ({noon_img:.1f} vs {amazon_img:.1f}) - 有优化空间'
        else:
            return 'Noon Listing 质量与 Amazon 相当'

    def analyze_review_barrier(self, review_threshold: int = 1000) -> Dict:
        """
        分析维度 5: 评论壁垒

        逻辑:
        - Amazon 头部产品评论数 > 1000 → 进入难度大
        - Noon 评论数普遍 < 100 → 新卖家机会

        Returns:
            评论壁垒分析结果
        """
        # Amazon 评论分布
        amazon_high_review = self.amazon_df[self.amazon_df.get('review_count', 0) > review_threshold]
        amazon_mid_review = self.amazon_df[(self.amazon_df.get('review_count', 0) > 100) &
                                           (self.amazon_df.get('review_count', 0) <= review_threshold)]
        amazon_low_review = self.amazon_df[self.amazon_df.get('review_count', 0) <= 100]

        # Noon 评论分布
        noon_high_review = self.noon_df[self.noon_df.get('review_count', 0) > review_threshold]
        noon_low_review = self.noon_df[self.noon_df.get('review_count', 0) <= 100]

        result = {
            'amazon_distribution': {
                'high_review_count': len(amazon_high_review),
                'mid_review_count': len(amazon_mid_review),
                'low_review_count': len(amazon_low_review),
            },
            'noon_distribution': {
                'high_review_count': len(noon_high_review),
                'low_review_count': len(noon_low_review),
            },
            'barrier_level': 'high' if len(amazon_high_review) > len(amazon_low_review) else 'normal',
            'opportunity_level': 'high' if len(noon_low_review) > len(noon_high_review) * 2 else 'normal',
            'summary': self._get_review_summary(amazon_high_review, noon_low_review),
        }

        self.analysis_results['review_barrier'] = result
        return result

    def _get_review_summary(self, amazon_high: pd.DataFrame, noon_low: pd.DataFrame) -> str:
        """生成评论壁垒分析摘要"""
        if len(amazon_high) > 10 and len(noon_low) < 5:
            return 'Amazon 评论壁垒高 - 需要差异化策略'
        elif len(noon_low) > len(amazon_high) * 2:
            return 'Noon 评论门槛低 - 新卖家进入窗口期'
        else:
            return '评论壁垒正常 - 产品质量是关键'

    def run_full_analysis(self) -> Dict[str, Any]:
        """运行全部 5 个分析维度"""
        logger.info("开始跨平台交叉分析...")

        results = {
            'price_gap': self.analyze_price_gap(),
            'supply_gap': self.analyze_supply_gap(),
            'ad_density': self.analyze_ad_density(),
            'listing_quality': self.analyze_listing_quality(),
            'review_barrier': self.analyze_review_barrier(),
        }

        # 生成总体摘要
        results['overall_summary'] = self._generate_overall_summary()

        logger.info("跨平台交叉分析完成")
        return results

    def _generate_overall_summary(self) -> Dict:
        """生成总体机会评估"""
        opportunities = []
        warnings = []

        # 价格差机会
        if self.analysis_results.get('price_gap', {}).get('high_premium_count', 0) > 10:
            opportunities.append('显著价格差机会 (Amazon 溢价 >30%)')

        # 供给缺口
        if self.analysis_results.get('supply_gap', {}).get('noon_missing_count', 0) > 5:
            opportunities.append('Noon 平台供给缺口 (Amazon 热销但 Noon 缺失)')

        # 广告密度警告
        if self.analysis_results.get('ad_density', {}).get('noon_competition_level') == 'high':
            warnings.append('Noon 广告密度高 - 竞争激烈')

        # Listing 质量机会
        if self.analysis_results.get('listing_quality', {}).get('optimization_opportunity'):
            opportunities.append('Noon Listing 质量优化空间大')

        return {
            'opportunities': opportunities,
            'warnings': warnings,
            'recommendation': self._generate_recommendation(opportunities, warnings),
        }

    def _generate_recommendation(self, opportunities: List[str], warnings: List[str]) -> str:
        """生成操作建议"""
        if len(opportunities) >= 3 and not warnings:
            return '强烈推荐进入 - 多个维度显示蓝海机会'
        elif len(opportunities) >= 2:
            return '推荐进入 - 存在明显机会点'
        elif len(warnings) >= 2:
            return '谨慎进入 - 竞争激烈且机会有限'
        elif opportunities:
            return '可考虑进入 - 有局部机会但需精细化运营'
        else:
            return '市场成熟 - 需要差异化定位'

    def export_to_excel(self, output_path: Path) -> Path:
        """导出分析报告到 Excel"""
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            # Sheet 1: 价格差分析
            price_gap_df = pd.DataFrame(self.analysis_results['price_gap'].get('high_premium_opportunities', []))
            if not price_gap_df.empty:
                price_gap_df.to_excel(writer, sheet_name='Price_Gap', index=False)

            # Sheet 2: 供给缺口
            supply_gap_df = pd.DataFrame(self.analysis_results['supply_gap'].get('opportunities', []))
            if not supply_gap_df.empty:
                supply_gap_df.to_excel(writer, sheet_name='Supply_Gap', index=False)

            # Sheet 3: 广告密度
            ad_data = [{
                'Platform': 'Noon',
                'Ad_Rate': self.analysis_results['ad_density']['noon_ad_rate'],
                'Ad_Count': self.analysis_results['ad_density']['noon_ad_count'],
                'Total': self.analysis_results['ad_density']['noon_total'],
                'Competition_Level': self.analysis_results['ad_density']['noon_competition_level'],
            }, {
                'Platform': 'Amazon',
                'Ad_Rate': self.analysis_results['ad_density']['amazon_ad_rate'],
                'Ad_Count': self.analysis_results['ad_density']['amazon_ad_count'],
                'Total': self.analysis_results['ad_density']['amazon_total'],
                'Competition_Level': self.analysis_results['ad_density']['amazon_competition_level'],
            }]
            pd.DataFrame(ad_data).to_excel(writer, sheet_name='Ad_Density', index=False)

            # Sheet 4: Listing 质量
            listing_data = [{
                'Metric': 'Image_Count_Avg',
                'Noon': self.analysis_results['listing_quality']['image_count']['noon_avg'],
                'Amazon': self.analysis_results['listing_quality']['image_count']['amazon_avg'],
                'Gap': self.analysis_results['listing_quality']['image_count']['gap'],
            }, {
                'Metric': 'Title_Length_Avg',
                'Noon': self.analysis_results['listing_quality']['title_length']['noon_avg'],
                'Amazon': self.analysis_results['listing_quality']['title_length']['amazon_avg'],
                'Gap': self.analysis_results['listing_quality']['title_length']['gap'],
            }]
            pd.DataFrame(listing_data).to_excel(writer, sheet_name='Listing_Quality', index=False)

            # Sheet 5: 总体摘要
            summary_data = [{
                'Type': 'Opportunity',
                'Item': opp,
            } for opp in self.analysis_results['overall_summary']['opportunities']] + [{
                'Type': 'Warning',
                'Item': warn,
            } for warn in self.analysis_results['overall_summary']['warnings']]
            if summary_data:
                pd.DataFrame(summary_data).to_excel(writer, sheet_name='Summary', index=False)

        logger.info(f"分析报告已导出：{output_path}")
        return output_path


def analyze_cross_platform(noon_source: Path, amazon_source: Path,
                           output_dir: Path = None) -> Dict[str, Any]:
    """
    便捷函数：执行跨平台分析并导出报告

    Args:
        noon_source: Noon 数据源 (JSON 文件或目录)
        amazon_source: Amazon 数据源 (JSON 文件或目录)
        output_dir: 输出目录

    Returns:
        分析结果字典
    """
    # 加载数据
    analyzer = CrossPlatformAnalyzer.load_from_json(noon_source, amazon_source)

    # 执行分析
    results = analyzer.run_full_analysis()

    # 导出 Excel
    if output_dir:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        excel_path = output_dir / 'cross_analysis_report.xlsx'
        analyzer.export_to_excel(excel_path)

        # 导出 JSON 摘要
        json_path = output_dir / 'cross_analysis_summary.json'
        json_path.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding='utf-8')

    return results


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='跨平台交叉分析器 (Noon vs Amazon)')
    parser.add_argument('--noon', '-n', type=str, required=True,
                        help='Noon 数据源 (JSON 文件或目录)')
    parser.add_argument('--amazon', '-a', type=str, required=True,
                        help='Amazon 数据源 (JSON 文件或目录)')
    parser.add_argument('--output', '-o', type=str, default='cross_analysis',
                        help='输出目录')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='显示详细日志')

    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')

    print("开始跨平台交叉分析...")
    print(f"Noon 数据源：{args.noon}")
    print(f"Amazon 数据源：{args.amazon}")

    results = analyze_cross_platform(Path(args.noon), Path(args.amazon), Path(args.output))

    print("\n分析完成!")
    print(f"\n总体摘要:")
    print(f"  机会点: {len(results['overall_summary']['opportunities'])} 个")
    print(f"  警告: {len(results['overall_summary']['warnings'])} 个")
    print(f"  建议：{results['overall_summary']['recommendation']}")

    print(f"\n报告已导出到：{args.output}/")
