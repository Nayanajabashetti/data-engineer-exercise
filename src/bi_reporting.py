"""
BI Reporting module for search keyword performance analytics.

Generates comprehensive business intelligence reports from aggregated data
including revenue trends, keyword performance, search engine analysis,
and actionable insights.
"""

from __future__ import annotations

import html
import json
import logging
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

try:
    from src.partition_time import TimePartition, partition_from_unix_seconds
except ImportError:  # pragma: no cover - Lambda zip layout
    from partition_time import TimePartition, partition_from_unix_seconds

logger = logging.getLogger(__name__)


@dataclass
class KeywordMetric:
    """Individual keyword performance metric."""
    keyword: str
    search_engine: str
    revenue: float
    revenue_percentage: float
    rank: int


@dataclass
class SearchEngineMetric:
    """Search engine performance summary."""
    engine: str
    total_revenue: float
    revenue_percentage: float
    keyword_count: int
    avg_revenue_per_keyword: float


@dataclass
class TrendData:
    """Time-series trend data point."""
    date: str
    total_revenue: float
    unique_keywords: int
    unique_search_engines: int
    avg_revenue_per_keyword: float


@dataclass
class BIReport:
    """Comprehensive BI report containing all insights."""
    report_date: str
    report_timestamp: str
    data_partition: Optional[TimePartition]
    
    # Summary metrics
    total_revenue: float
    total_keywords: int
    total_search_engines: int
    avg_revenue_per_keyword: float
    
    # Top performers
    top_keywords: List[KeywordMetric]
    bottom_keywords: List[KeywordMetric]
    search_engines: List[SearchEngineMetric]
    
    # Trends (if historical data available)
    revenue_trend: List[TrendData]
    
    # Insights
    insights: List[str]
    recommendations: List[str]


class BIReporter:
    """
    Generates business intelligence reports from search keyword performance data.
    
    Supports both real-time (single day) and historical analysis with trend detection.
    """
    
    def __init__(self):
        self.insight_thresholds = {
            "high_performing_keyword_min_revenue": 100.0,
            "low_performing_keyword_max_revenue": 10.0,
            "concerning_drop_percentage": 20.0,
            "excellent_growth_percentage": 15.0,
        }
    
    def generate_report(
        self,
        revenue_data: List[Dict[str, Any]],
        historical_data: Optional[List[Dict[str, Any]]] = None,
        partition_info: Optional[TimePartition] = None,
        report_date: Optional[str] = None,
    ) -> BIReport:
        """
        Generate comprehensive BI report from revenue data.
        
        Args:
            revenue_data: List of records with 'engine_domain', 'keyword', 'revenue'
            historical_data: Optional historical data for trend analysis
            partition_info: Time partition information for the current data
            report_date: Optional ``YYYY-MM-DD`` (e.g. Airflow ``ds``). If omitted, uses today's UTC date.
            
        Returns:
            BIReport with all insights and metrics
        """
        now = datetime.now(timezone.utc)
        rd = (report_date or "").strip() or now.strftime("%Y-%m-%d")
        
        # Basic metrics
        total_revenue = sum(float(r.get("revenue", 0)) for r in revenue_data)
        total_keywords = len(revenue_data)
        unique_engines = len(set(r.get("engine_domain", "") for r in revenue_data))
        avg_revenue_per_keyword = total_revenue / total_keywords if total_keywords > 0 else 0
        
        # Top/Bottom keywords
        sorted_keywords = sorted(
            revenue_data, 
            key=lambda x: float(x.get("revenue", 0)), 
            reverse=True
        )
        
        top_keywords = self._build_keyword_metrics(sorted_keywords[:10], total_revenue)
        bottom_keywords = self._build_keyword_metrics(sorted_keywords[-10:], total_revenue)
        
        # Search engine analysis
        engine_metrics = self._analyze_search_engines(revenue_data, total_revenue)
        
        # Trend analysis
        revenue_trend = self._analyze_trends(historical_data) if historical_data else []
        
        # Generate insights and recommendations
        insights = self._generate_insights(
            total_revenue, top_keywords, bottom_keywords, engine_metrics, revenue_trend
        )
        recommendations = self._generate_recommendations(insights, engine_metrics)
        
        return BIReport(
            report_date=rd,
            report_timestamp=now.isoformat(),
            data_partition=partition_info,
            total_revenue=total_revenue,
            total_keywords=total_keywords,
            total_search_engines=unique_engines,
            avg_revenue_per_keyword=avg_revenue_per_keyword,
            top_keywords=top_keywords,
            bottom_keywords=bottom_keywords,
            search_engines=engine_metrics,
            revenue_trend=revenue_trend,
            insights=insights,
            recommendations=recommendations
        )
    
    def _build_keyword_metrics(self, keywords: List[Dict[str, Any]], total_revenue: float) -> List[KeywordMetric]:
        """Build keyword metrics with revenue percentages and rankings."""
        metrics = []
        for i, kw in enumerate(keywords, 1):
            revenue = float(kw.get("revenue", 0))
            metrics.append(KeywordMetric(
                keyword=kw.get("keyword", ""),
                search_engine=kw.get("engine_domain", ""),
                revenue=revenue,
                revenue_percentage=(revenue / total_revenue * 100) if total_revenue > 0 else 0,
                rank=i
            ))
        return metrics
    
    def _analyze_search_engines(self, revenue_data: List[Dict[str, Any]], total_revenue: float) -> List[SearchEngineMetric]:
        """Analyze performance by search engine."""
        engine_stats = {}
        
        for record in revenue_data:
            engine = record.get("engine_domain", "")
            revenue = float(record.get("revenue", 0))
            
            if engine not in engine_stats:
                engine_stats[engine] = {"revenue": 0, "count": 0}
            
            engine_stats[engine]["revenue"] += revenue
            engine_stats[engine]["count"] += 1
        
        metrics = []
        for engine, stats in engine_stats.items():
            metrics.append(SearchEngineMetric(
                engine=engine,
                total_revenue=stats["revenue"],
                revenue_percentage=(stats["revenue"] / total_revenue * 100) if total_revenue > 0 else 0,
                keyword_count=stats["count"],
                avg_revenue_per_keyword=stats["revenue"] / stats["count"] if stats["count"] > 0 else 0
            ))
        
        return sorted(metrics, key=lambda x: x.total_revenue, reverse=True)
    
    def _analyze_trends(self, historical_data: List[Dict[str, Any]]) -> List[TrendData]:
        """Analyze revenue trends over time."""
        trends = []
        
        # Group by date
        daily_stats = {}
        for record in historical_data:
            date = record.get("date", "unknown")
            revenue = float(record.get("revenue", 0))
            keyword = record.get("keyword", "")
            engine = record.get("engine_domain", "")
            
            if date not in daily_stats:
                daily_stats[date] = {"revenue": 0, "keywords": set(), "engines": set()}
            
            daily_stats[date]["revenue"] += revenue
            daily_stats[date]["keywords"].add(keyword)
            daily_stats[date]["engines"].add(engine)
        
        # Build trend data
        for date, stats in sorted(daily_stats.items()):
            total_revenue = stats["revenue"]
            keyword_count = len(stats["keywords"])
            engine_count = len(stats["engines"])
            avg_revenue = total_revenue / keyword_count if keyword_count > 0 else 0
            
            trends.append(TrendData(
                date=date,
                total_revenue=total_revenue,
                unique_keywords=keyword_count,
                unique_search_engines=engine_count,
                avg_revenue_per_keyword=avg_revenue
            ))
        
        return trends
    
    def _generate_insights(
        self,
        total_revenue: float,
        top_keywords: List[KeywordMetric],
        bottom_keywords: List[KeywordMetric],
        engine_metrics: List[SearchEngineMetric],
        revenue_trend: List[TrendData]
    ) -> List[str]:
        """Generate actionable insights from the data."""
        insights = []
        
        # Revenue insights
        if total_revenue > 1000:
            insights.append(f"Strong revenue performance with ${total_revenue:.2f} total revenue")
        elif total_revenue > 100:
            insights.append(f"Moderate revenue performance with ${total_revenue:.2f} total revenue")
        else:
            insights.append(f"Low revenue performance with ${total_revenue:.2f} total revenue")
        
        # Top keyword insights
        if top_keywords:
            top_kw = top_keywords[0]
            if top_kw.revenue > self.insight_thresholds["high_performing_keyword_min_revenue"]:
                insights.append(
                    f"Top performing keyword '{top_kw.keyword}' on {top_kw.search_engine} "
                    f"generated ${top_kw.revenue:.2f} ({top_kw.revenue_percentage:.1f}% of total)"
                )
        
        # Search engine dominance
        if engine_metrics:
            top_engine = engine_metrics[0]
            if top_engine.revenue_percentage > 70:
                insights.append(
                    f"{top_engine.engine} dominates with {top_engine.revenue_percentage:.1f}% of total revenue"
                )
            elif len(engine_metrics) == 1:
                insights.append(f"Single search engine dependency: {top_engine.engine}")
        
        # Trend insights
        if len(revenue_trend) >= 2:
            recent = revenue_trend[-1]
            previous = revenue_trend[-2]
            
            if previous.total_revenue > 0:
                change_pct = ((recent.total_revenue - previous.total_revenue) / previous.total_revenue) * 100
                
                if change_pct > self.insight_thresholds["excellent_growth_percentage"]:
                    insights.append(f"Excellent revenue growth of {change_pct:.1f}% vs previous period")
                elif change_pct < -self.insight_thresholds["concerning_drop_percentage"]:
                    insights.append(f"Concerning revenue drop of {abs(change_pct):.1f}% vs previous period")
                elif change_pct > 0:
                    insights.append(f"Positive revenue growth of {change_pct:.1f}% vs previous period")
                else:
                    insights.append(f"Revenue decline of {abs(change_pct):.1f}% vs previous period")
        
        # Keyword diversity insights
        if len(top_keywords) >= 5:
            top_5_revenue = sum(kw.revenue for kw in top_keywords[:5])
            top_5_percentage = (top_5_revenue / total_revenue * 100) if total_revenue > 0 else 0
            
            if top_5_percentage > 80:
                insights.append("High concentration: top 5 keywords drive most revenue")
            elif top_5_percentage < 40:
                insights.append("Good keyword diversity with distributed revenue")
        
        return insights
    
    def _generate_recommendations(
        self,
        insights: List[str],
        engine_metrics: List[SearchEngineMetric]
    ) -> List[str]:
        """Generate actionable recommendations based on insights."""
        recommendations = []
        
        # Revenue-based recommendations
        if any("Low revenue performance" in insight for insight in insights):
            recommendations.append("Consider optimizing SEO/SEM strategies for low-performing keywords")
            recommendations.append("Review keyword bidding strategies and ad copy")
        
        # Search engine diversification
        if len(engine_metrics) == 1:
            recommendations.append("Diversify traffic sources to reduce dependency on single search engine")
        elif len(engine_metrics) > 0:
            top_engine = engine_metrics[0]
            if top_engine.revenue_percentage > 70:
                recommendations.append(f"Explore opportunities on other search engines to reduce {top_engine.engine} dependency")
        
        # Trend-based recommendations
        if any("Concerning revenue drop" in insight for insight in insights):
            recommendations.append("Investigate cause of revenue drop immediately")
            recommendations.append("Consider promotional campaigns to boost performance")
        
        if any("Excellent revenue growth" in insight for insight in insights):
            recommendations.append("Analyze successful strategies and scale them")
            recommendations.append("Consider increasing budget for high-performing keywords")
        
        # Keyword performance recommendations
        if any("High concentration" in insight for insight in insights):
            recommendations.append("Develop long-tail keyword strategy to reduce concentration risk")
        
        return recommendations
    
    def export_to_json(self, report: BIReport) -> str:
        """Export BI report to JSON format."""
        return json.dumps(asdict(report), indent=2, default=str)
    
    def export_to_html_summary(self, report: BIReport) -> str:
        """Export BI report to a self-contained HTML summary with charts (Chart.js CDN) and styled KPIs."""

        def esc(s: str) -> str:
            return html.escape(s, quote=True)

        # Chart data (JSON-safe for embedding)
        engines = list(report.search_engines)
        eng_labels = [e.engine for e in engines]
        eng_values = [round(e.total_revenue, 2) for e in engines]
        eng_colors = ["#6366f1", "#8b5cf6", "#ec4899", "#f59e0b", "#10b981", "#06b6d4"]

        top_kw = report.top_keywords[:10]
        kw_labels = [f"{k.keyword}"[:32] for k in top_kw]
        kw_values = [round(k.revenue, 2) for k in top_kw]
        kw_full = [f"{k.keyword} ({k.search_engine})" for k in top_kw]

        trend_labels = [t.date for t in report.revenue_trend[:30]]
        trend_values = [round(t.total_revenue, 2) for t in report.revenue_trend[:30]]

        insight_icons = ["🎯", "📈", "💡", "⚡", "🔍", "✨", "🏆", "📊"]
        rec_icons = ["→", "★", "◆", "●", "▸"]

        eng_labels_j = json.dumps(eng_labels)
        eng_values_j = json.dumps(eng_values)
        eng_colors_j = json.dumps([eng_colors[i % len(eng_colors)] for i in range(len(engines))])
        kw_labels_j = json.dumps(kw_labels)
        kw_values_j = json.dumps(kw_values)
        kw_tooltips_j = json.dumps(kw_full)
        trend_labels_j = json.dumps(trend_labels)
        trend_values_j = json.dumps(trend_values)
        has_trend = len(trend_labels) > 0

        rows_top = ""
        for kw in top_kw:
            rows_top += f"""
                    <tr>
                        <td>{kw.rank}</td>
                        <td>{esc(kw.keyword)}</td>
                        <td>{esc(kw.search_engine)}</td>
                        <td>${kw.revenue:.2f}</td>
                        <td>{kw.revenue_percentage:.1f}%</td>
                    </tr>"""

        rows_eng = ""
        for engine in engines:
            rows_eng += f"""
                    <tr>
                        <td>{esc(engine.engine)}</td>
                        <td>${engine.total_revenue:.2f}</td>
                        <td>{engine.revenue_percentage:.1f}%</td>
                        <td>{engine.keyword_count}</td>
                        <td>${engine.avg_revenue_per_keyword:.2f}</td>
                    </tr>"""

        insight_blocks = ""
        for i, insight in enumerate(report.insights):
            ic = insight_icons[i % len(insight_icons)]
            insight_blocks += f"""
                <div class="insight-card">
                    <span class="insight-icon" aria-hidden="true">{ic}</span>
                    <p class="insight-text">{esc(insight)}</p>
                </div>"""

        rec_blocks = ""
        for i, rec in enumerate(report.recommendations):
            rc = rec_icons[i % len(rec_icons)]
            rec_blocks += f"""
                <div class="rec-card">
                    <span class="rec-bullet">{rc}</span>
                    <p>{esc(rec)}</p>
                </div>"""

        if has_trend:
            trend_chart_script = f"""
    const tLabels = {trend_labels_j};
    const tData = {trend_values_j};
    if (tLabels.length && document.getElementById('trendChart')) {{
      new Chart(document.getElementById('trendChart'), {{
        type: 'line',
        data: {{
          labels: tLabels,
          datasets: [{{
            label: 'Revenue ($)',
            data: tData,
            fill: true,
            backgroundColor: 'rgba(167, 139, 250, 0.15)',
            borderColor: 'rgba(167, 139, 250, 1)',
            tension: 0.35,
            pointRadius: 4
          }}]
        }},
        options: {{
          responsive: true,
          plugins: {{
            title: {{ display: true, text: 'Revenue trend (historical)', color: '#e2e8f0', font: {{ size: 14 }} }}
          }},
          scales: {{
            y: {{ grid: {{ color: 'rgba(148,163,184,0.15)' }}, ticks: {{ callback: v => '$' + v }} }}
          }}
        }}
      }});
    }}
"""
        else:
            trend_chart_script = ""

        trend_canvas_html = (
            '<div class="chart-box" style="margin-top:1rem"><canvas id="trendChart" aria-label="Revenue trend"></canvas></div>'
            if has_trend
            else ""
        )

        return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Search Keyword Performance — BI Report</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js" crossorigin="anonymous"></script>
  <style>
    :root {{
      --bg: #0f172a;
      --card: #1e293b;
      --muted: #94a3b8;
      --accent: #38bdf8;
      --accent2: #a78bfa;
      --success: #34d399;
      --warning: #fbbf24;
      --border: rgba(148, 163, 184, 0.15);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      font-family: "Segoe UI", system-ui, -apple-system, sans-serif;
      margin: 0;
      background: linear-gradient(160deg, #0f172a 0%, #1e1b4b 45%, #0f172a 100%);
      color: #e2e8f0;
      min-height: 100vh;
    }}
    .wrap {{ max-width: 1200px; margin: 0 auto; padding: 1.5rem; }}
    .hero {{
      background: linear-gradient(135deg, rgba(56, 189, 248, 0.2), rgba(167, 139, 250, 0.25));
      border: 1px solid var(--border);
      border-radius: 16px;
      padding: 2rem;
      margin-bottom: 1.5rem;
      box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.45);
    }}
    .hero h1 {{
      margin: 0 0 0.5rem;
      font-size: 1.75rem;
      font-weight: 700;
      letter-spacing: -0.02em;
    }}
    .hero-meta {{ color: var(--muted); font-size: 0.9rem; }}
    .kpi-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
      gap: 1rem;
      margin-bottom: 1.5rem;
    }}
    .kpi {{
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 1.25rem;
      position: relative;
      overflow: hidden;
    }}
    .kpi::before {{
      content: "";
      position: absolute;
      top: 0; left: 0; right: 0;
      height: 3px;
      background: linear-gradient(90deg, var(--accent), var(--accent2));
    }}
    .kpi-label {{ font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.08em; color: var(--muted); }}
    .kpi-value {{ font-size: 1.65rem; font-weight: 700; margin-top: 0.35rem;
      background: linear-gradient(90deg, #f8fafc, #cbd5e1);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
      background-clip: text;
    }}
    .kpi-sub {{ font-size: 0.8rem; color: var(--muted); margin-top: 0.25rem; }}
    h2 {{
      font-size: 1.15rem;
      margin: 0 0 1rem;
      display: flex;
      align-items: center;
      gap: 0.5rem;
    }}
    .section {{
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 1.25rem 1.5rem;
      margin-bottom: 1.25rem;
    }}
    .chart-row {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 1.25rem;
    }}
    @media (max-width: 900px) {{ .chart-row {{ grid-template-columns: 1fr; }} }}
    .chart-box {{
      background: rgba(15, 23, 42, 0.5);
      border-radius: 12px;
      padding: 1rem;
      min-height: 280px;
    }}
    .chart-box canvas {{ max-height: 260px; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 0.9rem; }}
    th, td {{ padding: 0.65rem 0.75rem; text-align: left; border-bottom: 1px solid var(--border); }}
    th {{ color: var(--muted); font-weight: 600; font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.05em; }}
    tr:hover td {{ background: rgba(56, 189, 248, 0.06); }}
    .insight-card {{
      display: flex; gap: 1rem; align-items: flex-start;
      background: linear-gradient(135deg, rgba(56, 189, 248, 0.12), rgba(167, 139, 250, 0.08));
      border: 1px solid rgba(56, 189, 248, 0.25);
      border-radius: 12px;
      padding: 1rem 1.1rem;
      margin-bottom: 0.75rem;
    }}
    .insight-icon {{ font-size: 1.5rem; line-height: 1; flex-shrink: 0; }}
    .insight-text {{ margin: 0; line-height: 1.5; }}
    .rec-card {{
      display: flex; gap: 0.75rem; align-items: baseline;
      background: rgba(251, 191, 36, 0.08);
      border: 1px solid rgba(251, 191, 36, 0.25);
      border-radius: 10px;
      padding: 0.85rem 1rem;
      margin-bottom: 0.5rem;
    }}
    .rec-card p {{ margin: 0; line-height: 1.45; }}
    .rec-bullet {{ color: var(--warning); font-weight: bold; flex-shrink: 0; }}
    .footnote {{ font-size: 0.75rem; color: var(--muted); margin-top: 2rem; text-align: center; }}
  </style>
</head>
<body>
  <div class="wrap">
    <header class="hero">
      <h1>Search Keyword Performance</h1>
      <p class="hero-meta">Report date <strong>{esc(report.report_date)}</strong> · Generated {esc(report.report_timestamp)}</p>
    </header>

    <div class="kpi-grid">
      <div class="kpi">
        <div class="kpi-label">Total revenue</div>
        <div class="kpi-value">${report.total_revenue:,.2f}</div>
        <div class="kpi-sub">Aggregated attributed revenue</div>
      </div>
      <div class="kpi">
        <div class="kpi-label">Keywords</div>
        <div class="kpi-value">{report.total_keywords}</div>
        <div class="kpi-sub">In this snapshot</div>
      </div>
      <div class="kpi">
        <div class="kpi-label">Search engines</div>
        <div class="kpi-value">{report.total_search_engines}</div>
        <div class="kpi-sub">Distinct sources</div>
      </div>
      <div class="kpi">
        <div class="kpi-label">Avg / keyword</div>
        <div class="kpi-value">${report.avg_revenue_per_keyword:,.2f}</div>
        <div class="kpi-sub">Mean revenue per row</div>
      </div>
    </div>

    <div class="section">
      <h2>📊 Revenue visuals</h2>
      <div class="chart-row">
        <div class="chart-box">
          <canvas id="engineChart" aria-label="Revenue by search engine"></canvas>
        </div>
        <div class="chart-box">
          <canvas id="keywordChart" aria-label="Top keywords by revenue"></canvas>
        </div>
      </div>
      {trend_canvas_html}
    </div>

    <div class="section">
      <h2>🏆 Top keywords</h2>
      <table>
        <thead><tr><th>Rank</th><th>Keyword</th><th>Search Engine</th><th>Revenue</th><th>% of Total</th></tr></thead>
        <tbody>{rows_top}</tbody>
      </table>
    </div>

    <div class="section">
      <h2>🌐 Search engine performance</h2>
      <table>
        <thead><tr><th>Engine</th><th>Revenue</th><th>%</th><th>Keywords</th><th>Avg / kw</th></tr></thead>
        <tbody>{rows_eng}</tbody>
      </table>
    </div>

    <div class="section">
      <h2>✨ Key insights</h2>
      {insight_blocks if insight_blocks else '<p class="hero-meta">No automated insights for this run.</p>'}
    </div>

    <div class="section">
      <h2>📋 Recommendations</h2>
      {rec_blocks if rec_blocks else '<p class="hero-meta">No recommendations.</p>'}
    </div>

    <p class="footnote">Charts load from CDN (requires network). Data reflects Glue/Lambda curated output for this report date.</p>
  </div>

  <script>
    Chart.defaults.color = '#94a3b8';
    Chart.defaults.borderColor = 'rgba(148, 163, 184, 0.2)';

    const engLabels = {eng_labels_j};
    const engData = {eng_values_j};
    const engBg = {eng_colors_j};

    if (engLabels.length && document.getElementById('engineChart')) {{
      new Chart(document.getElementById('engineChart'), {{
        type: 'doughnut',
        data: {{
          labels: engLabels,
          datasets: [{{
            data: engData,
            backgroundColor: engBg,
            borderWidth: 2,
            borderColor: '#1e293b'
          }}]
        }},
        options: {{
          responsive: true,
          maintainAspectRatio: true,
          plugins: {{
            legend: {{ position: 'bottom', labels: {{ padding: 12, usePointStyle: true }} }},
            title: {{ display: true, text: 'Revenue by search engine', color: '#e2e8f0', font: {{ size: 14 }} }}
          }}
        }}
      }});
    }}

    const kwLabels = {kw_labels_j};
    const kwData = {kw_values_j};
    const kwTips = {kw_tooltips_j};

    if (kwLabels.length && document.getElementById('keywordChart')) {{
      new Chart(document.getElementById('keywordChart'), {{
        type: 'bar',
        data: {{
          labels: kwLabels,
          datasets: [{{
            label: 'Revenue ($)',
            data: kwData,
            backgroundColor: 'rgba(56, 189, 248, 0.65)',
            borderColor: 'rgba(56, 189, 248, 1)',
            borderWidth: 1,
            borderRadius: 6
          }}]
        }},
        options: {{
          responsive: true,
          maintainAspectRatio: true,
          indexAxis: 'y',
          plugins: {{
            legend: {{ display: false }},
            title: {{ display: true, text: 'Top keywords (revenue)', color: '#e2e8f0', font: {{ size: 14 }} }},
            tooltip: {{
              callbacks: {{
                title: function(items) {{
                  const i = items[0].dataIndex;
                  return kwTips[i] || items[0].label;
                }}
              }}
            }}
          }},
          scales: {{
            x: {{ grid: {{ color: 'rgba(148,163,184,0.15)' }}, ticks: {{ callback: v => '$' + v }} }},
            y: {{ grid: {{ display: false }} }}
          }}
        }}
      }});
    }}

    {trend_chart_script}
  </script>
</body>
</html>
"""
