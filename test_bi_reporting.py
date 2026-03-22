#!/usr/bin/env python3
"""
Quick test to verify BI reporting works correctly.
"""

import sys
import json
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from bi_reporting import BIReporter

def test_bi_reporting():
    """Test BI reporting functionality."""
    print("Testing BI reporting...")
    
    # Sample revenue data
    sample_data = [
        {"engine_domain": "google.com", "keyword": "ipod", "revenue": 480.00},
        {"engine_domain": "bing.com", "keyword": "zune", "revenue": 250.00},
        {"engine_domain": "yahoo.com", "keyword": "cd player", "revenue": 150.00},
        {"engine_domain": "google.com", "keyword": "mp3 player", "revenue": 120.00},
        {"engine_domain": "bing.com", "keyword": "music player", "revenue": 80.00},
    ]
    
    # Generate BI report
    reporter = BIReporter()
    report = reporter.generate_report(revenue_data=sample_data)
    
    print(f"✅ BI report generated successfully")
    print(f"   Total Revenue: ${report.total_revenue:.2f}")
    print(f"   Keywords: {report.total_keywords}")
    print(f"   Search Engines: {report.total_search_engines}")
    print(f"   Insights: {len(report.insights)}")
    print(f"   Recommendations: {len(report.recommendations)}")
    
    # Test JSON export
    json_report = reporter.export_to_json(report)
    assert json.loads(json_report)  # Verify valid JSON
    
    # Test HTML export
    html_report = reporter.export_to_html_summary(report)
    assert "<html>" in html_report and "</html>" in html_report
    
    print("✅ JSON export works")
    print("✅ HTML export works")
    
    # Display some insights
    print("\n--- Sample Insights ---")
    for insight in report.insights[:3]:
        print(f"  • {insight}")
    
    print("\n--- Sample Recommendations ---")
    for rec in report.recommendations[:3]:
        print(f"  • {rec}")
    
    print("\n✅ BI reporting test passed!")

if __name__ == "__main__":
    test_bi_reporting()
