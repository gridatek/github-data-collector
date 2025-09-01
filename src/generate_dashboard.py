#!/usr/bin/env python3
"""
Generate HTML dashboard from GitHub data
"""
import argparse
import json
from pathlib import Path
from datetime import datetime
import plotly.graph_objects as go
import plotly.express as px
from jinja2 import Template


def create_dashboard_html(summary_data: dict) -> str:
  """Create HTML dashboard"""

  template_str = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>GitHub Data Collection Dashboard</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }
        .container { max-width: 1200px; margin: 0 auto; }
        .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; border-radius: 10px; text-align: center; margin-bottom: 30px; }
        .metrics { display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 20px; margin-bottom: 30px; }
        .metric-card { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); text-align: center; }
        .metric-value { font-size: 2em; font-weight: bold; color: #667eea; }
        .metric-label { color: #666; margin-top: 5px; }
        .chart-container { background: white; margin: 20px 0; padding: 20px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
        .table { width: 100%; border-collapse: collapse; margin-top: 10px; }
        .table th, .table td { padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }
        .table th { background-color: #f8f9fa; font-weight: bold; }
        .table tr:hover { background-color: #f5f5f5; }
        .org-tag { background: #667eea; color: white; padding: 2px 8px; border-radius: 12px; font-size: 0.8em; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 GitHub Data Collection Dashboard</h1>
            <p>Generated on {{ summary_data.collection_metadata.generation_timestamp[:19] }}</p>
        </div>

        <div class="metrics">
            <div class="metric-card">
                <div class="metric-value">{{ "{:,}".format(summary_data.collection_metadata.total_repositories) }}</div>
                <div class="metric-label">Total Repositories</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{{ "{:,}".format(summary_data.repository_statistics.total_stars) }}</div>
                <div class="metric-label">Total Stars</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{{ "{:,}".format(summary_data.repository_statistics.total_forks) }}</div>
                <div class="metric-label">Total Forks</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{{ summary_data.collection_metadata.total_organizations }}</div>
                <div class="metric-label">Organizations</div>
            </div>
        </div>

        <div class="chart-container">
            <h2>🏆 Top Starred Repositories</h2>
            <table class="table">
                <thead>
                    <tr>
                        <th>Repository</th>
                        <th>Organization</th>
                        <th>Stars</th>
                        <th>Forks</th>
                        <th>Language</th>
                    </tr>
                </thead>
                <tbody>
                    {% for repo in summary_data.repository_rankings.top_starred_repositories[:15] %}
                    <tr>
                        <td><strong>{{ repo.full_name }}</strong></td>
                        <td><span class="org-tag">{{ repo.organization }}</span></td>
                        <td>{{ "{:,}".format(repo.stars) }}</td>
                        <td>{{ "{:,}".format(repo.forks) }}</td>
                        <td>{{ repo.language or 'N/A' }}</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>

        <div class="chart-container">
            <h2>🔀 Programming Languages Distribution</h2>
            <div id="languages-chart"></div>
        </div>

        <div class="chart-container">
            <h2>🏢 Organizations Comparison</h2>
            <div id="organizations-chart"></div>
        </div>

        {% if summary_data.contribution_analysis %}
        <div class="chart-container">
            <h2>👥 Top Contributors</h2>
            <table class="table">
                <thead>
                    <tr>
                        <th>Contributor</th>
                        <th>Total Contributions</th>
                        <th>Repositories</th>
                        <th>Profile</th>
                    </tr>
                </thead>
                <tbody>
                    {% for contrib in summary_data.contribution_analysis.top_contributors[:10] %}
                    <tr>
                        <td>
                            <img src="{{ contrib.avatar_url }}" width="24" height="24" style="border-radius: 50%; vertical-align: middle; margin-right: 8px;">
                            <strong>{{ contrib.login }}</strong>
                        </td>
                        <td>{{ "{:,}".format(contrib.total_contributions) }}</td>
                        <td>{{ contrib.repos_contributed }}</td>
                        <td><a href="{{ contrib.html_url }}" target="_blank">View Profile</a></td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>
        {% endif %}
    </div>

    <script>
        // Languages Chart
        const languagesData = {{ summary_data.programming_languages.top_languages | tojsonfilter }};
        const languagesChart = {
            data: [{
                values: Object.values(languagesData),
                labels: Object.keys(languagesData),
                type: 'pie',
                hole: 0.4,
                marker: {
                    colors: ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7', '#DDA0DD', '#98D8C8', '#F7DC6F', '#BB8FCE', '#85C1E9']
                }
            }],
            layout: {
                title: 'Programming Languages Distribution',
                showlegend: true,
                height: 400
            }
        };
        Plotly.newPlot('languages-chart', languagesChart.data, languagesChart.layout);

        // Organizations Chart
        const orgData = {{ summary_data.organization_breakdown | tojsonfilter }};
        const orgNames = Object.keys(orgData);
        const orgRepos = orgNames.map(org => orgData[org].name);
        const orgStars = orgNames.map(org => orgData[org].stars.sum);

        const organizationsChart = {
            data: [{
                x: orgNames,
                y: orgStars,
                type: 'bar',
                marker: {
                    color: '#667eea',
                    opacity: 0.8
                },
                text: orgStars.map(val => val.toLocaleString()),
                textposition: 'auto',
            }],
            layout: {
                title: 'Total Stars by Organization',
                xaxis: { title: 'Organization' },
                yaxis: { title: 'Total Stars' },
                height: 400
            }
        };
        Plotly.newPlot('organizations-chart', organizationsChart.data, organizationsChart.layout);
    </script>
</body>
</html>
    """

  template = Template(template_str)
  return template.render(summary_data=summary_data)


def main():
  parser = argparse.ArgumentParser(description='Generate HTML dashboard')
  parser.add_argument('--output-dir', required=True, help='Output directory containing JSON files')
  parser.add_argument('--web-dir', required=True, help='Web directory for HTML output')

  args = parser.parse_args()

  # Find the latest summary file
  output_dir = Path(args.output_dir)
  summary_files = list(output_dir.glob('github_summary_*.json'))

  if not summary_files:
    print("❌ No summary files found")
    return

  latest_summary = max(summary_files, key=lambda x: x.stat().st_mtime)
  print(f"📊 Using summary file: {latest_summary}")

  # Load summary data
  with open(latest_summary, 'r', encoding='utf-8') as f:
    summary_data = json.load(f)

  # Generate HTML
  html_content = create_dashboard_html(summary_data)

  # Save HTML dashboard
  web_dir = Path(args.web_dir)
  web_dir.mkdir(parents=True, exist_ok=True)

  dashboard_file = web_dir / 'index.html'
  with open(dashboard_file, 'w', encoding='utf-8') as f:
    f.write(html_content)

  print(f"✅ Dashboard generated: {dashboard_file}")


if __name__ == '__main__':
  main()
