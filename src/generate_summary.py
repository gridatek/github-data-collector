#!/usr/bin/env python3
"""
Generate summary report from collected GitHub data
"""
import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional
import pandas as pd


def generate_summary_report(repo_file: str, contrib_file: Optional[str] = None) -> Dict[str, Any]:
  """Generate comprehensive summary report"""

  # Load repository data
  with open(repo_file, 'r', encoding='utf-8') as f:
    repos_data = json.load(f)

  repo_df = pd.DataFrame(repos_data)

  # Basic statistics
  total_repos = len(repos_data)
  total_orgs = repo_df['organization'].nunique()
  total_stars = int(repo_df['stars'].sum())
  total_forks = int(repo_df['forks'].sum())

  # Organization statistics
  org_stats = repo_df.groupby('organization').agg({
    'name': 'count',
    'stars': ['sum', 'mean', 'max'],
    'forks': ['sum', 'mean', 'max'],
    'open_issues': 'sum'
  }).round(2)

  # Language statistics
  language_counts = repo_df['language'].value_counts().head(15).to_dict()

  # Top repositories
  top_starred = repo_df.nlargest(25, 'stars')[
    ['full_name', 'stars', 'forks', 'language', 'organization']
  ].to_dict('records')

  top_forked = repo_df.nlargest(25, 'forks')[
    ['full_name', 'stars', 'forks', 'language', 'organization']
  ].to_dict('records')

  # License analysis
  license_counts = repo_df['license'].value_counts().head(10).to_dict()

  # Repository size analysis
  size_stats = {
    'total_size_kb': int(repo_df['size'].sum()),
    'average_size_kb': round(repo_df['size'].mean(), 2),
    'median_size_kb': round(repo_df['size'].median(), 2),
    'largest_repo': repo_df.loc[repo_df['size'].idxmax()]['full_name'] if not repo_df.empty else None
  }

  summary = {
    'collection_metadata': {
      'generation_timestamp': datetime.now().isoformat(),
      'total_repositories': total_repos,
      'total_organizations': total_orgs,
      'organizations': sorted(repo_df['organization'].unique().tolist()),
    },
    'repository_statistics': {
      'total_stars': total_stars,
      'total_forks': total_forks,
      'total_open_issues': int(repo_df['open_issues'].sum()),
      'average_stars_per_repo': round(repo_df['stars'].mean(), 2),
      'average_forks_per_repo': round(repo_df['forks'].mean(), 2),
      'median_stars': int(repo_df['stars'].median()),
      'median_forks': int(repo_df['forks'].median())
    },
    'organization_breakdown': org_stats.to_dict(),
    'programming_languages': {
      'top_languages': language_counts,
      'total_languages': repo_df['language'].nunique(),
      'repositories_with_language': repo_df['language'].notna().sum()
    },
    'repository_rankings': {
      'top_starred_repositories': top_starred,
      'top_forked_repositories': top_forked
    },
    'license_analysis': {
      'license_distribution': license_counts,
      'repositories_with_license': repo_df['license'].notna().sum(),
      'repositories_without_license': repo_df['license'].isna().sum()
    },
    'repository_sizes': size_stats,
    'activity_indicators': {
      'recently_updated': repo_df['updated_at'].notna().sum(),
      'with_issues_enabled': repo_df['has_issues'].sum(),
      'with_wiki_enabled': repo_df['has_wiki'].sum(),
      'with_pages_enabled': repo_df['has_pages'].sum(),
      'archived_repositories': repo_df['archived'].sum()
    }
  }

  # Add contribution analysis if available
  if contrib_file and Path(contrib_file).exists():
    with open(contrib_file, 'r', encoding='utf-8') as f:
      contrib_data = json.load(f)

    if contrib_data:
      total_contributors = sum(item['total_contributors'] for item in contrib_data)

      # Most active contributors across all repos
      contributor_activity = {}
      for repo in contrib_data:
        for contributor in repo['contributors']:
          login = contributor['login']
          if login not in contributor_activity:
            contributor_activity[login] = {
              'total_contributions': 0,
              'repos_contributed': 0,
              'avatar_url': contributor['avatar_url'],
              'html_url': contributor['html_url']
            }
          contributor_activity[login]['total_contributions'] += contributor['contributions']
          contributor_activity[login]['repos_contributed'] += 1

      # Sort by total contributions
      top_contributors = sorted(
        contributor_activity.items(),
        key=lambda x: x[1]['total_contributions'],
        reverse=True
      )[:25]

      summary['contribution_analysis'] = {
        'repositories_analyzed': len(contrib_data),
        'total_contributors': total_contributors,
        'average_contributors_per_repo': round(total_contributors / len(contrib_data), 2),
        'top_contributors': [
          {
            'login': login,
            'total_contributions': data['total_contributions'],
            'repos_contributed': data['repos_contributed'],
            'avatar_url': data['avatar_url'],
            'html_url': data['html_url']
          }
          for login, data in top_contributors
        ]
      }

  return summary


def main():
  parser = argparse.ArgumentParser(description='Generate GitHub data summary report')
  parser.add_argument('--repo-file', required=True, help='Repository data JSON file')
  parser.add_argument('--contrib-file', help='Contribution data JSON file (optional)')
  parser.add_argument('--output-file', required=True, help='Output summary JSON file')

  args = parser.parse_args()

  print("📊 Generating summary report...")
  summary = generate_summary_report(args.repo_file, args.contrib_file)

  # Save summary
  output_file = Path(args.output_file)
  output_file.parent.mkdir(parents=True, exist_ok=True)

  with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(summary, f, indent=2, ensure_ascii=False, default=str)

  print(f"✅ Summary report saved to {output_file}")
  print(f"📈 Processed {summary['collection_metadata']['total_repositories']} repositories")
  print(f"🏢 Across {summary['collection_metadata']['total_organizations']} organizations")


if __name__ == '__main__':
  main()
