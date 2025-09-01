#!/usr/bin/env python3
"""
GitHub Contribution Data Collection Script
"""
import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any

from github import Github


class GitHubContributionCollector:
  """Collect GitHub contribution data"""

  def __init__(self, github_token: str):
    self.github = Github(github_token)

  def collect_repository_contributors(self, repo_full_name: str, max_contributors: int = 20) -> Dict[str, Any]:
    """Collect contributors for a specific repository"""
    try:
      print(f"  👥 Collecting contributors for {repo_full_name}")
      repo = self.github.get_repo(repo_full_name)
      contributors = list(repo.get_contributors())[:max_contributors]

      contribution_data = {
        'repo_full_name': repo_full_name,
        'total_contributors': len(contributors),
        'contributors': [],
        'collection_timestamp': datetime.now().isoformat()
      }

      for contributor in contributors:
        contrib_info = {
          'login': contributor.login,
          'contributions': contributor.contributions,
          'avatar_url': contributor.avatar_url,
          'html_url': contributor.html_url,
          'type': contributor.type,
          'site_admin': getattr(contributor, 'site_admin', False)
        }
        contribution_data['contributors'].append(contrib_info)

      return contribution_data

    except Exception as e:
      print(f"    ❌ Error collecting contributors for {repo_full_name}: {str(e)}")
      return None


def main():
  parser = argparse.ArgumentParser(description='Collect GitHub contribution data')
  parser.add_argument('--input-file', required=True, help='Input repository data JSON file')
  parser.add_argument('--output-dir', required=True, help='Output directory')
  parser.add_argument('--date', required=True, help='Collection date')
  parser.add_argument('--max-contributors', type=int, default=20, help='Max contributors per repo')
  parser.add_argument('--max-repos', type=int, default=30, help='Max repositories to process')

  args = parser.parse_args()

  # Get GitHub token
  github_token = os.getenv('GITHUB_TOKEN')
  if not github_token:
    print("❌ GITHUB_TOKEN environment variable not set")
    sys.exit(1)

  # Load repository data
  input_file = Path(args.input_file)
  if not input_file.exists():
    print(f"❌ Input file not found: {input_file}")
    sys.exit(1)

  with open(input_file, 'r', encoding='utf-8') as f:
    repos_data = json.load(f)

  print(f"📊 Loaded {len(repos_data)} repositories")

  # Sort by stars and take top repositories to avoid rate limits
  top_repos = sorted(repos_data, key=lambda x: x.get('stars', 0), reverse=True)[:args.max_repos]
  print(f"🎯 Processing top {len(top_repos)} repositories by stars")

  # Initialize collector
  collector = GitHubContributionCollector(github_token)

  # Collect contribution data
  contribution_data = []
  for i, repo in enumerate(top_repos, 1):
    print(f"📈 Processing {i}/{len(top_repos)}: {repo['full_name']} ({repo['stars']} stars)")

    contrib_info = collector.collect_repository_contributors(
      repo['full_name'],
      args.max_contributors
    )

    if contrib_info:
      # Add repository metadata
      contrib_info.update({
        'organization': repo['organization'],
        'repo_name': repo['name'],
        'repo_stars': repo['stars'],
        'repo_forks': repo['forks'],
        'repo_language': repo['language'],
        'collection_date': args.date
      })
      contribution_data.append(contrib_info)

    # Rate limit management
    if i % 10 == 0:
      rate_limit = collector.github.get_rate_limit()
      print(f"    ⏱️  Rate limit: {rate_limit.core.remaining}/{rate_limit.core.limit}")

  # Save contribution data
  output_file = Path(args.output_dir) / f"contributions_{args.date}.json"
  output_file.parent.mkdir(parents=True, exist_ok=True)

  with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(contribution_data, f, indent=2, ensure_ascii=False, default=str)

  print(f"💾 Saved contribution data for {len(contribution_data)} repositories to {output_file}")


if __name__ == '__main__':
  main()
