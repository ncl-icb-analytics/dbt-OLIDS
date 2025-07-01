# GitHub Actions Workflows

This directory contains automated workflows that help manage the repository.

## Workflows

### auto-label-urgency.yml
**Trigger:** Issues opened or edited
**Purpose:** Automatically applies urgency labels (`urgent`, `critical`) to issues based on form responses

**How it works:**
- Scans issue body for urgency form responses
- Removes existing urgency labels
- Applies new urgency label based on form selection

### copy-issue-labels-to-pr.yml
**Trigger:** Pull requests opened, edited, or synchronized
**Purpose:** Automatically copies labels from referenced issues to the pull request and updates issue status

**How it works:**
- Scans PR body for issue references (e.g., "Closes #123", "Fixes #456")
- Fetches labels from referenced issues
- Applies those labels to the PR (without duplicating existing labels)
- Assigns referenced issues to the PR author
- Adds "Code Review" label to referenced issues
- Handles multiple issue references
- Continues working even if some issues can't be fetched

### auto-move-assigned-issues.yml
**Trigger:** Issues assigned to users
**Purpose:** Automatically moves assigned issues to "In Progress" status

**How it works:**
- Triggers when an issue is assigned to someone
- Removes existing status labels (In Progress, Code Review, Done)
- Adds "In Progress" label to the assigned issue
- Provides task board automation

**Supported issue reference formats:**
- `Closes #123`
- `Fixes #456`
- `Resolves #789`
- `Closes https://github.com/owner/repo/issues/123`
- And other GitHub-recognised keywords

## Benefits

These workflows help maintain consistency in labeling and automate project management:
- **Consistency:** PRs inherit the same categorisation as their related issues
- **Automation:** No manual effort required to copy labels or update issue status
- **Organisation:** Better filtering and searching of PRs by label
- **Tracking:** Easier to see what types of changes are in PRs
- **Project Management:** Automatic task board updates based on issue assignment and PR creation
- **Status Tracking:** Clear progression from assignment → In Progress → Code Review → Done