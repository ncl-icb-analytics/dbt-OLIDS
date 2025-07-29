# Contributing to NCL Analytics DBT Project - OLIDS

When working with this project please remember this repository:
1. requires all commits to be signed 
2. has branch protection rules in place to maintain code quality and security
3. uses pre-commit hooks that will automatically process and/or reject commits

## 1. SSH key setup for signing commits

### Setup
Before contributing to this project, you'll need to set up the following on your Windows machine.

1. **Generate an SSH key**:
   ```bash
   ssh-keygen -t ed25519 -C "your.email@nhs.net"
   ```
   - When prompted, press Enter to accept the default file location
   - Enter a passphrase (recommended). You will need to remember this passphrase and use it for commits.

2. **Configure Git to use SSH signing**:
   ```bash
   git config --global gpg.format ssh
   git config --global user.signingkey ~/.ssh/id_ed25519.pub
   git config --global commit.gpgsign true
   ```

3. **Add the SSH key to your GitHub account as a signing key**:
   - Copy your public key onto your clipboard: `clip < ~/.ssh/id_ed25519.pub`
   - Go to GitHub Settings → SSH and GPG keys
   - Click "New SSH key"
   - Paste your public key
   - Select "Signing Key" as the key type
   - Paste your public key and save

*Note - this section has omitted adding the SSH key to your SSH key agent to bypass admin requirements and setting up SSH keys for cloning as HTTPS is currently widely used. GPG keys are also an option but have not been explicitly outlined - please speak to the head of data science or engineering if you with to use GPG keys or clone via SSH*

### Verifying Your Setup

Verify your setup:

1. **Create a test commit**:
   ```bash
   git commit --allow-empty -m "test: verify signed commits"
   ```

2. **Verify the signature**:
   ```bash
   git log --show-signature -1
   ```

3. **Check on GitHub**:
   - Push your commit
   - View on GitHub - it should show "Verified" badge

### Troubleshooting Signing Methods

**SSH Signing Issues**:
- Ensure Git version is 2.34 or higher
- The SSH key must be the same one added to GitHub

1. **Check your Git version**:
   ```bash
   git --version
   ```
   If below 2.34, update Git for Windows from https://git-scm.com/download/win

## 2. Branch Protection Rules

This repository has branch protection rules in place to maintain code quality and security:

### Protected Branch: `main`

- **No direct commits**: All changes must go through a pull request (direct commits to main are disabled)
- **Require signed commits**: All commits must be cryptographically signed (SSH, please discuss if GPG or S/MIME preferred)
- **Include administrators**: These rules apply to everyone, including repository administrators

### Additional Security Measures

- **Force pushes disabled**: Cannot force push to the main branch, preventing history rewriting
- **Force deletions disabled**: Cannot delete the main branch, protecting against accidental removal
- **Linear history enforced**: Ensures a clean commit history through rebasing before merge

These rules ensure that:
- All code changes go through a pull request process
- The commit history remains intact and auditable
- All commits can be verified as coming from trusted contributors
- The main branch is protected from accidental or malicious changes

### Creating a Feature Branch

Never work directly on the main branch. Always create a new branch. This can be done following [VScode IDE guidance](https://code.visualstudio.com/docs/sourcecontrol/overview) or via git bash:

```bash
# Create and switch to a new feature branch
git switch -c feature/your-feature-name

# Or for bug fixes
git switch -c fix/your-bug-fix

# Or for documentation
git switch -c docs/your-doc-update
```

### Commit Message Format

We follow the [Conventional Commits](https://www.conventionalcommits.org/) specification. Commit messages should be structured as:

```
<type>: <description>

[optional body]

[optional footer(s)]
```

**Types**:
- `feat`: A new feature
- `fix`: A bug fix
- `docs`: Documentation only changes
- `style`: Changes that don't affect code meaning (formatting, etc.)
- `refactor`: Code change that neither fixes a bug nor adds a feature
- `test`: Adding or correcting tests
- `chore`: Changes to build process or auxiliary tools
- `perf`: Performance improvements
- `ci`: Changes to CI configuration

**Examples**:
```bash
git commit -m "feat: add diabetes triple target measure"
git commit -m "fix: correct BMI calculation in int_bmi_latest"
git commit -m "docs: update README with new setup instructions"
git commit -m "chore: update dbt dependencies to latest version"
```

### Creating a Pull Request

1. **Push your branch**:
   ```bash
   git push -u origin feature/your-feature-name
   ```

2. **Create a pull request on GitHub**:
   - Go to the repository on GitHub
   - Click "Pull requests" → "New pull request"
   - Select your branch
   - Fill in the PR template
   - Reference any related issues (e.g., "Fixes #123" or "Closes #123")

3. **PR Guidelines**:
   - Provide a clear description of changes
   - Include test results if applicable (`dbt test` output)
   - Ensure all checks pass
   - Request review from appropriate team members

## 3. Pre-commit Hooks

This project uses pre-commit hooks that will automatically:
- Validate commit message format (must follow Conventional Commits)
- Check for trailing whitespace
- Ensure files end with a newline
- Fix common formatting issues

The hooks run automatically when you commit. If a hook fails, fix the issue and try committing again.

## Windows-Specific Troubleshooting

### SSH Connection Issues

If you can't connect via SSH on Windows:

1. **Test your SSH connection**:
   ```bash
   ssh -T git@github.com
   ```
   You should see: "Hi username! You've successfully authenticated..."

2. **Check SSH agent is running**:
   ```bash
   ssh-add -l
   ```

3. **If using PowerShell**, you may need to start ssh-agent differently:
   ```powershell
   Start-Service ssh-agent
   ```

4. **Ensure correct permissions on SSH files**:
   - Your `~/.ssh` directory should only be accessible by you
   - Private key files should have restricted permissions

### Line Ending Issues

Windows uses different line endings than Unix systems. Configure Git to handle this:

```bash
git config --global core.autocrlf true
```

## Environment Setup Reminder

Don't forget to also set up your development environment as per the main README:

1. **Python virtual environment**:
*note - this will only work if python is part of the PATH, if this is has not been set up either add python to PATH (admin required) or switch to py notation (examples in the best practices documentation)*
   ```bash
   python -m venv venv
   venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. **Snowflake credentials**:
   ```bash
   cp env.example .env
   # Edit .env with your credentials
   ```

3. **Run the dbt setup script**:
   ```bash
   .\start_dbt.ps1
   ```

## Getting Help

If you encounter issues:
- Check existing [GitHub Issues](https://github.com/ncl-icb-analytics/dbt-olids/issues)
- Create a new issue with the appropriate label