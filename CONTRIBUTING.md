# Contributing to NCL Analytics DBT Project - OLIDS

## Prerequisites

Before contributing to this project, you'll need to set up the following on your Windows machine:

### SSH Key Setup

1. **Generate an SSH key**:
   ```bash
   ssh-keygen -t ed25519 -C "your.email@example.com"
   ```
   - When prompted, press Enter to accept the default file location
   - Enter a passphrase (recommended) or press Enter for no passphrase

2. **Add your SSH key to the ssh-agent**:
   ```bash
   # Start the ssh-agent
   eval $(ssh-agent -s)
   
   # Add your SSH private key
   ssh-add ~/.ssh/id_ed25519
   ```

3. **Add the SSH key to your GitHub account**:
   - Copy your public key: `cat ~/.ssh/id_ed25519.pub`
   - Go to GitHub Settings → SSH and GPG keys
   - Click "New SSH key"
   - Paste your public key and save

### Commit Signing Setup

This repository requires all commits to be signed. You have several options for setting this up:

#### Option 1: SSH Key Signing (Recommended - Simplest)

Since you've already set up an SSH key above, you can use it for signing commits. This is the simplest method and requires Git 2.34+:

1. **Check your Git version**:
   ```bash
   git --version
   ```
   If below 2.34, update Git for Windows from https://git-scm.com/download/win

2. **Configure Git to use SSH signing**:
   ```bash
   git config --global gpg.format ssh
   git config --global user.signingkey ~/.ssh/id_ed25519.pub
   git config --global commit.gpgsign true
   ```

3. **Create allowed signers file**:
   ```bash
   echo "$(git config --get user.email) $(cat ~/.ssh/id_ed25519.pub)" >> ~/.ssh/allowed_signers
   git config --global gpg.ssh.allowedSignersFile ~/.ssh/allowed_signers
   ```

4. **Add your SSH key for signing on GitHub**:
   - Go to GitHub Settings → SSH and GPG keys
   - Click "New SSH key"
   - Select "Signing Key" as the key type
   - Paste your public key and save

#### Option 2: GPG Signing (More complex)

1. **Install GPG**:
   - Download Gpg4win from https://www.gnupg.org/download/
   - Run the installer and follow the prompts
   - Restart your terminal after installation

2. **Generate a GPG key**:
   ```bash
   gpg --full-generate-key
   ```
   - Select RSA and RSA (default option 1)
   - Key size: 4096 bits
   - Key validity: Choose your preference (0 = key does not expire)
   - Enter your name and email (must match your GitHub email)
   - Create a strong passphrase

3. **Get your GPG key ID**:
   ```bash
   gpg --list-secret-keys --keyid-format LONG
   ```
   Look for the key ID after `sec rsa4096/` (e.g., `3AA5C34371567BD2`)

4. **Configure Git to use your GPG key**:
   ```bash
   git config --global user.signingkey YOUR_KEY_ID
   git config --global commit.gpgsign true
   git config --global gpg.program "C:/Program Files (x86)/GnuPG/bin/gpg.exe"
   ```

5. **Add your GPG key to GitHub**:
   - Export your public key: `gpg --armor --export YOUR_KEY_ID`
   - Go to GitHub Settings → SSH and GPG keys
   - Click "New GPG key"
   - Paste your entire public key (including the BEGIN/END lines) and save

6. **Configure GPG for Git Bash** (if using Git Bash):
   ```bash
   echo 'export GPG_TTY=$(tty)' >> ~/.bashrc
   source ~/.bashrc
   ```

#### Verifying Your Setup

Regardless of which method you choose, verify your setup:

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

#### Troubleshooting Signing Methods

**SSH Signing Issues**:
- Ensure Git version is 2.34 or higher
- The SSH key must be the same one added to GitHub

**GPG Issues**:
- If passphrase prompt doesn't appear, try: `gpg-connect-agent reloadagent /bye`
- For VS Code, add to settings.json: `"git.enableCommitSigning": true`

## Branch Protection Rules

This repository has branch protection rules in place to maintain code quality and security:

### Protected Branch: `main`

- **No direct commits**: All changes must go through a pull request (direct commits to main are disabled)
- **Require signed commits**: All commits must be cryptographically signed (SSH, GPG or S/MIME)
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

## Development Workflow

### Creating a Feature Branch

Never work directly on the main branch. Always create a new branch:

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

### Pre-commit Hooks

This project uses pre-commit hooks that will automatically:
- Validate commit message format (must follow Conventional Commits)
- Check for trailing whitespace
- Ensure files end with a newline
- Fix common formatting issues

The hooks run automatically when you commit. If a hook fails, fix the issue and try committing again.

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

## Windows-Specific Troubleshooting

### GPG Signing Issues

If you encounter "failed to sign the data" or "gpg failed to sign the data" errors:

1. **Ensure GPG is in your PATH**:
   ```bash
   where gpg
   ```
   Should return: `C:\Program Files (x86)\GnuPG\bin\gpg.exe`

2. **Test your GPG setup**:
   ```bash
   echo "test" | gpg --clearsign
   ```

3. **For Git Bash users**, add to `~/.bashrc`:
   ```bash
   export GPG_TTY=$(tty)
   ```

4. **For VS Code terminal**, you may need to:
   - Use Git Bash as your default terminal
   - Or use Windows Terminal instead of the integrated terminal

5. **Restart the GPG agent**:
   ```bash
   gpgconf --kill gpg-agent
   gpgconf --launch gpg-agent
   ```

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