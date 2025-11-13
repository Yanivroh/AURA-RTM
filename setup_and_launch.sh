#!/bin/bash

# Install required system dependencies
echo "Installing system dependencies..."
brew install postgresql@14

# Add PostgreSQL to PATH
echo 'export PATH="/opt/homebrew/opt/postgresql@14/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc

# Install Python dependencies
echo "Installing Python packages..."
pip3 install --upgrade pip
pip3 install -r requirements.txt

# Create desktop launcher
LAUNCHER_PATH="$HOME/Desktop/Aura Dashboard.command"
echo '#!/bin/bash
cd "$(dirname "$0")"
cd /Users/yaniv.rohberg/CascadeProjects/windsurf-project-3
source venv/bin/activate
streamlit run aura_dashboard.py' > "$LAUNCHER_PATH"

# Make the launcher executable
chmod +x "$LAUNCHER_PATH"

echo "\n✅ Setup complete!"
echo "You can now launch the Aura Dashboard by double-clicking the 'Aura Dashboard' icon on your desktop."
