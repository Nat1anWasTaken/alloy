#!/bin/bash
# Simple WebSocket test client

DOC_ID=$(uuidgen | tr '[:upper:]' '[:lower:]')
URL="ws://localhost:3000/ws/${DOC_ID}?user=test"

echo "Connecting to: $URL"
echo "Press Ctrl+C to exit"
echo ""

# Use websocat if available, otherwise show instructions
if command -v websocat &> /dev/null; then
    websocat "$URL"
else
    echo "websocat not found. Install it with:"
    echo "  brew install websocat"
    echo "  or"
    echo "  cargo install websocat"
    echo ""
    echo "Then run: websocat \"$URL\""
fi
