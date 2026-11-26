# LAN Drop

Discover other devices on your local network and send files and text between them, right from the browser.

## Run

Requires Node.js 18+. No dependencies.

```sh
node server.js
```

The server prints addresses like `http://192.168.0.105:8080`. Open one on every phone, tablet or computer on the same network; they appear in each other's device list.

Set a different port with `PORT=9000 node server.js`.

## Features

- Live list of connected devices (each gets a random name you can rename)
- Send files by button or drag-and-drop, with upload progress
- Send text, or paste (Ctrl/⌘+V) anywhere to send clipboard contents
- Received items: download, image preview, copy text, open links
- Download all received files **as a zip** (built on the fly, up to 4 GB) or **separately**, or **Clear all**
- **Speed test**: ping, download and upload between this device and the host computer
- Files are relayed through a temp folder on the host and removed after 30 minutes, on dismiss, or when the server stops

## How it works

Browsers can't scan a network themselves, so one machine runs `server.js`. Each browser that opens it registers over Server-Sent Events; the server broadcasts the device list and relays text and file uploads to the chosen recipient.

## Security

There is no authentication or encryption: anyone on the same network who opens the address can see and send to connected devices. Use it on networks you trust. On Windows, allow Node through the firewall for private networks when prompted.
