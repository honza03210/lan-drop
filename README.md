# LAN Drop

Discover other devices on your local network and send files, folders and text between them, right from the browser.

## Run

Requires Node.js 18+. No dependencies.

```sh
node server.js
```

The server prints addresses like `http://192.168.0.105:8080`. Open one on every phone, tablet or computer on the same network; they appear in each other's device list.

Set a different port with `PORT=9000 node server.js`.

## Features

- Live list of connected devices (each gets a random name you can rename)
- Send files or whole folders by button or drag-and-drop; send text, or paste (Ctrl/⌘+V) to send clipboard contents
- Receive: download one file, everything **as a zip** (folders kept, no size limit), or **separately**; decline or clear
- Live progress, speed and time left on the sending side
- **Speed test**: ping, download and upload between this device and the host computer

## How it works

Browsers can't scan a network themselves, so one machine runs `server.js`. Each browser that opens it registers over Server-Sent Events and the server broadcasts the device list.

Files are **never stored on the host**. Sending a file only announces it. When the receiver clicks Download, the server asks the sender's page for exactly those bytes and pipes them straight through to the receiver:

```
receiver  --GET /api/dl/...-->  server  --"pull" {offset}-->  sender's page
receiver  <------ bytes ------  server  <--- POST /api/push --  sender's page
```

This means:

- **No copies and no disk use on the host**, and memory use stays flat whatever the size: everything is streamed with backpressure.
- **Hiccups are survived.** If the sender's connection drops, the server waits and asks it to continue from the last byte that arrived. A device that reconnects (Wi-Fi blip, page reload on the receiving side) keeps its identity and its pending offers.
- **Downloads of single files can be resumed** (the server supports `Range` requests), for example from the browser's download list after a network error.
- **Zips are built on the fly** in ZIP64 format, so there's no 4 GB limit, and they keep folder structure. A zip download can't be resumed if the *receiver* drops; download big files separately if that matters.

### Limits

- **The sender's page must stay open** (and the device awake) until the receiver has downloaded everything; the page warns before closing. If it stays away for 10 minutes, its offers are cancelled.
- "Download separately" is limited to 50 files and can't recreate folders; use the zip for more.
- Restarting the server forgets all devices and offers.

## Security

- There is **no encryption** (plain HTTP): anyone able to capture traffic on your network can read what is sent. Use it on networks you trust.
- There is **no password**: anyone on the same network who opens the address can see device names and offer files. Nothing is downloaded unless the receiver clicks Download, and pasted files always ask for a recipient first.
- Each tab has a public ID and a private secret; requests must carry the secret, so devices can't impersonate each other or download offers meant for someone else.
- On Windows, allow Node through the firewall for private networks when prompted.
