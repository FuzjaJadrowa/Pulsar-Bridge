# Pulsar-Bridge
Simple yt-dlp bridge which guarantees the correct functioning of the [Pulsar](https://github.com/FuzjaJadrowa/Pulsar) application.
## Features
* Utilizes the direct Python API of `yt-dlp`, ensuring faster execution and superior error handling compared to traditional CLI wrapper.
* Communicates with the main application via structured JSON objects sent over `stdin`/`stdout`.
* Provides accurate, machine-readable reporting of download progress, speed, and ETA.
## Example
**Input (Command):**
```json
{
  "command": "download",
  "url": "https://www.youtube.com/watch?v=example",
  "args": {
    "audioOnly": false,
    "path": "/Downloads"
  }
}
```
**Output (Response):**
```json
{ "type": "progress", "percent": 45.2, "eta": 12, "speed": 1540000 }
{ "type": "finished", "success": true }
```
