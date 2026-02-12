# Pulsar-Bridge
Simple yt-dlp bridge which guarantees the correct functioning of the [Pulsar](https://github.com/FuzjaJadrowa/Pulsar) application.
## Features
* Utilizes the direct Python API of `yt-dlp`, ensuring faster execution and superior error handling compared to traditional CLI wrapper.
* Communicates with the main application via structured JSON objects sent over `stdin`/`stdout`.
* Provides accurate, machine-readable reporting of download progress, speed, and ETA.
* Allows you to use the `metadata` command to obtain the most important information about the selected video.
* Allows multiple downloads at once using the ID system.
## Examples
**Input for download:**
```json
{
  "command": "download",
  "id": "example123",
  "args":
  [
    "https://www.youtube.com/watch?v=example",
    "-f",
    "bestvideo+bestaudio/best"
  ]
}
```
**Input for metadata:**
```json
{
  "command": "metadata",
  "id": "example123",
  "args": ["https://www.youtube.com/watch?v=example"]
}
```
**Output for download:**
```json
{ "type": "progress", "percent": 45.2, "eta": 12, "speed": 1540000 }
{ "type": "finished", "success": true }
```
**Output for metadata:**
```json
{
  "type": "metadata",
  "id": "example123",
  "success": true,
  "data": {
    "id": "example",
    "title": "Example Title for YouTube video!",
    "..."
  }
}
```