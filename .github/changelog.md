# Build 16
- Added converting and compressing support.
- Moved download specified files to Download directory.
- Added new stable ffmpeg runner that runs ffmpeg using `better-ffmpeg-progress`.
- Added metadata support for local files. It gains data about duration, size, extension, category of file and much more.
- Stabilised connection between Pulsar and Pulsar Bridge.
- Added new libs to binary:
    * `hachoir` - for metadata from file.
    * `Pillow` - for image converting support.
    * `pillow-heif` - for HEIF support for image converting.
    * `py7zr` - for 7Z support for archive converting.
    * `rarfile` - for archive converting support.
    * `fonttools` - for font converting support.
    * `brotli` - for WOFF2 support for font converting.
    * `better-ffmpeg-progress` - for better utilising and reading progress from ffmpeg binary.
- Bumped yt-dlp version to `2026.3.17`.