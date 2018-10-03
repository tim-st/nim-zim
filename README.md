**Installation**
---
```
nimble install https://github.com/tim-st/nim-zim
```

**Webserver**
---
* Download a ZIM file from the [Kiwix page](https://download.kiwix.org/zim/)
* In the following we use `wikipedia_de_all_nopic_2018-05.zim`
* Compile the http server using [Nim >= 0.19.0](https://nim-lang.org/install.html):
  ```
  nim c -d:release zim.nim
  ```
* Change to the directory where the ZIM file exists.
  You will need the `liblzma` library installed on your system. If you're on Windows, download the file `liblzma.dll` from [here](https://tukaani.org/xz/) and place it in the folder next to the `zim` binary.
* Start the server (setting the port is optional and defaults to 8080)
  ```
  zim server --filename=wikipedia_de_all_nopic_2018-05.zim --port=8081
  ```
* You should see
  ```
  Serving ZIM file at http://localhost:8081/kiwix.wikipedia_de_all/A/Wikipedia:Hauptseite.html
  Wikipedia
  aus Wikipedia, der freien Enzyklopädie
  2018-05-28
  Press CTRL-C to stop the server.
  ```
* You can now visit the page using your webbrowser.
  Also just http://localhost:8081 will work and redirect you to the main page.

* If you want to search an article you can put the search word behind `/A/` like    
  http://localhost:8081/kiwix.wikipedia_de_all/A/Berlin

  You will be redirected to a similiar search result.

**API**
---
Nim programmers can import `zim` and get information like title, url or html data 
from a `ZimFile` object:
```nim
import zim

let reader = newZimFileReader("wikipedia_de_all_nopic_2018-05.zim")
echo reader.getTitle
echo reader.getDescription
echo reader.getDate

for entry in reader.entriesSortedByNamespace(namespaceArticles, limit=100):
  echo entry.kind
  echo entry.title
  echo entry.url
  # let html = reader.readBlob(entry)
```