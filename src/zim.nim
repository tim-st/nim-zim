import asyncdispatch
import asynchttpserver
import md5
import random
import streams
import strformat
import strutils
import tables
import uri
import zim/lzma

## This module implements a ZIM file reader and an HTTP server
## for browsing the ZIM file.
## For details see: http://www.openzim.org/wiki/ZIM_file_format

# FIXME: increase `limit` to the number of real entries,
# where `limit` is used as parameter. 

const
  magicNumberZimFormat = 72173914u32
  noMainPage = uint32.high
  noLayoutPage = noMainPage

type ZimUuid* = array[16, uint8]

type DirectoryEntryKind* = enum
  ArticleEntry
  RedirectEntry
  LinkTarget
  DeletedEntry

type ZimMimetype = uint16

const
  DeletedEntryValue: ZimMimetype = 0xFFFD
  LinkTargetValue: ZimMimetype = 0xFFFE
  RedirectEntryValue: ZimMimetype = 0xFFFF

type ZimHeader* = object
  magicNumber: uint32 # Magic number to recognise the file format, must be 72173914
  majorVersion: uint16 # Major version of the ZIM file format (5 or 6)
  minorVersion: uint16 # Minor version of the ZIM file format
  uuid: ZimUuid # unique id of this zim file
  articleCount: uint32 # total number of articles
  clusterCount: uint32 # total number of clusters
  urlPtrPos: uint64 # position of the directory pointerlist ordered by URL
  titlePtrPos: uint64 # position of the directory pointerlist ordered by Title
  clusterPtrPos: uint64 # position of the cluster pointer list
  mimeListPos: uint64 # position of the MIME type list (also header size)
  mainPage: uint32 # main page or 0xffffffff if no main page
  layoutPage: uint32 # layout page or 0xffffffff if no layout page
  checksumPos: uint64 # pointer to the md5checksum of this file without the checksum itself. This points always 16 bytes before the end of the file.

type ZimFile* = object
  filename: string
  header: ZimHeader
  metadata*: Table[string, string]
  mimetypeList*: seq[string]
  stream: FileStream

proc len*(z: ZimFile): int = z.header.articleCount.int
proc filesize*(z: ZimFile): int = z.header.checksumPos.int + 16
proc uuid*(z: ZimFile): ZimUuid = z.header.uuid
proc close*(z: ZimFile) = z.stream.close()

proc readHeader(z: var ZimFile) =
  z.stream.setPosition(0)
  z.header.magicNumber = z.stream.readUint32
  doAssert z.header.magicNumber == magicNumberZimFormat
  z.header.majorVersion = z.stream.readUint16
  z.header.minorVersion = z.stream.readUint16
  doAssert z.stream.readData(addr(z.header.uuid[0]), sizeof(ZimUuid)) == sizeof(ZimUuid)
  z.header.articleCount = z.stream.readUint32
  z.header.clusterCount = z.stream.readUint32
  z.header.urlPtrPos = z.stream.readUint64
  z.header.titlePtrPos = z.stream.readUint64
  z.header.clusterPtrPos = z.stream.readUint64
  z.header.mimeListPos = z.stream.readUint64
  z.header.mainPage = z.stream.readUint32
  z.header.layoutPage = z.stream.readUint32
  z.header.checksumPos = z.stream.readUint64

proc readNullTerminated(s: Stream): string =
  while true:
    let c = s.readChar
    if c == '\0': break
    result.add(c)

proc readMimeTypeList(z: var ZimFile) =
  z.mimetypeList = newSeqOfCap[string](32)
  z.stream.setPosition(z.header.mimeListPos.int)
  while true:
    let mimetype = z.stream.readNullTerminated()
    if mimetype.len == 0: break
    z.mimetypeList.add(mimeType.toLowerAscii.strip)

const
  namespaceLayout* = '-'
  namespaceArticles* = 'A'
  namespaceArticleMetadata* = 'B'
  namespaceImagesFiles* = 'I'
  namespaceImagesText* = 'J'
  namespaceZimMetadata* = 'M'
  namespaceCategoriesText* = 'U'
  namespaceCategoriesArticleList* = 'V'
  namespaceCategoriesPerArticleCategoryList* = 'W'
  namespaceFulltextIndex* = 'X'

type DirectoryEntry* = object
  mimetype*: ZimMimetype # MIME type number as defined in the MIME type list
  parameterLen: byte # (not used) length of extra paramters
  namespace*: char # defines to which namespace this directory entry belongs
  revision*: uint32 # (optional) identifies a revision of the contents of this directory entry, needed to identify updates or revisions in the original history
  case kind*: DirectoryEntryKind # MIME type number as defined in the MIME type list
  of ArticleEntry:
    clusterNumber*: uint32 # cluster number in which the data of this directory entry is stored
    blobNumber*: uint32 # blob number inside the compress cluster where the contents are stored
  of RedirectEntry:
    redirectIndex*: uint32 # pointer to the directory entry of the redirect target
  of DeletedEntry, LinkTarget: discard # no extra fields
  url*: string # string with the URL as refered in the URL pointer list
  title*: string # string with an title as refered in the Title pointer list or empty; in case it is empty, the URL is used as title
  parameter*: string # (not used) extra parameters; see `parameterLen`

proc contentType*(z: ZimFile, entry: DirectoryEntry): string =
  if entry.mimetype.int in 0..<z.mimetypeList.len:
    result = z.mimetypeList[entry.mimetype.int]

proc isHtmlDocument*(z: ZimFile, entry: DirectoryEntry): bool =
  z.contentType(entry) == "text/html"

proc urlPointerAtPos(z: ZimFile, pos: Natural): uint64 =
  z.stream.setPosition(z.header.urlPtrPos.int+pos*8)
  result = z.stream.readUint64

proc titlePointerAtPos(z: ZimFile, pos: Natural): uint64 =
  z.stream.setPosition(z.header.titlePtrPos.int+pos*4)
  result = z.urlPointerAtPos(z.stream.readUint32)

proc clusterPointerAtPos(z: ZimFile, pos: Natural): uint64 =
  z.stream.setPosition(z.header.clusterPtrPos.int+pos*8)
  result = z.stream.readUint64

proc readDirectoryEntry*(z: ZimFile, position: uint64, followRedirects = true): DirectoryEntry =
  z.stream.setPosition(position.int)
  result.mimetype = z.stream.readUint16
  result.parameterLen = z.stream.readUint8
  result.namespace = z.stream.readChar
  result.revision = z.stream.readUint32
  case result.mimetype
  of DeletedEntryValue:
    result.kind = DeletedEntry
  of LinkTargetValue:
    result.kind = LinkTarget
  of RedirectEntryValue:
    result.kind = RedirectEntry
    result.redirectIndex = z.stream.readUint32
    if followRedirects:
      return z.readDirectoryEntry(z.urlPointerAtPos(result.redirectIndex), true)
  else:
    result.kind = ArticleEntry
    result.clusterNumber = z.stream.readUint32
    result.blobNumber = z.stream.readUint32
  result.url = z.stream.readNullTerminated
  result.title = z.stream.readNullTerminated
  if unlikely(result.parameterLen.int > 0):
    result.parameter = z.stream.readStr(result.parameterLen.int)

proc isArticle*(entry: DirectoryEntry): bool =
  result = entry.kind == ArticleEntry and entry.namespace == namespaceArticles

proc isRedirect*(entry: DirectoryEntry): bool =
  result = entry.kind == RedirectEntry

proc followRedirect*(z: ZimFile, redirectIndex: int): DirectoryEntry =
  result = z.readDirectoryEntry(z.urlPointerAtPos(redirectIndex), true)

proc followRedirect*(z: ZimFile, entry: DirectoryEntry): DirectoryEntry =
  if likely(entry.isRedirect): z.followRedirect(entry.redirectIndex.int)
  else: entry

iterator entriesSortedByUrl*(z: ZimFile, reverse = false, limit = -1): DirectoryEntry =
  if reverse:
    let l = if limit > 0 and limit < z.len: z.len-1-limit else: 0
    for x in countdown(z.len-1, l):
      yield z.readDirectoryEntry(z.urlPointerAtPos(x), false)
  else:
    let l = if limit > 0: min(limit, z.len-1) else: z.len-1
    for x in countup(0, l):
      yield z.readDirectoryEntry(z.urlPointerAtPos(x), false)

iterator entriesSortedByNamespace*(z: ZimFile, namespace: char, limit = -1): DirectoryEntry =
  let reverse = namespace > namespaceArticles
  if reverse:
    for entry in z.entriesSortedByUrl(true, limit):
      if entry.namespace < namespace: break
      if entry.namespace == namespace:
        yield entry
  else:
    for entry in z.entriesSortedByUrl(false, limit):
      if entry.namespace > namespace: break
      if entry.namespace == namespace:
        yield entry

iterator entriesSortedByTitle*(z: ZimFile, limit = -1): DirectoryEntry =
  let l = if limit > 0: min(limit, z.len-1) else: z.len-1
  for x in 0..l:
    yield z.readDirectoryEntry(z.titlePointerAtPos(x), false)

proc linearSearchImpl(z: ZimFile, namespace: char, candidate: string, searchTitle: static[bool], limit = -1):
    tuple[entry: DirectoryEntry, success: bool] =
  # Runtime: O(n)
  for entry in z.entriesSortedByNamespace(namespace, limit):
    result.entry = entry
    if (when searchTitle: result.entry.title else: result.entry.url) == candidate:
      result.success = true
      break

proc linearSearchByUrl*(z: ZimFile, url: string, namespace = namespaceArticles, limit = -1):
    tuple[entry: DirectoryEntry, success: bool] =
  result = z.linearSearchImpl(namespace, url, false, limit)

proc linearSearchByTitle*(z: ZimFile, title: string, namespace = namespaceArticles, limit = -1):
    tuple[entry: DirectoryEntry, success: bool] =
  result = z.linearSearchImpl(namespace, title, true, limit)

proc binarySearchImpl(z: ZimFile, namespace: char, candidate: string, searchTitle: static[bool]):
    tuple[entry: DirectoryEntry, success: bool] =
  # Runtime: O(log_2(n))
  var firstUrlPosition = 0
  var lastUrlPosition = z.len - 1
  while firstUrlPosition <= lastUrlPosition:
    let middleUrlPosition = firstUrlPosition + ((lastUrlPosition - firstUrlPosition) div 2)
    result.entry = z.readDirectoryEntry(z.urlPointerAtPos(middleUrlPosition), followRedirects = false)
    var c = cmp(result.entry.namespace, namespace)
    if c == 0:
      c = cmp(when searchTitle: result.entry.title else: result.entry.url, candidate)
      if c == 0:
        result.success = true
        break
    if c < 0: firstUrlPosition = middleUrlPosition + 1
    else: lastUrlPosition = middleUrlPosition - 1

proc binarySearchByUrl*(z: ZimFile, url: string, namespace = namespaceArticles):
    tuple[entry: DirectoryEntry, success: bool] =
  result = z.binarySearchImpl(namespace, url, false)

proc binarySearchByTitle*(z: ZimFile, title: string, namespace = namespaceArticles):
    tuple[entry: DirectoryEntry, success: bool] =
  result = z.binarySearchImpl(namespace, title, true)

proc readDirectoryEntry*(z: ZimFile, url: string, namespace = namespaceArticles, followRedirects = true):
    tuple[entry: DirectoryEntry, success: bool] =
  result = z.binarySearchByUrl(url, namespace)
  if result.success and followRedirects and result.entry.isRedirect:
    result.entry = z.readDirectoryEntry(z.urlPointerAtPos(result.entry.redirectIndex), true)

proc readDirectoryEntryByTitle*(z: ZimFile, title: string, namespace = namespaceArticles, followRedirects = true):
    tuple[entry: DirectoryEntry, success: bool] =
  result = z.binarySearchByTitle(title, namespace)
  if result.success and followRedirects and result.entry.isRedirect:
    result.entry = z.readDirectoryEntry(z.urlPointerAtPos(result.entry.redirectIndex), true)

proc containsTitle*(z: ZimFile, title: string, namespace = namespaceArticles): bool =
  result = z.binarySearchByTitle(title, namespace).success

proc containsUrl*(z: ZimFile, url: string, namespace = namespaceArticles): bool =
  result = z.binarySearchByUrl(url, namespace).success

proc contains*(z: ZimFile, title: string, namespace = namespaceArticles): bool =
  result = containsTitle(z, title, namespace)

proc internalChecksum*(z: ZimFile): MD5Digest =
  z.stream.setPosition(z.header.checksumPos.int)
  doAssert z.stream.readData(addr(result[0]), 16) == 16

proc toMD5(s: Stream, length: Positive, blockSize: static[Positive]): MD5Digest =
  var
    context: MD5Context
    buffer = newString(blockSize)
  md5Init(context)
  var i = 0
  while i < length:
    let chunkSize = if i + blockSize < length: blockSize else: length - i
    if unlikely(chunkSize <= 0): break
    let bytesRead = s.readData(addr(buffer[0]), chunkSize)
    if unlikely(bytesRead != chunkSize): break
    md5Update(context, buffer, bytesRead)
    inc(i, bytesRead)
  md5Final(context, result)

proc calculatedChecksum*(z: ZimFile): MD5Digest =
  z.stream.setPosition(0)
  result = z.stream.toMD5(z.filesize-16, 2 shl 20)

proc matchesChecksum*(z: ZimFile): bool =
  z.internalChecksum == z.calculatedChecksum

proc readBlobAt*(z: ZimFile, clusterPosition, blobPosition: Natural): string =
  # TODO: support cluster caching?
  let thisClusterPointer = z.clusterPointerAtPos(clusterPosition)
  z.stream.setPosition(thisClusterPointer.int)
  let clusterInformation = z.stream.readUint8
  let isExtended = (clusterInformation and 0b0001_0000) == 0b0001_0000
  let offsetSize = if isExtended: 8 else: 4
  let clusterCompression = clusterInformation and 0b0000_1111 
  case clusterCompression
  of 0, 1:
    let thisBlobIndex = thisClusterPointer.int + 1 + blobPosition * offsetSize
    z.stream.setPosition(thisBlobIndex)
    let (thisBlobPointer, nextBlobPointer) = if likely(offsetSize == 4):
      (z.stream.readUint32.int, z.stream.readUint32.int)
    else:
      (z.stream.readUint64.int, z.stream.readUint64.int)
    let blobLen = nextBlobPointer - thisBlobPointer
    z.stream.setPosition(thisClusterPointer.int + 1 + thisBlobPointer)
    result = z.stream.readStr(blobLen)
  of 4:
    let nextClusterPointer = if unlikely(clusterPosition == z.header.clusterCount.int-1):
      z.header.checksumPos-1
    else:
      z.clusterPointerAtPos(clusterPosition+1)
    let clusterLen = nextClusterPointer - thisClusterPointer - 1
    z.stream.setPosition(thisClusterPointer.int+1)
    try:
      var clusterData = lzma.decompress(z.stream.readStr(clusterLen.int))
      let thisBlobIndex = blobPosition * offsetSize
      let nextBlobIndex = thisBlobIndex + offsetSize
      let (thisBlobPointer, nextBlobPointer) = if likely(offsetSize == 4):
        (
          int(cast[ptr uint32](addr(clusterData[thisBlobIndex]))[]),
          int(cast[ptr uint32](addr(clusterData[nextBlobIndex]))[])
        )
      else:
        (
          int(cast[ptr uint64](addr(clusterData[thisBlobIndex]))[]),
          int(cast[ptr uint64](addr(clusterData[nextBlobIndex]))[])
        )
      result = clusterData[thisBlobPointer..<nextBlobPointer]
    except:
      when not defined(release):
        echo fmt"Decompressing clusterData of length {clusterLen} failed at position {thisClusterPointer.int+1}."
        echo getCurrentExceptionMsg()
  else:
    # Return empty string
    when not defined(release):
      echo fmt"Unsupported cluster compression: {clusterCompression}"

proc readBlob*(z: ZimFile, entry: DirectoryEntry): string =
  assert entry.kind == ArticleEntry
  result = z.readBlobAt(entry.clusterNumber, entry.blobNumber)

proc hasMainPage*(z: ZimFile): bool = z.header.mainPage != noMainPage
proc hasLayoutPage*(z: ZimFile): bool = z.header.layoutPage != noLayoutPage
proc mainPage*(z: ZimFile): DirectoryEntry

proc randomArticleEntry*(z: ZimFile): DirectoryEntry =
  randomize()
  var tries = 0
  while tries < 20:
    let randomPosition = rand(z.len-1)
    result = z.readDirectoryEntry(z.urlPointerAtPos(randomPosition))
    if result.isArticle and result.title.len > 0: return
    inc(tries)
  if likely(z.hasMainPage): result = z.mainPage

proc mainPage*(z: ZimFile): DirectoryEntry =
  if likely(z.hasMainPage): z.followRedirect(z.header.mainPage.int)
  else: z.randomArticleEntry

proc layoutPage*(z: ZimFile): DirectoryEntry =
  if likely(z.hasLayoutPage): z.followRedirect(z.header.layoutPage.int)
  else: z.mainPage

proc readMetadata(z: var ZimFile) =
  z.metadata = initTable[string, string](16)
  for entry in z.entriesSortedByNamespace(namespaceZimMetadata):
    z.metadata[entry.url] = z.readBlob(entry)

proc getFavicon*(z: ZimFile): DirectoryEntry =
  let r = z.readDirectoryEntry("favicon", namespaceLayout, true)
  result = r.entry

proc getMetadata*(z: ZimFile, key: string): string = z.metadata.getOrDefault(key)

proc getName*(z: ZimFile): string = 
  result = z.getMetadata("Name").strip
  if result.len == 0: result = $z.uuid

proc getTitle*(z: ZimFile): string = z.getMetadata("Title")
proc getCreator*(z: ZimFile): string = z.getMetadata("Creator")
proc getPublisher*(z: ZimFile): string = z.getMetadata("Publisher")
proc getDate*(z: ZimFile): string = z.getMetadata("Date")
proc getDescription*(z: ZimFile): string = z.getMetadata("Description")
proc getLongDescription*(z: ZimFile): string = z.getMetadata("LongDescription")
proc getLanguage*(z: ZimFile): string = z.getMetadata("Language")
proc getLicense*(z: ZimFile): string = z.getMetadata("License")
proc getTags*(z: ZimFile): string = z.getMetadata("Tags")
proc getRelation*(z: ZimFile): string = z.getMetadata("Relation")
proc getSource*(z: ZimFile): string = z.getMetadata("Source")
proc getCounter*(z: ZimFile): string = z.getMetadata("Counter")

proc openZimFile*(filename: string): ZimFile =
  result.filename = filename
  result.stream = newFileStream(filename, fmRead)
  result.readHeader()
  result.readMimeTypeList()
  result.readMetadata()

proc startZimHttpServer*(filename: string, port: uint16 = 8080) = 
  
  const maxAge = 87840
  let zf = openZimFile(filename)
  let zimName = decodeUrl(zf.getName)
  let zimNameLen = zimName.len
  let urlMainpage = zf.mainPage.url

  proc redirectTo(req: Request, namespace: char, url: string) {.async.} =
    let headers = newHttpHeaders({
      "Cache-Control": fmt"max-age={maxAge}, must-revalidate",
      "Location": fmt"/{zimName}/{namespace}/{url}"
    })
    await req.respond(Http301, "", headers)

  proc redirectToMainpage(req: Request) {.async.} =
    let headers = newHttpHeaders({
      "Cache-Control": "no-store",
      "Location": fmt"/{zimName}/{namespaceArticles}/{urlMainpage}"
    })
    await req.respond(Http301, "", headers)

  proc responseOk(req: Request, entry: DirectoryEntry) {.async.} =
    let headers = newHttpHeaders({
      "Content-Type": zf.contentType(entry),
      "Cache-Control": fmt"max-age={maxAge}, must-revalidate",
      "Connection": "Close"
    })
    await req.respond(Http200, zf.readBlob(entry), headers)
  
  proc handleRequest(req: Request) {.async.} =
    let path = req.url.path
    var decodedPath: string
    try: decodedPath = decodeUrl(path)
    except: decodedPath = path
    when not defined(release):
      echo decodedPath
    if unlikely(decodedPath == "/favicon.ico"):
      await req.responseOk(zf.getFavicon)
    elif unlikely(
        decodedPath.len < zimNameLen + 5 or
        not decodedPath.startsWith('/' & zimName & '/') or
        decodedPath[zimNameLen+3] != '/' or
        decodedPath[zimNameLen+2] notin {
          namespaceLayout,
          namespaceArticles,
          namespaceImagesFiles
        }): await req.redirectToMainpage
    else:
      let namespace = decodedPath[zimNameLen+2]
      var url = decodedPath[zimNameLen + 4..^1]
      let r = zf.readDirectoryEntry(url, namespace)
      if likely(r.success):
        await req.responseOk(r.entry)
      elif namespace != namespaceArticles:
        await req.redirectToMainpage
      else:
        # The user looked for an article but the filename was not found:
        # We search for the best match and redirect because the filename is definetly different
        # to the filename that was requested.
        if not url.endsWith(".html"): url = url & ".html" # gives better results
        let bestMatchResult = zf.readDirectoryEntry(url, namespaceArticles, true)
        await req.redirectTo(namespace, bestMatchResult.entry.url)

  echo fmt"Serving ZIM file at http://localhost:{port}/{zimName}/{namespaceArticles}/{urlMainpage}"
  echo zf.getTitle
  echo zf.getDescription
  echo zf.getDate
  echo "Press CTRL-C to stop the server."
  let server = newAsyncHttpServer(maxBody = 0)
  waitFor server.serve(Port(port), handleRequest)

when isMainModule:
  import cligen

  proc checksum(filename: string) =
    let zf = openZimFile(filename)
    let zimName = decodeUrl(zf.getName)
    echo &"Calculating MD5 checksum for ZIM file with name: {zimName}.\nPlease wait..."
    let internal = zf.internalChecksum
    let calculated = zf.calculatedChecksum
    echo fmt"Internal checksum was:   {internal}"
    echo fmt"Calculated checksum was: {calculated}"

  proc metadata(filename: string) =
    let zf = openZimFile(filename)
    echo zf.metadata

  proc printArticle(filename: string, articleName: string) =
    # TODO: implement search
    let zf = openZimFile(filename)
    var result = zf.binarySearchByUrl(if not articleName.endsWith(".html"): articleName & ".html" else: articleName, namespaceArticles)
    if result.entry.isRedirect:
      result.entry = zf.readDirectoryEntry(zf.urlPointerAtPos(result.entry.redirectIndex), true)
    echo zf.readBlob(result.entry)

  proc randomArticle(filename: string) =
    let zf = openZimFile(filename)
    let randomResult = zf.randomArticleEntry()
    echo zf.readBlob(randomResult)

  proc mimetypes(filename: string) =
    let zf = openZimFile(filename)
    echo zf.mimetypeList

  proc debug(filename: string) =
    let zf = openZimFile(filename)
    echo &"filename: {filename}\nfilesize: {zf.filesize}\ninternalChecksum: {zf.internalChecksum}\nheader: {zf.header}\nmetadata: {zf.metadata}"


  dispatchMulti(
    [startZimHttpServer, cmdname="server"],
    [checksum],
    [metadata],
    [printArticle],
    [randomArticle, cmdname="random"],
    [mimetypes],
    [debug]
  )